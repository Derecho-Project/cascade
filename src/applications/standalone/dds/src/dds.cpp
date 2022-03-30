#include <cascade_dds/dds.hpp>
#include <derecho/core/derecho_exception.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <iostream>
#include <string>
#include <fstream>
#include <typeindex>
#include <sys/prctl.h>
#include <unistd.h>
#include <thread>

namespace derecho{
namespace cascade {

/**
 * Message Format
 * ObjectWithStringKey[full_path_topic,blob]
 *                                      |
 *                                      V
 *                                 DDSMessage[topic,blob]
 */

std::shared_ptr<DDSConfig> DDSConfig::dds_config_singleton;
std::mutex DDSConfig::dds_config_singleton_mutex;

std::shared_ptr<DDSConfig> DDSConfig::get() {
    if (dds_config_singleton == nullptr) {
        std::lock_guard<std::mutex> lock(dds_config_singleton_mutex);
        if (dds_config_singleton == nullptr) {
            dds_config_singleton = std::make_shared<DDSConfigJsonImpl>();
        }
    }
    return dds_config_singleton;
}

DDSConfigJsonImpl::DDSConfigJsonImpl(const std::string& conf_file) {
    if (access(conf_file.c_str(),F_OK|R_OK)!=0) {
        throw derecho::derecho_exception("Failed to load dds configuration.");
    }

    std::ifstream infile(conf_file);

    infile >> config;
    infile.close();
}

std::string DDSConfigJsonImpl::get_metadata_pathname() const {
    return config[DDS_CONFIG_METADATA_PATHNAME].get<std::string>();
}

std::vector<std::string> DDSConfigJsonImpl::get_data_plane_pathnames() const {
    std::vector<std::string> vec;
    for(const auto& dp: config[DDS_CONFIG_DATA_PLANE_PATHNAMES]) {
        vec.emplace_back(dp.get<std::string>());
    }
    return vec;
}

std::string DDSConfigJsonImpl::get_control_plane_suffix() const {
    return config[DDS_CONFIG_CONTROL_PLANE_SUFFIX].get<std::string>();
}

DDSConfigJsonImpl::~DDSConfigJsonImpl() {
    // do nothing so far
}

Topic::Topic():
    name(""),
    pathname("") {}

Topic::Topic(const std::string& _name, const std::string& _pathname):
    name(_name),
    pathname(_pathname) {}

Topic::Topic(const Topic& rhs):
    name(rhs.name),
    pathname(rhs.pathname) {}

Topic::Topic(Topic&& rhs):
    name(std::move(rhs.name)),
    pathname(std::move(rhs.pathname)) {}

bool Topic::is_valid() {
    return !name.empty() && !pathname.empty();
}

std::string Topic::get_full_path() const {
    return pathname + PATH_SEPARATOR + name;
}

DDSMetadataClient::DDSMetadataClient(
        const std::shared_ptr<ServiceClientAPI>& _capi,
        const std::string& _metadata_pathname) : 
    capi(_capi),
    metadata_pathname(_metadata_pathname){}

void DDSMetadataClient::refresh_topics() {
    auto topic_keys_future = capi->list_keys(CURRENT_VERSION,true,metadata_pathname);
    auto topic_keys = capi->wait_list_keys(topic_keys_future);

    std::unordered_map<std::string,Topic> topics_map;

    for (const auto& topic_key: topic_keys) {
        auto res = capi->get(topic_key);
        for (auto& reply_future : res.get()) {
            auto reply = reply_future.second.get();
            // skip the deleted objects.
            if (reply.is_null()) {
                continue;
            }
            mutils::deserialize_and_run(nullptr,reply.blob.bytes,
                [&topics_map](const Topic& topic)->void{
                    topics_map.emplace(topic.name,topic);
                });
        }
    }

    std::lock_guard<std::shared_mutex> wlck(topics_shared_mutex);
    this->topics.swap(topics_map);
}

DDSMetadataClient::~DDSMetadataClient() {}

std::unique_ptr<DDSMetadataClient> DDSMetadataClient::create(
        const std::shared_ptr<ServiceClientAPI>& capi, 
        std::shared_ptr<DDSConfig> dds_config) {
    return std::make_unique<DDSMetadataClient>(capi,dds_config->get_metadata_pathname());
}

Topic DDSMetadataClient::get_topic(const std::string& topic_name,bool refresh) {
    if (refresh) {
        refresh_topics();
    }

    std::shared_lock<std::shared_mutex> rlck(topics_shared_mutex);
    if (topics.find(topic_name) != topics.cend()) {
        return topics.at(topic_name);
    } else {
        return Topic{};
    }
}

void DDSMetadataClient::create_topic(const Topic& topic) {
    std::shared_lock<std::shared_mutex> rlock(topics_shared_mutex);
    if (topics.find(topic.name)!=topics.cend()) {
        throw derecho::derecho_exception("Cannot create topic:"+topic.name+" because it has already existed.");
    }
    rlock.unlock();

    refresh_topics();

    rlock.lock();
    if (topics.find(topic.name)!=topics.cend()) {
        throw derecho::derecho_exception("Cannot create topic:"+topic.name+" because it has already existed.");
    }

    // prepare the object
    std::size_t size = mutils::bytes_size(topic);
    uint8_t stack_buffer[size];
    mutils::to_bytes(topic,stack_buffer);
    Blob blob(stack_buffer,size,true);
    ObjectWithStringKey topic_object(metadata_pathname+PATH_SEPARATOR+topic.name,blob);
    dbg_default_trace("create topic:{}", topic.name);
    auto result = capi->put(topic_object);
    for (auto& reply_future: result.get() ) {
        auto reply = reply_future.second.get();
        dbg_default_trace("Node {} replied with (v:0x{:x},t:{}us)", reply_future.first, 
                std::get<0>(reply), std::get<1>(reply));
    }

    topics.emplace(topic.name,topic);
}

void DDSMetadataClient::remove_topic(const std::string& topic_name) {
    refresh_topics();

    std::shared_lock<std::shared_mutex> rlock(topics_shared_mutex);
    if (topics.find(topic_name)==topics.cend()) {
        return;
    }
    rlock.unlock();

    std::lock_guard<std::shared_mutex> wlock(topics_shared_mutex);
    dbg_default_trace("remove topic:{}",topic_name);
    auto result = capi->remove(metadata_pathname+PATH_SEPARATOR+topic_name);
    for (auto& reply_future: result.get() ) {
        auto reply = reply_future.second.get();
        dbg_default_trace("Node {} replied with (v:0x{:x},t:{}us)", reply_future.first, 
                std::get<0>(reply), std::get<1>(reply));
    }

    topics.erase(topic_name);
}

DDSCommand::DDSCommand():
    command_type(DDSCommand::INVALID_TYPE),
    topic() {}

DDSCommand::DDSCommand(const CommandType _command_type, const std::string& _topic):
    command_type(_command_type),
    topic(_topic) {}

DDSMessage::DDSMessage():
    topic(),
    app_data() {}

DDSMessage::DDSMessage(const std::string& _topic,const Blob& _blob):
    topic(_topic),
    app_data(_blob) {}

DDSMessage::DDSMessage(const DDSMessage& rhs):
    topic(rhs.topic),
    app_data(rhs.app_data) {}

DDSMessage::DDSMessage(DDSMessage&& rhs):
    topic(std::move(rhs.topic)),
    app_data(std::move(rhs.app_data)) {}

SubscriberCore::SubscriberCore(const std::string& _topic, const uint32_t _index):
    topic(_topic),
    index(_index),
    online(true),
    handlers() {
    message_worker = std::thread([this](){
        while(online) {
            std::unique_lock<std::mutex> qlck(message_queue_mutex);
            message_queue_cv.wait(qlck,[this](){return !message_queue.empty() || !online;});
            std::deque<Blob> q(std::move(message_queue));
            qlck.unlock();

            while(!q.empty()) {
                std::lock_guard<std::mutex> hlck(handlers_mutex);
                for (const auto& handle:handlers) {
                    dbg_default_trace("call: handler {} on topic {}, size = {} bytes.",handle.first,topic,q.front().size);
                    handle.second(q.front());
                    dbg_default_trace("done: handler {} on topic {}, size = {} bytes.",handle.first,topic,q.front().size);
                }
                q.pop_front();
            }
        }
    });
}

void SubscriberCore::add_handler(const std::string& handler_name, const cascade_notification_handler_t& handler) {
    std::lock_guard<std::mutex> lck(handlers_mutex);
    handlers.emplace(handler_name, handler);
}

std::vector<std::string> SubscriberCore::list_handlers() const {
    std::lock_guard<std::mutex> lck(handlers_mutex);
    std::vector<std::string> vec;
    for (const auto& ent:handlers) {
        vec.emplace_back(ent.first);
    }
    return vec;
}

void SubscriberCore::delete_handler(const std::string& handler_name) {
    std::lock_guard<std::mutex> lck(handlers_mutex);
    handlers.erase(handler_name);
}

void SubscriberCore::post(const Blob& blob) {
    dbg_default_trace("{} post a blob of {} bytes.", __PRETTY_FUNCTION__, blob.size);
    std::lock_guard<std::mutex> lck(message_queue_mutex);
    message_queue.emplace_back(blob);
    message_queue_cv.notify_one();
}

void SubscriberCore::shutdown() {
    if (online) {
        online.store(false);
        message_queue_cv.notify_all();
        message_worker.join();
    }
}

SubscriberCore::~SubscriberCore() {
    shutdown();
}

DDSSubscriberRegistry::DDSSubscriberRegistry(const std::string& _control_plane_suffix) :
    control_plane_suffix(_control_plane_suffix) {}

void DDSSubscriberRegistry::_topic_control(std::shared_ptr<ServiceClientAPI>& capi, const Topic& topic_info, DDSCommand::CommandType command_type) {
        DDSCommand command(command_type,topic_info.name);
        std::size_t buffer_size = mutils::bytes_size(command);
        uint8_t stack_buffer[buffer_size];
        mutils::to_bytes(command,stack_buffer);
        ObjectWithStringKey object(topic_info.pathname + PATH_SEPARATOR + control_plane_suffix,Blob(stack_buffer,buffer_size,true));
        capi->trigger_put(object);
        dbg_default_trace("Sent DDS command:{} to service, command key={}", command.to_string(), object.get_key_ref());
    }

DDSClient::DDSClient(const std::shared_ptr<ServiceClientAPI>& _capi,
        const std::shared_ptr<DDSConfig>& _dds_config):
        capi(_capi) {
    subscriber_registry = std::make_unique<DDSSubscriberRegistry>(_dds_config->get_control_plane_suffix());
    metadata_service = std::make_unique<DDSMetadataClient>(_capi,_dds_config->get_metadata_pathname());
}

DDSClient::~DDSClient() {
    // nothing to release manually
}

std::unique_ptr<DDSClient> DDSClient::create(const std::shared_ptr<ServiceClientAPI>& capi, const std::shared_ptr<DDSConfig>& dds_config) {
    return std::make_unique<DDSClient>(capi, dds_config);
}

}
}
