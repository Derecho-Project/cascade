#include <cascade_dds/dds.hpp>
#include <derecho/core/derecho_exception.hpp>
#include <iostream>
#include <string>
#include <fstream>
#include <typeindex>
#include <sys/prctl.h>
#include <unistd.h>

namespace derecho{
namespace cascade {

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
        const json& dds_config) {
    return std::make_unique<DDSMetadataClient>(capi,dds_config[DDS_CONFIG_METADATA_PATHNAME].get<std::string>());
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

DDSClient::~DDSClient() {}

std::unique_ptr<DDSClient> DDSClient::create(const std::shared_ptr<ServiceClientAPI>& capi, const json& dds_config) {
    //TODO:
    return std::make_unique<DDSClient>();
}

json load_config() {
    static const char* dds_config_file = "dds.json";
    if (access(dds_config_file,F_OK|R_OK)!=0) {
        throw derecho::derecho_exception("Failed to load dds configuration.");
    }

    std::ifstream infile(dds_config_file);

    json config;
    infile >> config;
    infile.close();
    
    return config;
}

}
}
