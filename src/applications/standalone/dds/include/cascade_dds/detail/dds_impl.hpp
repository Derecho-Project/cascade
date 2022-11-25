#include <bits/c++config.h>
#include <cascade_dds/dds.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <mutex>
#include <thread>
#include <deque>
#include <atomic>
namespace derecho {
namespace cascade {

#define DDS_CONFIG_JSON_FILE    "dds.json"

class DDSConfigJsonImpl: public DDSConfig {
    nlohmann::json config;
public:
    DDSConfigJsonImpl(const std::string& conf_file=DDS_CONFIG_JSON_FILE);
    std::string get_metadata_pathname() const override;
    std::vector<std::string> get_data_plane_pathnames() const override;
    std::string get_control_plane_suffix() const override;
    virtual ~DDSConfigJsonImpl();
};

template <typename T>
auto DDSMetadataClient::list_topics(const std::function<T(const std::unordered_map<std::string,Topic>&)>& func, bool refresh) {
    if (refresh) {
        refresh_topics();
    }
    std::shared_lock<std::shared_mutex> rlock(topics_shared_mutex);
    return func(topics);
}

/* The data plane message type */
class DDSMessage : mutils::ByteRepresentable {
public:
    /* topic */
    std::string topic;
    /* application data */
    Blob app_data;

    DEFAULT_SERIALIZATION_SUPPORT(DDSMessage,topic,app_data);

    /* constructors */
    DDSMessage();
    DDSMessage(const std::string& _topic,const Blob& _blob);
    DDSMessage(const DDSMessage& rhs);
    DDSMessage(DDSMessage&& rhs);
};

#define MAX_TOPIC_NAME_LENGTH    (32)
struct __attribute__((packed, aligned(4))) DDSMessageHeader {
    std::size_t topic_name_length;
    char        topic_name[MAX_TOPIC_NAME_LENGTH];
    uint8_t     message_bytes;
};
#define DDS_MESSAGE_HEADER_SIZE   offsetof(DDSMessageHeader,message_bytes)

template <typename MessageType>
class DDSPublisherImpl: public DDSPublisher<MessageType> {
    ServiceClientAPI& capi;
    const std::string topic;
    const std::string cascade_key;

public:
    /** Constructor
     * @param _topic        The topic of the publisher
     * @parma _object_pool  The object pool
     */
    DDSPublisherImpl(
            const std::string& _topic,
            const std::string& _object_pool) : 
        capi(ServiceClientAPI::get_service_client()),
        topic(_topic),
        cascade_key(_object_pool+PATH_SEPARATOR+_topic) {
        if(topic.size() > MAX_TOPIC_NAME_LENGTH) {
            throw derecho::derecho_exception(
                    "the size of '" + topic + "' exceeds " + std::to_string(MAX_TOPIC_NAME_LENGTH));
        }
    }

    virtual const std::string& get_topic() const override {
        return topic;
    }

    virtual void send(const MessageType& message
#ifdef ENABLE_EVALUATION
                      ,uint64_t message_id = 0
#endif
                      ) override {
#if !defined(USE_DDS_TIMESTAMP_LOG)
        TimestampLogger::log(TLT_DDS_PUBLISHER_SEND_START,capi.get_my_id(),message_id,get_time_ns());
#endif
        std::size_t requested_size = mutils::bytes_size(message) + DDS_MESSAGE_HEADER_SIZE;
        const blob_generator_func_t blob_generator = [requested_size,&message,this] (uint8_t* buffer, const std::size_t buffer_size) {
            if ( buffer_size > requested_size ) {
                throw std::runtime_error("message is too large to fit in the buffer.");
            }
            DDSMessageHeader* msg_ptr = reinterpret_cast<DDSMessageHeader*>(buffer);
            std::memcpy(msg_ptr->topic_name,topic.c_str(),topic.size());
            msg_ptr->topic_name_length = topic.size();
            mutils::to_bytes(message,&msg_ptr->message_bytes);

            return requested_size;
        };

        
        // prepare the object
        ObjectWithStringKey object(
#ifdef ENABLE_EVALUATION
                    message_id,
#endif//ENABLE_EVALUATION
                    CURRENT_VERSION,
                    0,
                    CURRENT_VERSION,
                    CURRENT_VERSION,
                    cascade_key,
                    blob_generator,
                    requested_size
                );

        // send message
        dbg_default_trace("in {}: put object with key:{}", __PRETTY_FUNCTION__, cascade_key);
        capi.put_and_forget(object);
#if !defined(USE_DDS_TIMESTAMP_LOG)
        TimestampLogger::log(TLT_DDS_PUBLISHER_SEND_END,capi.get_my_id(),message_id,get_time_ns());
#endif
    }

    /** Destructor */
    ~DDSPublisherImpl(){
        // nothing to release manually.
    }
};

class PerTopicRegistry;
class DDSSubscriberRegistry;
template <typename MessageType>
class DDSSubscriberImpl;

/**
 * @class SubscriberCore
 */
class SubscriberCore {
    friend PerTopicRegistry;
    friend DDSSubscriberRegistry;
    template <typename MessageType>
    friend class DDSSubscriberImpl;

private:
    const std::string topic;
    const uint32_t index;
    std::atomic<bool> online;
    std::unordered_map<std::string,cascade_notification_handler_t>  handlers;
    mutable std::mutex                                              handlers_mutex;

    std::deque<Blob>                message_queue;
    mutable std::condition_variable message_queue_cv;
    mutable std::mutex              message_queue_mutex;
    std::thread                     message_worker;

public:
    /**
     * The Constructor
     * @param _topic    topic
     * @param _index    the index in the per topic registry.
     */
    SubscriberCore(const std::string& _topic, const uint32_t _index);

    /**
     * add a handler
     * @param handler_name
     * @param handler
     */
    void add_handler(const std::string& handler_name, const cascade_notification_handler_t& handler);

    /**
     * list all handlers
     *
     * @return the name of the handlers
     */
    std::vector<std::string> list_handlers() const;

    /**
     * delete handler by name
     * @param handler name
     */
    void delete_handler(const std::string& handler_name);

    /**
     * Post a message to this subscriber.
     */
    void post(const Blob& blob);

    /**
     * shutdown the SubscriberCore thread
     * Please note that this will not remove it from the topic registry. Calling DDSClient::unsubscribe will do the
     * work.
     */
    void shutdown();

    /**
     * Destructor
     */
    virtual ~SubscriberCore();
};

/**
 * @class DDSSubscriberImpl
 */
template <typename MessageType>
class DDSSubscriberImpl: public DDSSubscriber<MessageType> {
    friend DDSSubscriberRegistry;
    std::shared_ptr<SubscriberCore> core;
public:
    /** Constructor
     * @param _capi         The shared cascade client handle
     * @param _topic        The topic of the publisher
     */
    DDSSubscriberImpl(const std::shared_ptr<SubscriberCore>& _core):
        core(_core) {}

    virtual const std::string& get_topic() override {
        return core->topic;
    }

    /**
     * add handler
     * @param handler_name
     * @param handler
     */
    void add_handler(const std::string& handler_name,const message_handler_t<MessageType>& handler) {
        core->add_handler(handler_name,
            [handler](const Blob& blob)->void {
                dbg_default_trace("subscriber core handler: blob size = {} bytes.", blob.size);
                const DDSMessageHeader* header_ptr = reinterpret_cast<const DDSMessageHeader*>(blob.bytes);
                mutils::deserialize_and_run(nullptr,&header_ptr->message_bytes,handler);
        });
    }

    /**
     * list handlers
     * @return the list of handlers.
     */
    std::vector<std::string> list_handlers() const {
        return core->list_handlers();
    }

    /**
     * delete handler by name
     * @param handler_name
     */
    void delete_handler(const std::string& handler_name) {
        core->delete_handler(handler_name);
    }
};


class PerTopicRegistry {
    friend DDSSubscriberRegistry;
    std::string topic;
    std::string cascade_key;
    std::map<uint32_t,std::shared_ptr<SubscriberCore>> registry;
    mutable uint32_t counter;

    /* constructor */
    PerTopicRegistry(const std::string& _topic,
            const std::string _cascade_key):
        topic(_topic),
        cascade_key(_cascade_key),
        registry{},
        counter(0) {}

    /** 
     * Insert a new subscriber core, assuming the big lock (DDSSubscriberRegistry::registry_mutex)
     * 
     * @tparam  MessageType
     * @param   handlers        an optional set of named handlers to go with the initial SubscriberCore. 
     *
     * @return a shared pointer to the new created SubscriberCore
     */
    template <typename MessageType>
    std::shared_ptr<SubscriberCore> create_subscriber_core(const std::unordered_map<std::string,message_handler_t<MessageType>>& handlers={}) {
        //2 - create a subscriber core
        registry.emplace(counter, std::make_shared<SubscriberCore>(topic,counter));
        for (const auto& handler: handlers) {
            registry[counter]->add_handler(handler.first,
                [hdlr=handler.second](const Blob& blob)->void{
                    dbg_default_trace("subscriber core handler: blob size = {} bytes.", blob.size);
                    const DDSMessageHeader* header_ptr = reinterpret_cast<const DDSMessageHeader*>(blob.bytes);
                    mutils::deserialize_and_run(nullptr,&header_ptr->message_bytes,hdlr);
                }
            );
        }
        counter ++;
        return registry.at(counter-1);
    }
};

/**
 * The core of DDSClient, it manages the subscribers.
 */
class DDSSubscriberRegistry {
    const std::string control_plane_suffix;
    std::unordered_map<std::string,PerTopicRegistry> registry;
    mutable std::mutex registry_mutex;

    /* helpers */
    void _topic_control(ServiceClientAPI& capi, const Topic& topic_info, DDSCommand::CommandType command_type);

public:
    DDSSubscriberRegistry(const std::string& _control_plane_suffix);
    /**
     * Create a subscriber
     * @tparam  MessageType         the message type
     * @param   capi                the shared cascade client
     * @param   metadata_service    the dds metadata service
     * @param   topic               the topic
     * @param   handlers            an unordered map for the named handlers.
     *
     * @return a subscriber
     */
    template <typename MessageType>
    std::unique_ptr<DDSSubscriber<MessageType>> subscribe(
            ServiceClientAPI& capi,
            DDSMetadataClient& metadata_service,
            const std::string& topic,
            const std::unordered_map<std::string,message_handler_t<MessageType>>& handlers) {
        // apply the big lock
        std::lock_guard<std::mutex> lck(registry_mutex);
        // check if topic is registered.
        if (registry.find(topic)==registry.cend()) {
            // create a topic entry.
            auto topic_info = metadata_service.get_topic(topic);
            registry.emplace(topic,PerTopicRegistry(topic,topic_info.pathname));
            // register universal per-topic handler, which dispatches messages to subscriber cores.
            capi.register_notification_handler(
                    [this,my_id=capi.get_my_id()](const Blob& blob)->void{
                        const DDSMessageHeader* ptr = reinterpret_cast<const DDSMessageHeader*>(blob.bytes);
                        std::string topic(ptr->topic_name,ptr->topic_name_length);
                        dbg_default_trace("notification handler is triggered on topic:{}, size={]",topic,blob.size);
#if !defined(USE_DDS_TIMESTAMP_LOG)
                        // We cannot get the message id here because the notification is not an object but a blob.
                        TimestampLogger::log(TLT_DDS_SUBSCRIBER_RECV,my_id,0);
#endif
                        for(auto& subscriber_core:registry.at(topic).registry) {
                            if (subscriber_core.second->online) {
                                subscriber_core.second->post(blob);
                            }
                        }
                    },topic_info.pathname);
            dbg_default_trace("registered a handler for topic:{} on pathname:{}",topic,topic_info.pathname);
            // subscribe to a cascade server.
            _topic_control(capi,topic_info,DDSCommand::SUBSCRIBE);
        }
        // register the handlers
        auto subscriber_core = registry.at(topic).template create_subscriber_core<MessageType>(handlers);
        // create a Subscriber object wrapping the Subscriber Core
        return std::make_unique<DDSSubscriberImpl<MessageType>>(subscriber_core);
    }

    /**
     * Unsubscribe
     * @tparam MessageType      the message type
     * @param  subscriber       the subscriber to unsubscribe
     */
    template <typename MessageType>
    void unsubscribe(
            ServiceClientAPI& capi,
            DDSMetadataClient& metadata_service,
            const DDSSubscriber<MessageType>& subscriber) {
        // apply the big lock
        std::lock_guard<std::mutex> lck(registry_mutex);
        const DDSSubscriberImpl<MessageType>* impl = dynamic_cast<const DDSSubscriberImpl<MessageType>*>(&subscriber);
        // test the existence of corresponding subscriber core.
        if (registry.find(impl->core->topic) == registry.cend()) {
            dbg_default_warn("unsubscribe abort because subscriber's topic '{}' does not exist in registry.", impl->core->topic);
            return;
        }
        if (registry.at(impl->core->topic).registry.find(impl->core->index) == registry.at(impl->core->topic).registry.cend()) {
            dbg_default_warn("unsubscribe abort because subscriber's index '{}' does not exist in the per topic registry.", impl->core->index);
            return;
        }
        // remove the subscriber core
        registry.at(impl->core->topic).registry.erase(impl->core->index);
        if (registry.at(impl->core->topic).registry.empty()) {
            auto topic_info = metadata_service.get_topic(impl->core->topic);
            // unsubscribe from a cascade server
            _topic_control(capi,topic_info,DDSCommand::UNSUBSCRIBE);
            // remove the topic entry
            registry.erase(impl->core->topic);
        }
    }

    /**
     * Destructor
     */
    virtual ~DDSSubscriberRegistry() {}
};

template <typename MessageType>
std::unique_ptr<DDSPublisher<MessageType>> DDSClient::create_publisher(const std::string& topic) {
    auto topic_info = metadata_service->get_topic(topic);
    if (topic_info.is_valid()) {
        return std::make_unique<DDSPublisherImpl<MessageType>>(topic_info.name,topic_info.pathname);
    } else {
        dbg_default_error("create_publisher failed because topic:'{}' does not exist.", topic);
        return nullptr;
    }
}

template <typename MessageType>
std::unique_ptr<DDSSubscriber<MessageType>> DDSClient::subscribe(
        const std::string& topic,
        const std::unordered_map<std::string,message_handler_t<MessageType>>& handlers) {
    return subscriber_registry->template subscribe<MessageType>(capi,*metadata_service,topic,handlers);
}

template <typename MessageType>
void DDSClient::unsubscribe(const std::unique_ptr<DDSSubscriber<MessageType>>& subscriber) {
    return subscriber_registry->template unsubscribe<MessageType>(capi,*metadata_service,*subscriber);
}

}
}
