#pragma once
#include <cascade/service_client_api.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>
#include <shared_mutex>
#include <nlohmann/json.hpp>
#include <cascade_dds/config.h>

using namespace nlohmann;

/**
 * cascade DDS client API
 */
namespace derecho {
namespace cascade {

#define DDS_CONFIG_METADATA_PATHNAME    "metadata_pathname"
#define DDS_CONFIG_DATA_PLANE_PATHNAMES "data_plane_pathnames"
#define DDS_CONFIG_CONTROL_PLANE_SUFFIX "control_plane_suffix"

/**
 * load json configuration
 */
class DDSConfig {
private:
    static std::shared_ptr<DDSConfig> dds_config_singleton;
    static std::mutex dds_config_singleton_mutex;

public:
    /**
     * getter for metadata_pathname
     * @return
     */
    virtual std::string get_metadata_pathname() const = 0;
    /**
     * getter for data_plane_pathnames
     * @return
     */
    virtual std::vector<std::string> get_data_plane_pathnames() const = 0;
    /**
     * getter for control_plane_suffix
     * @return
     */
    virtual std::string get_control_plane_suffix() const = 0;
    /**
     * get the singleton
     */
    static std::shared_ptr<DDSConfig> get();
};

/**
 * The topic type
 */
class Topic:public mutils::ByteRepresentable {
public:
    /** topic name */
    std::string name;
    /** the object pool */
    std::string pathname;
    /** serialization support */
    DEFAULT_SERIALIZATION_SUPPORT(Topic,name,pathname);

    /**
     * Constructor 1
     */
    Topic();

    /**
     * Contructor 2
     * @param _name     topic name
     * @param _pathname object pool
     */
    Topic(const std::string& _name,const std::string& _pathname);

    /**
     * Constructor 3: copy constructor
     * @param rhs
     */
    Topic(const Topic& rhs);

    /**
     * Constructor 4: move constructor
     * @param rhs
     */
    Topic(Topic&& rhs);

    /**
     * Test if the topic is valid or not
     */
    bool is_valid();

    /**
     * get the full path or 'key' for this topic.
     * @return the key.
     */
    std::string get_full_path() const;
};

/**
 * DDSMetadataClient is the API for accessing DDS Metadata
 */
class DDSMetadataClient {
private:
    /** shared cascade client */
    ServiceClientAPI& capi;
    /** the object pool for DDS metadata */
    std::string metadata_pathname;
    /* local cache of the topics, a map from topic name to topic object. */
    std::unordered_map<std::string,Topic> topics;
    /* local cache lock */
    mutable std::shared_mutex topics_shared_mutex;

public:
    /**
     * Constructor
     * @param metadata_pathname the object pool for DDS metadata
     */
    DDSMetadataClient(const std::string& metadata_pathname);

    /**
     * list the topics
     * @tparam  T       the type of the return value
     * @param func      a lambda function that will be fed with the local topic cache.
     * @param refresh   true for refresh the topic list, otherwise, just use the local cache.
     *
     * @return return value is decided by func
     */
    template <typename T>
    auto list_topics(
            const std::function<T(const std::unordered_map<std::string,Topic>&)>& func,
            bool refresh=true);

    /** refresh local topic cache */
    void refresh_topics();

    /**
     * Get a topic
     * @param topic_name        the name of the topic
     * @param refresh           true for refresh the topic list, otherwise, just use the local cache.
     *
     * @return the topic object
     */
    Topic get_topic(const std::string& topic_name,bool refresh=true);

    /**
     * create a topic, if such a topic does exist, it will through an exception
     * @param topic             The topic object
     */
    void create_topic(const Topic& topic);

    /**
     * remove a topic, if such a topic does not exist, it return silently.
     * @param topic_name        The topic name
     */
    void remove_topic(const std::string& topic_name);

    /**
     * the destructor
     */
    virtual ~DDSMetadataClient();

    /**
     * create an DDSMetadataClient object.
     */
    static std::unique_ptr<DDSMetadataClient> create(std::shared_ptr<DDSConfig> dds_config);
};

/**
 * DDSCommand
 */
class DDSCommand : public mutils::ByteRepresentable {
public:
    enum CommandType {
        INVALID_TYPE,
        SUBSCRIBE,
        UNSUBSCRIBE,
#ifdef USE_DDS_TIMESTAMP_LOG
        FLUSH_TIMESTAMP_TRIGGER, // trigger a flush timestamp operation (in trigger)
        FLUSH_TIMESTAMP_ORDERED // really flush timestamp
#endif
    };
    /* Command type */
    CommandType command_type;
    /* Topic */
    std::string topic;
    
    DEFAULT_SERIALIZATION_SUPPORT(DDSCommand,command_type,topic);

    DDSCommand();
    DDSCommand(const CommandType _command_type, const std::string& _topic);

    std::string to_string() {
        std::string command_name;
        switch(command_type) {
        case SUBSCRIBE:
            command_name = "subscribe";
            break;
        case UNSUBSCRIBE:
            command_name = "unsubscribe";
            break;
#ifdef USE_DDS_TIMESTAMP_LOG
        case FLUSH_TIMESTAMP_TRIGGER:
            command_name = "flush_timestamp_trigger";
            break;
        case FLUSH_TIMESTAMP_ORDERED:
            command_name = "flush_timestamp_ordered";
            break;
#endif
        default:
            command_name = "invalid";
        }
        return std::string("DDSCommand: { command:") + command_name + ", topic:" + topic;
    }
};

/**
 * DDSPublisher interface
 */
template <typename MessageType>
class DDSPublisher {
public:
    /**
     * get the topic of this publisher
     * @return topic
     */
    virtual const std::string& get_topic() const = 0;
    /**
     * publish a message
     * @message the message to publish to the topic
     */
    virtual void send(const MessageType& message
#ifdef ENABLE_EVALUATION
            ,uint64_t message_id = 0
#endif
            ) = 0;
};

/**
 * application's message handler for DDSSubscriber
 */
template <typename MessageType>
using message_handler_t = std::function<void(const MessageType&)>;

/**
 * DDSSubscriber interface
 */
template <typename MessageType>
class DDSSubscriber {
public:
    /**
     * get the topic of this publisher
     * @return topic
     */
    virtual const std::string& get_topic() = 0;
};

/* For internal use only*/
class DDSSubscriberRegistry;
/**
 * DDS Client
 */
class DDSClient {
private:
    ServiceClientAPI&                       capi;
    std::unique_ptr<DDSSubscriberRegistry>  subscriber_registry;
    std::unique_ptr<DDSMetadataClient>      metadata_service;
#ifdef USE_DDS_TIMESTAMP_LOG
    std::string                             control_plane_suffix;
#endif

public:
    /**
     * Constructor
     * @param dds_config            The dds configuration
     */
    DDSClient(const std::shared_ptr<DDSConfig>& _dds_config);
    /**
     * create a publisher
     * @tparam MessageType          Serializable application message type, must be either pod types, stl types, or
     *                              derived from mutil::BytesRepresentable.
     * @param topic                 topic name
     *
     * @return a unique pointer to the publisher
     */
    template <typename MessageType>
    std::unique_ptr<DDSPublisher<MessageType>>   create_publisher(const std::string& topic);
    /**
     * create a subscriber (or subscribe)
     * @tparam MessageType          Serializable application message type, must be either pod types, stl types, or
     *                              derived from mutil::BytesRepresentable.
     * @param topic                 topic name
     * @param handlers              The optional named message handlers.
     *
     * @return a unique pointer to the subscriber
     */
    template <typename MessageType>
    std::unique_ptr<DDSSubscriber<MessageType>> subscribe(
            const std::string& topic,
            const std::unordered_map<std::string,message_handler_t<MessageType>>& handlers = {});
    /**
     * remove a subscriber (or unsubscribe)
     * @param subscriber            The created subscriber
     */
    template <typename MessageType>
    void unsubscribe(const std::unique_ptr<DDSSubscriber<MessageType>>& subscriber);

#ifdef USE_DDS_TIMESTAMP_LOG
    /**
     * flush the timestamp of a topic
     * @param topic                 topic name
     */
    void flush_timestamp(const std::string& topic);
#endif

    /**
     * destructor
     */
    virtual ~DDSClient();

    /**
     * create a DDSClient
     * @param dds_config            The dds configuration
     */
    static std::unique_ptr<DDSClient> create(
            const std::shared_ptr<DDSConfig>& dds_config);
};

}
}

#include "detail/dds_impl.hpp"
