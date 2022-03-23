#pragma once
#include <cascade/service_client_api.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <memory>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <shared_mutex>
#include <nlohmann/json.hpp>

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
    std::shared_ptr<ServiceClientAPI> capi;
    /** the object pool for DDS metadata */
    std::string metadata_pathname;
    /* local cache of the topics, a map from topic name to topic object. */
    std::unordered_map<std::string,Topic> topics;
    /* local cache lock */
    mutable std::shared_mutex topics_shared_mutex;

public:
    /**
     * Constructor
     * @param _capi             shared cascade client
     * @param metadata_pathname the object pool for DDS metadata
     */
    DDSMetadataClient(const std::shared_ptr<ServiceClientAPI>& _capi,const std::string& metadata_pathname);

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
    static std::unique_ptr<DDSMetadataClient> create(const std::shared_ptr<ServiceClientAPI>& capi, std::shared_ptr<DDSConfig> dds_config);
};

//TODO
template <typename MessageType>
class DDSPublisher {
public:
    DDSPublisher();
    void send(const MessageType& message);
    virtual ~DDSPublisher();
};

//TODO
template <typename MessageType>
using message_handler_t = std::function<void(const MessageType&)>;

template <typename MessageType>
class DDSSubscriber {
public:
    DDSSubscriber(const message_handler_t<MessageType>&);
    virtual ~DDSSubscriber();
};

/**
 * DDS Client
 */
class DDSClient {
public:
    template <typename MessageType>
    std::unique_ptr<DDSPublisher<MessageType>>   create_publisher(std::string& topic);
    template <typename MessageType>
    std::unique_ptr<DDSSubscriber<MessageType>>  create_subscriber(std::string& topic, const message_handler_t<MessageType>& handler);

    virtual ~DDSClient();

    static std::unique_ptr<DDSClient> create(const std::shared_ptr<ServiceClientAPI>& capi, std::shared_ptr<DDSConfig> dds_config);
};

}
}

#include "detail/dds_impl.hpp"
