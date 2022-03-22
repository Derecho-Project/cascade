#pragma once
#include <cascade/service_client_api.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <memory>
#include <mutex>
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
 * DDSConfiguration
 */
class DDSConfig {
};

class Topic:public mutils::ByteRepresentable {
public:
    // topic name
    std::string name;
    // the object pool
    std::string pathname;
    DEFAULT_SERIALIZATION_SUPPORT(Topic,name,pathname);

    Topic();
    Topic(const std::string& _name,const std::string& _pathname);
    Topic(const Topic& rhs);
    Topic(Topic&& rhs);

    std::string get_full_path() const;
};

//TODO
class DDSMetadataClient {
private:
    std::shared_ptr<ServiceClientAPI> capi;
    std::string metadata_pathname;
    std::unordered_map<std::string,Topic> topics;
    mutable std::shared_mutex topics_shared_mutex;

public:
    DDSMetadataClient(const std::shared_ptr<ServiceClientAPI>& _capi,const std::string& metadata_pathname);

    template <typename T>
    auto list_topics(
            const std::function<T(const std::unordered_map<std::string,Topic>&)>& func,
            bool refresh=true);

    void refresh_topics();

    Topic get_topic(const std::string& topic_name,bool refresh=true);

    void create_topic(const Topic& topic);

    virtual ~DDSMetadataClient();

    static std::unique_ptr<DDSMetadataClient> create(const std::shared_ptr<ServiceClientAPI>& capi, const json& dds_config);
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

//TODO
class DDSClient {
public:
    template <typename MessageType>
    std::unique_ptr<DDSPublisher<MessageType>>   create_publisher(std::string& topic);
    template <typename MessageType>
    std::unique_ptr<DDSSubscriber<MessageType>>  create_subscriber(std::string& topic, const message_handler_t<MessageType>& handler);

    virtual ~DDSClient();

    static std::unique_ptr<DDSClient> create(const std::shared_ptr<ServiceClientAPI>& capi, const json& dds_config);
};

//TODO
json load_config();

}
}

#include "detail/dds_impl.hpp"
