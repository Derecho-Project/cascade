#include <cascade_dds/dds.hpp>
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

}
}
