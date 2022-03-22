#include <cascade_dds/dds.hpp>
namespace derecho {
namespace cascade {

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
