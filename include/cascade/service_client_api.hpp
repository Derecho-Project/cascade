#pragma once
#include "config.h"
#include "service_types.hpp"
#include "service.hpp"

#if HAS_BOOLINQ
#include <boolinq/boolinq.h>
#endif

namespace derecho {
namespace cascade {
/**
 * The client API
 */
using ServiceClientAPI = ServiceClient<VCSU,VCSS,PCSU,PCSS>;

/**
 * Create Linq iterators on keys or versions of keys
 * TODO: Linq iterators on version, timestamp ...
 */
#if HAS_BOOLINQ

template <typename CascadeType>
using CascadeShardLinqStorageType = std::pair<typename std::vector<typename CascadeType::KeyType>::iterator,typename std::vector<typename CascadeType::KeyType>::iterator>;

/**
 * The shard linq iterate the keys in a shard.
 */
template <typename CascadeType, typename ServiceClientType>
class CascadeShardLinq : public boolinq::Linq<CascadeShardLinqStorageType<CascadeType>, typename CascadeType::ObjectType> {
private:
    ServiceClientType& client_api;
    uint32_t subgroup_index;
    uint32_t shard_index;
    persistent::version_t version;

public:
    CascadeShardLinq() : boolinq::Linq<CascadeShardLinqStorageType<CascadeType>,typename CascadeType::ObjectType>() {}
    CascadeShardLinq(ServiceClientType& capi,
                     uint32_t sgidx,
                     uint32_t shidx,
                     persistent::version_t ver,
                     std::vector<typename CascadeType::KeyType>& key_list,
                     std::function<typename CascadeType::ObjectType(CascadeShardLinqStorageType<CascadeType>&)> nextFunc) :
        boolinq::Linq<CascadeShardLinqStorageType<CascadeType>,typename CascadeType::ObjectType>(std::make_pair(key_list.begin(),key_list.end()), nextFunc),
        client_api(capi),
        subgroup_index(sgidx),
        shard_index(shidx),
        version(ver) {}
};

/**
 * Creat a Linq iterating the objects in a shard.
 * @param key_list  This is an output argument to keep the generated key_list. Please keep it alive throughout the life
 *                  time of the Link object.
 * @param capi      The cascade client.
 * @param subgroup_index
 * @param shard_index
 * @param version
 * @return the Linq object
 */
template <typename CascadeType, typename ServiceClientType>
CascadeShardLinq<CascadeType,ServiceClientType> from_shard(
        std::vector<typename CascadeType::KeyType>& key_list,
        ServiceClientType& capi, uint32_t subgroup_index, 
        uint32_t shard_index, persistent::version_t version) {
        /* load keys. */
        auto result = capi.template list_keys<CascadeType>(version, subgroup_index, shard_index);
        for(auto& reply_future:result.get()) {
            key_list = reply_future.second.get();
        }
        /* set up storage and nextFunc*/
        return CascadeShardLinq<CascadeType,ServiceClientType>(capi,subgroup_index,shard_index,version,key_list,
            [&capi,subgroup_index,shard_index,version](CascadeShardLinqStorageType<CascadeType>& _storage) {
                if (_storage.first == _storage.second) {
                    throw boolinq::LinqEndException();
                }
    
                /* get object */
                auto result = capi.template get<CascadeType>(*_storage.first,version,subgroup_index,shard_index);
    
                _storage.first++;
    
                for (auto& reply_future:result.get()) {
                    auto object = reply_future.second.get();
                    return object;
                }
    
                throw boolinq::LinqEndException();
            });
}

#endif // HAS_BOOLINQ

}// namespace cascade
}// namespace derecho
