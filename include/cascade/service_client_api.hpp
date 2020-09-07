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
    std::vector<typename CascadeType::KeyType> key_list;

public:
    CascadeShardLinq() : boolinq::Linq<CascadeShardLinqStorageType<CascadeType>,typename CascadeType::ObjectType>() {}
    CascadeShardLinq(ServiceClientType& capi,
                     uint32_t sgidx,
                     uint32_t shidx,
                     persistent::version_t version = CURRENT_VERSION) :
        boolinq::Linq<CascadeShardLinqStorageType<CascadeType>,typename CascadeType::ObjectType>(),
        client_api(capi),
        subgroup_index(sgidx),
        shard_index(shidx) {
        /* load keys. */
        auto result = client_api.template list_keys<CascadeType>(CURRENT_VERSION, subgroup_index, shard_index);
        for(auto& reply_future:result.get()) {
            key_list = reply_future.second.get();
        }
        /* set up storage and nextFunc*/
        this->storage = std::make_pair(key_list.begin(), key_list.end());
        this->nextFunc = [this](CascadeShardLinqStorageType<CascadeType>& _storage) {
            if (_storage.first == _storage.second) {
                throw boolinq::LinqEndException();
            }

            /* get object */
            auto result = this->capi.template get<CascadeType>(*_storage.first,this->version,this->subgroup_index,this->shard_index);

            _storage.first++;

            for (auto& reply_future:result.get()) {
                auto object = reply_future.second.get();
                return object;
            }

            throw boolinq::LinqEndException();
        };
    }
};

#endif // HAS_BOOLINQ

}// namespace cascade
}// namespace derecho
