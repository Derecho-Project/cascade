#pragma once
#include "service_types.hpp"
#include "service.hpp"

#ifdef HAS_BOOLINQ
#include <boolinq/boolinq.h>
#endif

namespace derecho {
namespace cascade {
/**
 * The client API
 */
using ServiceClientAPI = ServiceClient<VolatileCascadeStoreWithStringKey, PersistentCascadeStoreWithStringKey, TriggerCascadeNoStoreWithStringKey>;

/**
 * Create Linq iterators on keys or versions of keys
 */
#ifdef HAS_BOOLINQ

template <typename CascadeType>
using CascadeShardLinqStorageType = std::pair<typename std::vector<typename CascadeType::KeyType>::iterator, typename std::vector<typename CascadeType::KeyType>::iterator>;

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
 * @return a Linq object
 */
template <typename CascadeType, typename ServiceClientType>
CascadeShardLinq<CascadeType,ServiceClientType> from_shard(
        std::vector<typename CascadeType::KeyType>& key_list,
        ServiceClientType& capi, uint32_t subgroup_index, 
        uint32_t shard_index, persistent::version_t version) {
    /* load keys. */
    auto result = capi.template list_keys<CascadeType>(version, true, subgroup_index, shard_index);
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
            auto result = capi.template get<CascadeType>(*_storage.first,version,true/*always use stable*/,subgroup_index,shard_index);

            _storage.first++;

            for (auto& reply_future:result.get()) {
                auto object = reply_future.second.get();
                return object;
            }

            throw boolinq::LinqEndException();
        });
}

/**
 * Create a Linq iterating the objects in a shard at a point of time.
 * @param key_list  This is an output argument to keep the generated key_list. Please keep it alive throughout the life
 *                  time of the Link object.
 * @param capi      The cascade client.
 * @param subgroup_index
 * @param shard_index
 * @param ts_us     The unix epoch time in microsecond. 
 * @return a Linq object
 */
template <typename CascadeType, typename ServiceClientType>
CascadeShardLinq<CascadeType,ServiceClientType> from_shard_by_time (
    std::vector<typename CascadeType::KeyType>& key_list,
    ServiceClientType& capi, uint32_t subgroup_index,
 uint32_t shard_index, const uint64_t ts_us) {
 /* load keys. */
    auto result = capi.template list_keys_by_time<CascadeType>(ts_us, true, subgroup_index, shard_index);
    for(auto& reply_future:result.get()) {
        key_list = reply_future.second.get();
    }
 /* set up storage and nextFunc*/
    return CascadeShardLinq<CascadeType,ServiceClientType>(capi,subgroup_index,shard_index,CURRENT_VERSION,key_list,
        [&capi,subgroup_index,shard_index,ts_us](CascadeShardLinqStorageType<CascadeType>& _storage) {
            if (_storage.first == _storage.second) {
                throw boolinq::LinqEndException();
            }

            /* get object, always use stable version */
            auto result = capi.template get_by_time<CascadeType>(*_storage.first,ts_us,true,subgroup_index,shard_index);
            _storage.first++;
            for (auto& reply_future:result.get()) {
                auto object = reply_future.second.get();
                return object;
            }
            throw boolinq::LinqEndException();
        });
}

/* A version linq iterates the versions of a Key*/
template <typename CascadeType, typename ServiceClientType>
class CascadeVersionLinq : public boolinq::Linq<persistent::version_t, typename CascadeType::ObjectType> {
private:
    ServiceClientType& client_api;
    uint32_t subgroup_index;
    uint32_t shard_index;
    const typename CascadeType::KeyType& key;
    persistent::version_t version;

public:
    CascadeVersionLinq() : boolinq::Linq<persistent::version_t,typename CascadeType::ObjectType>() {};
    
    CascadeVersionLinq(ServiceClientType& capi, 
        uint32_t sgidx, 
        uint32_t shidx, 
        const typename CascadeType::KeyType& objkey, 
        persistent::version_t ver,
                       std::function<typename CascadeType::ObjectType(persistent::version_t&)> nextFunc) :

        boolinq::Linq<persistent::version_t, typename CascadeType::ObjectType>(ver, nextFunc),
        client_api(capi),
     subgroup_index(sgidx),
        shard_index(shidx),
        key(objkey),
     version(ver) {}
};

/**
 * Create a Linq iterating the objects of a key for given versions.
 * @param key       The key to iterate over
 * @param capi      The cascade client.
 * @param subgroup_index
 * @param shard_index
 * @param version   The start version going backward: TODO: this should be a pair for version range.
 * @return a Linq object
 */
template <typename CascadeType, typename ServiceClientType>
CascadeVersionLinq<CascadeType,ServiceClientType> from_versions(
    const typename CascadeType::KeyType& key, 
 ServiceClientType &capi, uint32_t subgroup_index,
    uint32_t shard_index, persistent::version_t version) {

 return CascadeVersionLinq<CascadeType,ServiceClientType>(capi,subgroup_index,shard_index,key,version,
     [&capi,&key,subgroup_index,shard_index](persistent::version_t& ver) {
   if (ver == INVALID_VERSION) {
       throw boolinq::LinqEndException();
   }
  
            do {
                auto result = capi.template get<CascadeType>(key,ver,true/*always use stable data*/,subgroup_index,shard_index);
                for (auto& reply_future:result.get()) {
                    auto object = reply_future.second.get();
                    ver = object.previous_version_by_key; 
                    if (!object.is_null())
                        return object;
                }
            } while (ver != INVALID_VERSION);

            throw boolinq::LinqEndException();
     });
}

template <typename CascadeType, typename ServiceClientType>
using CascadeSubgroupLinqStorageType = typename std::pair<typename std::vector<CascadeShardLinq<CascadeType,ServiceClientType>>::iterator, typename std::vector<CascadeShardLinq<CascadeType,ServiceClientType>>::iterator>;

/**
 * The subgroup linq iterates all the objects in the shards of a subgroup
 */
template <typename CascadeType, typename ServiceClientType>
class CascadeSubgroupLinq : public boolinq::Linq<CascadeSubgroupLinqStorageType<CascadeType,ServiceClientType>, typename CascadeType::ObjectType> {
private:
    ServiceClientType& client_api;
    uint32_t subgroup_index;
    persistent::version_t version;

public:
    CascadeSubgroupLinq(): boolinq::Linq<CascadeSubgroupLinqStorageType<CascadeType,ServiceClientType>, typename CascadeType::ObjectType>() {}

    CascadeSubgroupLinq(ServiceClientType& capi,
      uint32_t sgidx,
      persistent::version_t ver,
      std::vector<CascadeShardLinq<CascadeType,ServiceClientType>>& shard_linq_list,
      std::function<typename CascadeType::ObjectType(CascadeSubgroupLinqStorageType<CascadeType,ServiceClientType>&)> nextFunc) :
  
  boolinq::Linq<CascadeSubgroupLinqStorageType<CascadeType,ServiceClientType>, typename CascadeType::ObjectType>(std::make_pair(shard_linq_list.begin(),shard_linq_list.end()), nextFunc),
  client_api(capi),
  subgroup_index(sgidx),
  version(ver) {}
};

/**
 * Create a Linq iterating the objects in a subgroup.
 * @param shardidx_to_keys      The map that contains mappings between shard index and the corresponding 
 *                              key lists. The key lists are used to store the generated key_list in [from_shard] 
 *                              function call. Please keep it alive throughout the life time of the Link object.
 * @param shard_linq_list       This is an output argument to keep the generated ShardLinq for each shard in the 
 *                              in the subgroup. Please keep it alive throughout the life time of the Link object.
 * @param capi                  The cascade client.
 * @param sgidx
 * @param version
 * @return a Linq object
 */
template <typename CascadeType, typename ServiceClientType>
CascadeSubgroupLinq<CascadeType,ServiceClientType> from_subgroup(
    std::unordered_map<uint32_t, std::vector<typename CascadeType::KeyType>>& shardidx_to_keys, 
 std::vector<CascadeShardLinq<CascadeType,ServiceClientType>>& shard_linq_list,
 ServiceClientType& capi, uint32_t sgidx, persistent::version_t ver) {
 
    uint32_t num_shards = capi.template get_number_of_shards<CascadeType>(sgidx);
    std::cout << "num_shards=" << num_shards << std::endl;
    for (uint32_t shidx=0;shidx<num_shards;shidx++) {
        shardidx_to_keys.emplace(shidx,std::vector<typename CascadeType::KeyType>());
        shard_linq_list.emplace_back(from_shard<CascadeType, ServiceClientType>(shardidx_to_keys[shidx],capi,sgidx,shidx,ver));
    }
    std::cout << "done prepare shard iterators." << std::endl;
 
 return CascadeSubgroupLinq<CascadeType,ServiceClientType>(capi,sgidx,ver,shard_linq_list,
     [&shard_linq_list](CascadeSubgroupLinqStorageType<CascadeType,ServiceClientType>& _storage) {
            if (_storage.first == _storage.second) {
       throw boolinq::LinqEndException();
   }

   do {
          try {
           auto obj = _storage.first->next();
     return obj;
       } 
    catch(boolinq::LinqEndException &) {
            _storage.first++;
       } 
   } while (_storage.first != _storage.second);
         
   throw boolinq::LinqEndException();
     });
}


template <typename CascadeType>
using CascadeObjectpoolLinqStorageType = CascadeShardLinqStorageType<CascadeType>;
/**
 * The shard linq iterate the keys in a objectpool.
 */
template <typename CascadeType, typename ServiceClientType>
class CascadeObjpoolLinq : public boolinq::Linq<CascadeObjectpoolLinqStorageType<CascadeType>, typename CascadeType::ObjectType> {
private:
    ServiceClientType& client_api;
    persistent::version_t version;
    std::string objpool_pathname;

public:
    CascadeObjpoolLinq() : boolinq::Linq<CascadeObjectpoolLinqStorageType<CascadeType>,typename CascadeType::ObjectType>() {}
    CascadeObjpoolLinq(ServiceClientType& capi,
                     persistent::version_t ver,
                     const std::string& pathname,
                     std::vector<typename CascadeType::KeyType>& key_list,
                     std::function<typename CascadeType::ObjectType(CascadeObjectpoolLinqStorageType<CascadeType>&)> nextFunc) :
        boolinq::Linq<CascadeObjectpoolLinqStorageType<CascadeType>,typename CascadeType::ObjectType>(std::make_pair(key_list.begin(),key_list.end()), nextFunc),
        client_api(capi),
        version(ver),
        objpool_pathname(pathname) {}
};

/**
 * Creat a Linq iterating the objects in an objectpool.
 * @param key_list  This is an output argument to keep the generated key_list. Please keep it alive throughout the life
 *                  time of the Link object.
 * @param capi      The cascade client.
 * @param version
 * @param objpool_pathname
 * @return a Linq object
 */
template <typename CascadeType, typename ServiceClientType>
CascadeObjpoolLinq<CascadeType,ServiceClientType> from_objectpool(
        ServiceClientType& capi,
        std::vector<typename CascadeType::KeyType>& key_list, 
        persistent::version_t version,
        const std::string objpool_path) {
    /* load keys. */
    auto future_results = capi.template list_keys<CascadeType>(version, true, objpool_path);
    key_list = std::move(capi.template wait_list_keys<CascadeType>(future_results));
    /* set up storage and nextFunc*/
    return CascadeObjpoolLinq<CascadeType,ServiceClientType>(capi,version,objpool_path,key_list,
        [&capi,version,objpool_path](CascadeObjectpoolLinqStorageType<CascadeType>& _storage) {
            if (_storage.first == _storage.second) {
                throw boolinq::LinqEndException();
            }
            /* get object */
            auto result = capi.template get<CascadeType>(*_storage.first,version);
            _storage.first++;
            for (auto& reply_future:result.get()) {
                auto object = reply_future.second.get();
                return object;
            }
            throw boolinq::LinqEndException();
        });
}

#endif//HAS_BOOLINQ

}// namespace cascade
}// namespace derecho
