#pragma once
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
 */
#if HAS_BOOLINQ

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
    auto result = capi.template list_keys_by_time<CascadeType>(ts_us, subgroup_index, shard_index);
    for(auto& reply_future:result.get()) {
        key_list = reply_future.second.get();
    }
	/* set up storage and nextFunc*/
    return CascadeShardLinq<CascadeType,ServiceClientType>(capi,subgroup_index,shard_index,CURRENT_VERSION,key_list,
        [&capi,subgroup_index,shard_index,ts_us](CascadeShardLinqStorageType<CascadeType>& _storage) {
            if (_storage.first == _storage.second) {
                throw boolinq::LinqEndException();
            }

            /* get object */
            auto result = capi.template get_by_time<CascadeType>(*_storage.first,ts_us,subgroup_index,shard_index);
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
		
            // while (ver != INVALID_VERSION) {
            //     auto not_null = true;
            //     persistent::version_t cur_ver = ver;

			//     /* decide if the object is null */
            //     auto result_check_null = capi.template get<CascadeType>(key,ver,subgroup_index,shard_index);
            //     for (auto& reply_future:result_check_null.get()) {
            //         auto object = reply_future.second.get();
            //         ver = object.previous_version_by_key; 
            //         not_null = not_null && !object.is_null();
            //     }

            //     std::cout << not_null;
            //     /* get object */
            //     if (not_null) {
            //         auto result_get_obj = capi.template get<CascadeType>(key,cur_ver,subgroup_index,shard_index);
            //         for (auto& reply_future:result_get_obj.get()) {
            //             auto object = reply_future.second.get();
            //             return  object;
            //         }
            //     } 
            // }

            do {
                auto result = capi.template get<CascadeType>(key,ver,subgroup_index,shard_index);
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
	ServiceClientType& capi, derecho::subgroup_id_t subgroup_id, uint32_t sgidx, persistent::version_t ver) {

    /* get number of shards in the current subgroup. */
    uint32_t subgroup_member_size = 0;
    uint32_t member_size = capi.template get_members().size();
    auto cur_subgroup_members = capi.template get_shard_members(subgroup_id, 0);
    if (cur_subgroup_members[0] + cur_subgroup_members.size() == member_size) {
        subgroup_member_size = cur_subgroup_members.size();
    } else {
        subgroup_member_size = capi.template get_shard_members(subgroup_id+1, 0)[0] - cur_subgroup_members[0];
    }
    for (uint32_t shidx = 0; shidx < subgroup_member_size; ++shidx) {
        shard_linq_list.push_back(from_shard<CascadeType, ServiceClientType>(shardidx_to_keys[shidx],capi,sgidx,shidx,ver));
    }

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

#endif//HAS_BOOLINQ

}// namespace cascade
}// namespace derecho
