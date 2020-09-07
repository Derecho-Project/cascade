#include <boolinq/boolinq.h>
#include <iostream>
#include <vector>
#include <utility>
#include <cascade/service_client_api.hpp>

using namespace boolinq;
using namespace derecho::cascade;

/**
 * CascadeLinq
 */
template <typename CascadeType>
using TestCascadeShardLinqStorageType = std::pair<typename std::vector<typename CascadeType::KeyType>::iterator,typename std::vector<typename CascadeType::KeyType>::iterator>;

template <typename CascadeType, typename ServiceClientType>
using TestCascadeShardLinqType = boolinq::Linq<TestCascadeShardLinqStorageType<CascadeType>, typename CascadeType::ObjectType>;

/**
 * LINQ Creators
 */
template <typename CascadeType, typename ServiceClientType>
TestCascadeShardLinqType<CascadeType,ServiceClientType> from_cascade_shard(ServiceClientType& capi,
        std::vector<typename CascadeType::KeyType>& storage, uint32_t subgroup_index, uint32_t shard_index) {
    auto result = capi.template list_keys<CascadeType>(CURRENT_VERSION,subgroup_index,shard_index);
    for(auto& reply_future:result.get()) {
        storage = reply_future.second.get();
    }

    return TestCascadeShardLinqType<CascadeType,ServiceClientType> (
            std::make_pair(storage.begin(),storage.end()),
            [&capi,subgroup_index,shard_index] (TestCascadeShardLinqStorageType<CascadeType>& _storage) {
                if (_storage.first == _storage.second) {
                    throw boolinq::LinqEndException();
                }
                
                /* get object */
                auto result = capi.template get<CascadeType>(*_storage.first,CURRENT_VERSION,subgroup_index,shard_index);

                _storage.first++;

                for (auto& reply_future:result.get()) {
                    auto object = reply_future.second.get();
                    return object;
                }

                throw boolinq::LinqEndException();
            });
}

int main(int argc, char** argv) {
    std::cout << "boolinq test." << std::endl;

    std::vector<int> src = {1,2,3,4,5,6,7,8,9,10};
    auto dst = from(src).where([](int a){return a%2 == 1;}).toStdVector();
    std::cout << "type is:" << typeid(decltype(dst)).name() << std::endl;

    std::cout << "output:" << std::endl;
    for (auto x: dst) {
        std::cout << x << std::endl;
    }

    ServiceClientAPI capi;
    std::vector<uint64_t> storage;

    for (auto& x: from_cascade_shard<VCSU,ServiceClientAPI>(capi,storage,0,0).where([](VCSU::ObjectType o){return o.blob.size >= 3;}).toStdVector()) {
        std::cout << x << std::endl;
    }

    return 0;
}
