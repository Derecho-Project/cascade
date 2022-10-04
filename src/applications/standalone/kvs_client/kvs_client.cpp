#include <cascade/service_client_api.hpp>
#include <iostream>
#include <string>
#include <fstream>
#include <typeindex>
#include <sys/prctl.h>

using namespace derecho::cascade;

#define PROC_NAME "kvs_client"

int main(int argc, char** argv) {
    std::cout << "KVS Client Example in C++." << std::endl;

    std::cout << "1) Load configuration and connecting to cascade service..." << std::endl;
    ServiceClientAPI& capi = ServiceClientAPI::get_service_client();
    std::cout << "- connected." << std::endl;

#define OBJECT_FOLDER   "/vcss_objects"
    std::cout << "2) Create a folder, a.k.a. object pool in the first VolatileCascadeStore subgroup." << std::endl;
    auto result_2 = capi.template create_object_pool<VolatileCascadeStoreWithStringKey>(OBJECT_FOLDER,0);
    for (auto& reply_future:result_2.get()) {
        auto reply = reply_future.second.get();
        std::cout << "node(" << reply_future.first << ") replied with version:" << std::get<0>(reply)
                  << ",ts_us:" << std::get<1>(reply)
                  << std::endl;
    }
    std::cout << "- " << OBJECT_FOLDER <<" folder is created." << std::endl;

    std::cout << "3) List all folders a.k.a. object pools:" << std::endl;
    for (auto& op: capi.list_object_pools(true)) {
        std::cout << "\t" << op << std::endl;
    }

#define OBJECT_KEY      OBJECT_FOLDER "/obj_001"
    std::cout << "4) Put an object with key '" << OBJECT_KEY << "'" << std::endl;
#define OBJECT_VALUE    "value of " OBJECT_KEY
    ObjectWithStringKey obj;
    obj.key = OBJECT_KEY;
    obj.previous_version = INVALID_VERSION;
    obj.previous_version_by_key = INVALID_VERSION;
    obj.blob = Blob(reinterpret_cast<const uint8_t*>(OBJECT_VALUE),std::strlen(OBJECT_VALUE));
    auto result_4 = capi.put(obj);
    for (auto& reply_future:result_4.get()) {
        auto reply = reply_future.second.get();
        std::cout << "node(" << reply_future.first << ") replied with version:" << std::get<0>(reply)
                  << ",ts_us:" << std::get<1>(reply)
                  << std::endl;
    }

    std::cout << "5) Get an object with key '" << OBJECT_KEY << "'" << std::endl;
    auto result_5 = capi.get(std::string(OBJECT_KEY),CURRENT_VERSION);
    for (auto& reply_future:result_5.get()) {
        auto reply = reply_future.second.get();
        std::cout << "node(" << reply_future.first << ") replied with value:" << reply << std::endl;
    }
    return 0;
}
