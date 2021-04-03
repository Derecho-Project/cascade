#include <cascade/service_client_api.hpp>
#include <iostream>
#include <string>
#include <fstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include "cnn_classifier_dpl.hpp"

using namespace derecho::cascade;

/**
 * The cnn classifier client post photos to cascade to be processed by the cnn classifier data path logic.
 */
int main(int argc, char** argv) {
    const char* HELP_INFO = "--(f)ile <photo> --(t)ype <pet|flower> [--(k)ey <the string key for the file, default value is the filename>]\n"
                            "--(h)elp";
    int c;
    static struct option long_options[] = {
        {"file",    required_argument,  0,  'f'},
        {"type",    required_argument,  0,  't'},
        {"key",     required_argument,  0,  'k'},
        {"help",    no_argument,        0,  'h'},
        {0,0,0,0}
    };
    const char* file_name = nullptr;
    const char* type = nullptr;
    const char* key = nullptr;
    bool print_help = false;

    while(true){
        int option_index = 0;
        c = getopt_long(argc,argv,"f:t:k:h",long_options,&option_index);
        if (c == -1) {
            break;
        }

        switch (c) {
        case 'f':
            file_name = optarg;
            break;
        case 't':
            type = optarg;
            break;
        case 'k':
            key = optarg;
            break;
        case 'h':
            print_help = true;
            std::cout << "Usage: " << argv[0] << " " << HELP_INFO << std::endl;
            break;
        }
    }

    if (!(file_name && type && key)) {
        if (!print_help) {
            std::cout << "Invalid argument." << std::endl;
            std::cout << "Usage: " << argv[0] << " " << HELP_INFO << std::endl;
            return -1;
        }
    } else {
        // STEP 1: load file
        auto obj = get_photo_object(type, key, file_name);
        // STEP 2: send to server
        ServiceClientAPI capi;
        // send to the subgroup 0, shard 0
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> ret = capi.template put<VolatileCascadeStoreWithStringKey>(obj, 0, 0);
        for (auto& reply_future:ret.get()) {
            auto reply = reply_future.second.get();
            std::cout << "node(" << reply_future.first << ") replied with version:" << std::get<0>(reply)
                      << ",ts_us:" << std::get<1>(reply) << std::endl;
        }
    }

    return 0;
}
