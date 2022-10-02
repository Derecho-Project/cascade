#include <cascade/service_client_api.hpp>
#include <iostream>
#include <string>
#include <fstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "demo_common.hpp"

using namespace derecho::cascade;

/**
 * parse frames and upload this to Diary Farm front end tier as an external client.
 */
int main(int argc, char** argv) {
    const char* HELP_INFO = "--(f)ile <photo>  --(k)ey <the string key for the file>\n"
                            "--(h)elp";
    int c;
    static struct option long_options[] = {
        {"file",    required_argument,  0,  'f'},
        {"key",     required_argument,  0,  'k'},
        {"help",    no_argument,        0,  'h'},
        {0,0,0,0}
    };
    const char* file_name = nullptr;
    const char* key = nullptr;
    bool print_help = false;

    while(true){
        int option_index = 0;
        c = getopt_long(argc,argv,"f:k:h",long_options,&option_index);
        if (c == -1) {
            break;
        }

        switch (c) {
        case 'f':
            file_name = optarg;
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

    if (!(file_name && key)) {
        if (!print_help) {
            std::cout << "Invalid argument." << std::endl;
            std::cout << "Usage: " << argv[0] << " " << HELP_INFO << std::endl;
            return -1;
        }
    } else {
        // STEP 1: load file
        auto obj = get_photo_object(key, file_name);
        // STEP 2: send to server
        auto& capi = ServiceClientAPI::get_service_client();
        derecho::rpc::QueryResults<void> ret = capi.trigger_put(obj);
        ret.get();
        std::cout << "finish put to trigger put" << std::endl;
    }

    return 0;
}
