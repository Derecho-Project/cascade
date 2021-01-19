#include <cascade/service_client_api.hpp>
#include <iostream>
#include <string>
#include <fstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sstream>
#include <vector>

using namespace derecho::cascade;

VolatileCascadeStoreWithStringKey::ObjectType get_photo_object(const char* type, const char* key, const char* photo_file) {
    int fd;
    struct stat st;
    void* file_data;

    // open and map file
    if(stat(photo_file, &st) || access(photo_file, R_OK)) {
		std::cerr << "file " << photo_file << " is not readable." << std::endl;
		return VolatileCascadeStoreWithStringKey::ObjectType::IV;
    }

    if((S_IFMT & st.st_mode) != S_IFREG) {
		std::cerr << photo_file << " is not a regular file." << std::endl;
		return VolatileCascadeStoreWithStringKey::ObjectType::IV;
    }

    if((fd = open(photo_file, O_RDONLY)) < 0) {
        std::cerr << "Failed to open file(" << photo_file << ") in readonly mode with "
                  << "error:" << strerror(errno) << "." << std::endl;
        return VolatileCascadeStoreWithStringKey::ObjectType::IV;
    }

    if((file_data = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE,
                         fd, 0))
       == MAP_FAILED) {
        std::cerr << "Failed to map file(" << photo_file << ") with "
                  << "error:" << strerror(errno) << "." << std::endl;
        return VolatileCascadeStoreWithStringKey::ObjectType::IV;
    }
    
    // create Object
    return VolatileCascadeStoreWithStringKey::ObjectType(std::string(type)+"/"+key,static_cast<const char*>(file_data),st.st_size);
}

auto parse_file_list(const char* type, const char* files) {
    std::vector<VolatileCascadeStoreWithStringKey::ObjectType> vec;
    std::istringstream fs(files);
    std::string f;
    int key = 0;
    while(std::getline(fs,f,':')){
        vec.push_back(get_photo_object(type,std::to_string(key).c_str(),f.c_str()));
    }
    return vec;
}

/**
 * The cnn classifier client post photos to cascade to be processed by the cnn classifier data path logic.
 */
int main(int argc, char** argv) {
    const char* HELP_INFO = "--(t)ype <pet|flower> --(f)iles <file1:file2:file3...>\n"
                            "--(h)elp";
    int c;
    static struct option long_options[] = {
        {"files",   required_argument,  0,  'f'},
        {"type",    required_argument,  0,  't'},
        {"help",    no_argument,        0,  'h'},
        {0,0,0,0}
    };
    const char* files = nullptr;
    const char* type = nullptr;
    bool print_help = false;

    while(true){
        int option_index = 0;
        c = getopt_long(argc,argv,"f:t:h",long_options,&option_index);
        if (c == -1) {
            break;
        }

        switch (c) {
        case 'f':
            files = optarg;
            break;
        case 't':
            type = optarg;
            break;
        case 'h':
            print_help = true;
            std::cout << "Usage: " << argv[0] << " " << HELP_INFO << std::endl;
            break;
        }
    }

    if (!(files && type)) {
        if (!print_help) {
            std::cout << "Invalid argument." << std::endl;
            std::cout << "Usage: " << argv[0] << " " << HELP_INFO << std::endl;
            return -1;
        }
    } else {
        auto vec_photos = parse_file_list(type,files);
        const size_t vec_size = vec_photos.size();
        ServiceClientAPI capi;
        // TODO: change this to asynchronous send.
        for(int i=0;i<100;i++) {
            auto ret = capi.template put<VolatileCascadeStoreWithStringKey>(vec_photos.at(i%vec_size), 0, 0);

            for (auto& reply_future:ret.get()) {
                auto reply = reply_future.second.get();
                std::cout << "node(" << reply_future.first << ") replied with version:" << std::get<0>(reply)
                          << ",ts_us:" << std::get<1>(reply) << std::endl;
            }
        }
/*
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
*/
    }

    return 0;
}
