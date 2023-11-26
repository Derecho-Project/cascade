#include "mproc_udl_client.hpp"

using namespace derecho::cascade;

int main(int argc, char** argv) {
    std::cout << argv[0] << " is an mproc tester client." << std::endl;
    auto client = MProcUDLClient<CASCADE_SUBGROUP_TYPE_LIST>::create(0xabcd0123);

    ObjectWithStringKey obj(std::string("MyObjectKey"),
        [](uint8_t* buf,const std::size_t size){
            memset(static_cast<void*>(buf),'A',size);
            return size;
        },
        32);

    std::unordered_map<std::string,bool> outputs{{"/to/pool1/",true},{"/to/pool2/",false}};

    client->submit(16,                  // sender_id
                   "/full/key/string",  // full_key_string
                   10,                  // prefix_length
                   100,                 // version
                   &obj,                 // value
                   outputs,             // outputs
                   32);                 // worker id
    std::cout << "message sent." << std::endl;
    return 0;
}
