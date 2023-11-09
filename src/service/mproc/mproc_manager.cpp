#include <cinttypes>

#include <string>
#include <unistd.h>
#include <rpc/server.h>
#include <wsong/ipc/ring_buffer.hpp>

#include <cascade/mproc/mproc_manager_api.hpp>

namespace derecho {
namespace cascade {
/**
 * @function
 * @param[in]   argc
 * @param[in]   argv
 * @return
 */
int mproc_main(int argc, char** argv) {
    ::rpc::server server(MPROC_MANAGER_HOST,MPROC_MANAGER_PORT);

    server.bind("start_udl",[](
            std::string     object_pool_path,
            std::string     uuid,
            std::string     udl_conf,
            uint32_t        execution_environment,
            std::string     execution_environment_conf,
            uint32_t        stateful,
            std::unordered_map<std::string,bool>
                            edges,
            key_t           shm_key) {

            return std::tuple<uint32_t,std::string,std::string,key_t>{0,"SUCCESS","n/a",0};
        }
    );

    server.run();
    return 0;
}

}
}

int main(int argc, char** argv) {
    return derecho::cascade::mproc_main(argc,argv);
}
