#include <cascade/mproc/mproc_manager_api.hpp>

namespace derecho {
namespace cascade {

MProcManagerAPI::MProcManagerAPI():
    client(MPROC_MANAGER_HOST,MPROC_MANAGER_PORT) {}

void MProcManagerAPI::start_udl(const mproc_mgr_req_start_udl_t& req, mproc_mgr_res_start_udl_t& res) {
    auto future = client.async_call("start_udl",
            req.object_pool_path,req.uuid,req.udl_conf.dump(),
            static_cast<uint32_t>(req.execution_environment),
            req.execution_environment_conf.dump(),
            static_cast<uint32_t>(req.stateful),
            req.edges,req.shm_key);
    auto _res = future.get().as<std::tuple<uint32_t,std::string,std::string,key_t>>();
    res.error_code = std::get<0>(_res);
    res.info = std::get<1>(_res);
    res.mproc_udl_id = std::get<2>(_res);
    res.rb_key = std::get<3>(_res);
}

}
}
