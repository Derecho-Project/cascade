#include <iostream>
#include <cascade/mproc/mproc_manager_api.hpp>

using namespace derecho::cascade;

int main(int argc, char** argv) {
    MProcManagerAPI mproc_api;
    mproc_mgr_req_start_udl_t req;
    mproc_mgr_res_start_udl_t res;
    mproc_api.start_udl(req,res);
    std::cout << "res.error_code = " << res.error_code << std::endl;
    std::cout << "res.info= " << res.info << std::endl;
    std::cout << "res.mproc_udl_id = " << res.mproc_udl_id << std::endl;
    std::cout << "res.rb_key = " << res.rb_key << std::endl;
    return 0;
}
