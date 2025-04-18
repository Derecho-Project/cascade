#include <cascade/object.hpp>
#include <derecho/conf/conf.hpp>

using namespace derecho::cascade;       
				
int main(int argc, char** argv) {
    std::cout << "Cascade OOB TEST C Node" << std::endl;
    auto& capi(ServiceClientAPI::get_service_client())
    void* oob_mr_ptr = nullptr; 
    size_t      oob_mr_size     = 1ul << 20;
    size_t      oob_data_size =256;
    oob_mr_ptr = aligned_alloc(4096,oob_mr_size);

    derecho::memory_attribute_t attr;
    attr.type = derecho::memory_attribute_t::SYSTEM;

    capi.oob_register_mem_ex(oob_mr_ptr,oob_mr_size,attr);
    auto rkey = capi.oob_rkey(oob_mr_ptr);
  	
    const char* arg = argv[1];
    uint64_t data_addr = std::strtoull(arg, nullptr, 10);
     for(const auto& member: group.get_members()){
	 if (member == group.get_my_id()) {
		 continue;
	     	}
	capi.oob_get_remote(member,0,data_addr,reinterpret_cast<uint64_t>(oob_mr_ptr), rkey,oob_data_size)
     }
     uint8_t* byte_ptr = reinterpret_cast<uint8_t*>(oob_mr_ptr);
     std::cout << "Recieved: " << static_cast<char>(byte_ptr[1]) << std::endl;
     std::cout << "Press ENTER to exit and trigger cleanup...\n";

     // Wait for user to hit ENTER
    std::cin.get(); // waits for input
    capi.oob_deregister_mem(oob_mr_ptr);
}


