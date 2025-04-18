#include <cascade/object.hpp>
#include <derecho/conf/conf.hpp>

using namespace derecho::cascade;       
				
int main(int argc, char** argv) {
    std::cout << "Cascade OOB TEST A Node" << std::endl;
    auto& capi(ServiceClientAPI::get_service_client())
    void* oob_mr_ptr = nullptr; 
    size_t      oob_mr_size     = 1ul << 20;
    size_t      oob_data_size =256;
    oob_mr_ptr = aligned_alloc(4096,oob_mr_size);
    void* get_buffer_laddr = reinterpret_cast<void*>(
		        ((reinterpret_cast<uint64_t>(oob_mr_ptr) + oob_data_size + 4095) >> 12) << 12
		    );
    memset(get_buffer_laddr, 'a', oob_data_size);

    derecho::memory_attribute_t attr;
    attr.type = derecho::memory_attribute_t::SYSTEM;

    capi.oob_register_mem_ex(oob_mr_ptr,oob_mr_size,attr);
    
    std::cout << "a written at" << reinterpret_cast<uint64_t>(get_buffer_laddr) << std::endl;
    std::cout << "Press ENTER to exit and trigger cleanup...\n";

     // Wait for user to hit ENTER
      std::cin.get(); // waits for input
    capi.oob_deregister_mem(oob_mr_ptr);
}


