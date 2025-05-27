#include <cascade/user_defined_logic_interface.hpp>
#include <iostream>
#include <cascade/utils.hpp>
namespace derecho{
namespace cascade{

#define MY_UUID     "48e60f7c-8500-11eb-8755-0242ac110002"
#define MY_DESC     "Demo DLL UDL that allocates CPU memory and performs Single Sided RDMA"

std::string get_uuid() {
    return MY_UUID;
}

std::string get_description() {
    return MY_DESC;
}

class OOBOCDPO: public OffCriticalDataPathObserver {
   
	void* oob_mr_ptr = nullptr; 
       
	virtual void operator () (const derecho::node_id_t sender,
                              const std::string& key_string,
                              const uint32_t prefix_length,
                              persistent::version_t version,
                              const mutils::ByteRepresentable* const value_ptr,
                              const std::unordered_map<std::string,bool>& outputs,
                              ICascadeContext* ctxt,
                              uint32_t worker_id) override {
       auto* typed_ctxt = dynamic_cast<DefaultCascadeContextType*>(ctxt);

	    std::cout << "[OOB]: I(" << worker_id << ") received an object from sender:" << sender << " with key=" << key_string 
                  << ", matching prefix=" << key_string.substr(0,prefix_length) << std::endl;
       auto tokens = str_tokenizer(key_string);
       if (tokens[1] == "send"){
/**
       	       cudaSetDevice(device_id);
	
	size_in_bytes = (std::stoul("8") << 20);	
      	void* dev_ptr = nullptr;
         cudaError_t err = cudaMalloc(&dev_ptr, size_in_bytes);
   	 if (err != cudaSuccess) {
        	std::cerr << "cudaMalloc failed: " << cudaGetErrorString(err) << std::endl;
   	 }
         err = cudaMemset(dev_ptr, value, size_in_bytes);
         if (err != cudaSuccess) {
                std::cerr << "cudaMemset failed: " << cudaGetErrorString(err) << std::endl;
                cudaFree(dev_ptr); // Clean up
         }
*/
	       void* oob_mr_ptr = nullptr; 
	       size_t      oob_mr_size     = 1ul << 20;
		size_t      oob_data_size =256;
		           oob_mr_ptr = aligned_alloc(4096,oob_mr_size);
			       void* get_buffer_laddr = static_cast<uint8_t*>(oob_mr_ptr);
     				       memset(get_buffer_laddr, 'a', oob_data_size);

				       derecho::memory_attribute_t attr;
				           attr.type = derecho::memory_attribute_t::SYSTEM;

					  typed_ctxt->get_service_client_ref().oob_register_mem_ex(oob_mr_ptr,oob_mr_size,attr);
					  Blob blob(reinterpret_cast<const uint8_t*>(get_buffer_laddr), oob_data_size); 
					  ObjectWithStringKey obj ("oob/receive",blob);
					  std::cout << "SEND" << std::endl;
      					typed_ctxt->get_service_client_ref().put_and_forget<VolatileCascadeStoreWithStringKey>(obj,0,1); 
       				         std::cout << "SEND put worked!" << std::endl; 				
       }
       else if(tokens[1] == "receive"){
		
	    size_t      oob_mr_size     = 1ul << 20;
	        size_t      oob_data_size =256;
		    oob_mr_ptr = aligned_alloc(4096,oob_mr_size);

		        derecho::memory_attribute_t attr;
			    attr.type = derecho::memory_attribute_t::SYSTEM;

			        typed_ctxt->get_service_client_ref().oob_register_mem_ex(oob_mr_ptr,oob_mr_size,attr);
				    auto rkey =  typed_ctxt->get_service_client_ref().oob_rkey(oob_mr_ptr);
				  /**  char arr [value_ptr->bytes_size()] = {};
                              value_ptr->to_bytes(arr);
			      uint64_t result;
			      std::memcpy(&result, arr, sizeof(uint64_t));
			*/	
				const ObjectWithStringKey* object = dynamic_cast<const ObjectWithStringKey*>(value_ptr);
				uint64_t result = *reinterpret_cast<const uint64_t*>(object->blob.bytes);
				std::cout << "RECEIVE" << std::endl;
			      	typed_ctxt->get_service_client_ref().oob_get_remote<VolatileCascadeStoreWithStringKey>(0,0,result,reinterpret_cast<uint64_t>(oob_mr_ptr), rkey,oob_data_size);
        std::cout << "RECEIVE UDL handeling has called oob_get_remote" << std::endl; 			
       }
       else if (tokens[1] == "check"){
	       std::cout << "CHECK" << std::endl;
	uint8_t* byte_ptr = reinterpret_cast<uint8_t*>(oob_mr_ptr);
	std::cout << "Recieved: " << static_cast<char>(byte_ptr[1]) << std::endl;
       } else {
	std::cout << "Unsupported oob operation called!" << std::endl;
       }
    }

    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
public:
    static void initialize() {
        if(!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<OOBOCDPO>();
        }
    }
    static auto get() {
        return ocdpo_ptr;
    }
};

std::shared_ptr<OffCriticalDataPathObserver> OOBOCDPO::ocdpo_ptr;

void initialize(ICascadeContext* ctxt) {
    OOBOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext*,const nlohmann::json&) {
    return OOBOCDPO::get();
}

void release(ICascadeContext* ctxt) {
    // nothing to release
    return;
}

} // namespace cascade
} // namespace derecho
