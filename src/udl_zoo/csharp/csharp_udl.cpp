#include <cascade/user_defined_logic_interface.hpp>
#include <iostream>
#include <cascade/object.hpp>
#include "gateway_to_managed.hpp"
#include <unistd.h>
#include <filesystem>
#include <functional>
#include <cascade/service_client_api.hpp>

/**
 * README
 *
 * This file implements the C# UDL wrapper for Cascade.
 *
 */

namespace derecho {
namespace cascade {

struct DLLPathModulePair {
    std::string dll_absolute_path;
    std::string module_name;
    DLLPathModulePair(const std::string& path, const std::string& module_name) {
        this->dll_absolute_path = path;
        this->module_name = module_name;
    }

    bool operator==(const DLLPathModulePair& rhs) const {
        return dll_absolute_path == rhs.dll_absolute_path &&
            module_name == rhs.module_name;
    }
};

#define MY_UUID "3fc0bfc9-ae62-4b57-b39d-af3f83e7f429"
#define MY_DESC "Wrapper DLL UDL responsible for invoking C# logic."

std::string get_uuid() { 
    return MY_UUID; 
}

std::string get_description() { 
    return MY_DESC; 
}

#define CSUDL_CONF_CSHARP_PATH "csharp_relative_path"
#define CSUDL_CONF_MODULE_NAME "module"
#define CSUDL_CONF_OBJECT_POOL_PATHNAME "pathname"

class CSharpOCDPO : public DefaultOffCriticalDataPathObserver {
    DLLPathModulePair dll_metadata;

    virtual void ocdpo_handler(const node_id_t sender,
                               const std::string& object_pool_pathname,
                               const std::string& key_string,
                               const ObjectWithStringKey& object,
                               const emit_func_t& emit,
                               DefaultCascadeContextType* typed_ctxt,
                               uint32_t worker_id) override {
      std::cout << "[csharp ocdpo]: calling into managed code from sender="
                << sender << " with key=" << key_string << std::endl;
      uint64_t start_us =
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::high_resolution_clock::now().time_since_epoch())
              .count();
      gateway->Invoke(dll_metadata.dll_absolute_path.c_str(), dll_metadata.module_name.c_str(),
                      {sender, object_pool_pathname.c_str(), key_string.c_str(), object.key.c_str(),
                      object.blob.bytes, object.blob.bytes_size(), worker_id, &emit},
                      [](const emit_func_t* emit_ptr, const char* key,
                         const uint8_t* bytes, const uint32_t size) {
                            Blob blob_wrapper(bytes, size, true);
                            (*emit_ptr)(std::string(key), EMIT_NO_VERSION_AND_TIMESTAMP, blob_wrapper);
                      });
      uint64_t end_us =
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::high_resolution_clock::now().time_since_epoch())
              .count();

      std::cout << "[csharp ocdpo]: EXECUTION TIME. start: " << start_us
                << " end: " << end_us << std::endl;
      std::cout << "[csharp ocdpo]: TOTAL TIME. " << end_us - start_us
                << std::endl;
    }
    
    /* ---- static members follow ---- */
 private:
    /* singleton attributes */
    static std::atomic<bool> gateway_is_initialized;
    static std::mutex gateway_initialization_mutex;
    static std::unique_ptr<GatewayToManaged> gateway;
    static ServiceClientAPI& capi;

 public:
    /**
         *  The constructor
         */
    CSharpOCDPO(const DLLPathModulePair& _dll_metadata)
        : dll_metadata(_dll_metadata) {}

    /**
         *  The destructor
         */
    virtual ~CSharpOCDPO() {
      // TODO
    }

    static void initialize() {
        std::cout << "[csharp ocdpo] initializing..." << std::endl;
        if (gateway_is_initialized) {
            return;
        }
        std::lock_guard<std::mutex> lock(gateway_initialization_mutex);
        if (!gateway_is_initialized) {
            std::filesystem::path cfg_absolute_path = get_current_working_dir();
            cfg_absolute_path = cfg_absolute_path.parent_path();
            gateway = std::make_unique<GatewayToManaged>();
            gateway->Init(cfg_absolute_path);
            gateway_is_initialized = true;
        }
    }

    static void release() {
        if (gateway_is_initialized) {
            std::lock_guard<std::mutex> lock(gateway_initialization_mutex);
            if (!gateway_is_initialized) {
                return;
            }
            gateway->Close();
            gateway = nullptr;
            gateway_is_initialized = false;
        }
    }

    static std::string get_current_working_dir() {
        char buf[FILENAME_MAX+1];
        return std::string{getcwd(buf,FILENAME_MAX + 1)};
    }
};

std::atomic<bool> CSharpOCDPO::gateway_is_initialized = false;
std::mutex CSharpOCDPO::gateway_initialization_mutex;
std::unique_ptr<GatewayToManaged> CSharpOCDPO::gateway;
ServiceClientAPI& CSharpOCDPO::capi = ServiceClientAPI::get_service_client();

void initialize(ICascadeContext* ctxt) { 
    CSharpOCDPO::initialize(); 
}

/*
* This will be called for each UDL(CSharpOCDPO) instance.
*/
std::shared_ptr<OffCriticalDataPathObserver> get_observer(
      ICascadeContext*, const nlohmann::json& conf) {
    dbg_default_trace("get_observer() is called with conf={}", conf.dump());

    std::string module_name;
    if (conf.contains(CSUDL_CONF_MODULE_NAME)) {
        module_name = conf[CSUDL_CONF_MODULE_NAME].get<std::string>();
    }

    std::string relative_path;
    if (conf.contains(CSUDL_CONF_CSHARP_PATH)) {
        relative_path = conf[CSUDL_CONF_CSHARP_PATH].get<std::string>();
    }

    std::filesystem::path node_absolute_path = CSharpOCDPO::get_current_working_dir();
    return std::make_shared<CSharpOCDPO>(DLLPathModulePair(
        (node_absolute_path / relative_path).string(), module_name));
}

void release(ICascadeContext* ctxt) { 
    CSharpOCDPO::release(); 
}

}    // namespace cascade
}    // namespace derecho
