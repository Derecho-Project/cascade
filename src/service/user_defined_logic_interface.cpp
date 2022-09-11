#include <cascade/user_defined_logic_interface.hpp>

namespace derecho {
namespace cascade {

    void DefaultOffCriticalDataPathObserver::operator() (
            const node_id_t sender,
            const std::string& full_key_string,
            const uint32_t prefix_length,
            persistent::version_t,
            const mutils::ByteRepresentable* const value_ptr,
            const std::unordered_map<std::string,bool>& outputs,
            ICascadeContext* ctxt,
            uint32_t worker_id) {
        auto* typed_ctxt = dynamic_cast<DefaultCascadeContextType*>(ctxt);
        const auto* object_ptr = dynamic_cast<const ObjectWithStringKey*>(value_ptr);
        std::string object_pool_pathname = full_key_string.substr(0,prefix_length);
        while (object_pool_pathname.back() == PATH_SEPARATOR && !object_pool_pathname.empty()) {
            object_pool_pathname.pop_back();
        }
        std::string key_string = full_key_string.substr(prefix_length);

        // call typed handler
        this->ocdpo_handler(
                sender,
                object_pool_pathname,
                key_string,
                *object_ptr,
                [&](const std::string& key, const Blob& blob) {
                    for (const auto& okv: outputs) {
                        std::string prefix = okv.first;
                        while (!prefix.empty() && prefix.back() == PATH_SEPARATOR) prefix.pop_back();
                        std::string new_key = (prefix.empty()? key : prefix+PATH_SEPARATOR+key);
                        // emplace constructor to avoid copy:
                        ObjectWithStringKey obj_to_send(
#ifdef ENABLE_EVALUATION
                                0,
#endif
                                INVALID_VERSION,
                                0ull,
                                INVALID_VERSION,
                                INVALID_VERSION,
                                new_key,
                                blob,
                                true);
                        if (okv.second) {
                            typed_ctxt->get_service_client_ref().trigger_put(obj_to_send);
                        } else {
                            typed_ctxt->get_service_client_ref().put_and_forget(obj_to_send);
                        }
                    }
                },
                typed_ctxt,
                worker_id);
    }
}
}
