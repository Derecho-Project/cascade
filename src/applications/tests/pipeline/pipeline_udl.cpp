#include <cascade/user_defined_logic_interface.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <iostream>

namespace derecho {
namespace cascade {

#define MY_UUID "b82ad3ee-254c-11ec-b081-0242ac110002"
#define MY_DESC "UDL for pipeline performance evaluation"

std::string get_uuid() {
    return MY_UUID;
}

std::string get_description() {
    return MY_DESC;
}

class PipelineOCDPO: public OffCriticalDataPathObserver {
    virtual void operator () (
            const std::string& key_string,
            const uint32_t prefix_length,
            persistent::version_t version,
            const mutils::ByteRepresentable* const value_ptr,
            const std::unordered_map<std::string,bool>& outputs,
            ICascadeContext* ctxt,
            uint32_t worker_id) override {
        //TODO: implementing the pipeline logics.
    }

    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
public:
    static void initialize() {
        if (!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<PipelineOCDPO>();
        }
    }
    static auto get() {
        return ocdpo_ptr;
    }
};

void initialize(ICascadeContext* ctxt) {
    PipelineOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext* ctxt,
        const std::string& pathname,
        const std::string& config) {
    return PipelineOCDPO::get();
}

void release(ICascadeContext* ctxt) {
    //TODO: release
}

} // namespace cascade
} // namespace derecho
