#include <cascade/user_defined_logic_interface.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <cascade/utils.hpp>
#include <mutex>
#include <nlohmann/json.hpp>
#include <iostream>
#include <memory>

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
            const node_id_t,
            const std::string& key_string,
            const uint32_t prefix_length,
            persistent::version_t version,
            const mutils::ByteRepresentable* const value_ptr,
            const std::unordered_map<std::string,bool>& outputs,
            ICascadeContext* ctxt,
            uint32_t worker_id) override {
        //TODO: implementing the pipeline logics.
        auto* typed_ctxt = dynamic_cast<DefaultCascadeContextType*>(ctxt);
        const auto* const value = dynamic_cast<const ObjectWithStringKey* const>(value_ptr);
#ifdef ENABLE_EVALUATION
        global_timestamp_logger.log(TLT_PIPELINE(stage),
            typed_ctxt->get_service_client_ref().get_my_id(),
            value->get_message_id(),
            get_walltime(),
            worker_id+stage*10000);
#endif//ENABLE_EVALUATION
        for (auto& okv:outputs) {
            std::string obj_key = okv.first;
            //TODO: why value_ptr is const???
            ObjectWithStringKey o(*value);
            o.key = obj_key + value->get_key_ref();
            o.set_previous_version(INVALID_VERSION,INVALID_VERSION);
            if (okv.second) {
                // TODO: how to decide the subgroup type of the put operations???
                // trigger put
                auto result = typed_ctxt->get_service_client_ref().trigger_put(o);
                result.get();
            } else {
                // normal put
                typed_ctxt->get_service_client_ref().put_and_forget(o);
            }
        }
    }

    uint32_t stage;

    static std::map<json,std::shared_ptr<OffCriticalDataPathObserver>> ocdpo_map;
    static std::mutex ocdpo_map_mutex;
public:
    /**
     * The constructor should receive a json configuration object like the following
     *  {
     *      "stage":1
     *  }
     *  where the stage represents which tier the node is in the whole pipeline.
     */
    PipelineOCDPO(const json& config) {
        try{
            if (config.find("stage") != config.end()) {
                stage = config["stage"].get<uint32_t>();
            } else {
                stage = 0;
            }
        } catch (json::exception& jsone) {
            dbg_default_error("Failed to parse pipeline configuration:{}, exception:{}",
                config.get<std::string>(), jsone.what());
        }
    }

    static void initialize() {
        // nothing to do
    }

    static auto get(const json& json_config) {
        std::lock_guard lck(ocdpo_map_mutex);
        if (ocdpo_map.find(json_config) == ocdpo_map.end()) {
            ocdpo_map.emplace(json_config,std::make_shared<PipelineOCDPO>(json_config));
        }
        return ocdpo_map.at(json_config);
    }
};

std::map<json,std::shared_ptr<OffCriticalDataPathObserver>> PipelineOCDPO::ocdpo_map;
std::mutex PipelineOCDPO::ocdpo_map_mutex;

void initialize(ICascadeContext* ctxt) {
    PipelineOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext* ctxt,
        const nlohmann::json& config) {
    return PipelineOCDPO::get(config);
}

void release(ICascadeContext* ctxt) {
    //TODO: release
}

} // namespace cascade
} // namespace derecho
