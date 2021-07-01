#include <cascade/user_defined_logic_interface.hpp>
#include <iostream>
#include <vector>
#include <opencv2/opencv.hpp>
#include <cppflow/cppflow.h>
#include "demo_udl.hpp"

namespace derecho{
namespace cascade{

#define MY_UUID     "22b86c6e-9d92-11eb-81d0-0242ac110002"
#define MY_DESC     "The Dairy Farm DEMO: Filter UDL."

std::string get_uuid() {
    return MY_UUID;
}

std::string get_description() {
    return MY_DESC;
}

#define FILTER_THRESHOLD       (0.9)
#define IMAGE_WIDTH            (352)
#define IMAGE_HEIGHT           (240)
#define FILTER_TENSOR_BUFFER_SIZE     (IMAGE_WIDTH*IMAGE_HEIGHT*3)
#define CONF_FILTER_MODEL             "filter_model"

class DairyFarmFilterOCDPO: public OffCriticalDataPathObserver {
    std::mutex p2p_send_mutex;

    virtual void operator () (const std::string& key_string,
                              const uint32_t prefix_length,
                              persistent::version_t version,
                              const mutils::ByteRepresentable* const value_ptr,
                              const std::unordered_map<std::string,bool>& outputs,
                              ICascadeContext* ctxt,
                              uint32_t worker_id) override {
        // TODO: test if there is a cow in the incoming frame.
        auto* typed_ctxt = dynamic_cast<CascadeContext<VolatileCascadeStoreWithStringKey,PersistentCascadeStoreWithStringKey,TriggerCascadeNoStoreWithStringKey>*>(ctxt);

#ifdef ENABLE_GPU
        /* Configure GPU context for tensorflow */
        if (typed_ctxt->resource_descriptor.gpus.size()==0) {
            dbg_default_error("Worker{}: GPU is requested but no GPU found...giving up on processing data.",worker_id);
            return;
        }
        std::cout << "Configuring tensorflow GPU context" << std::endl;
        // Serialized config options (example of 30% memory fraction)
        // TODO: configure gpu settings, link: https://serizba.github.io/cppflow/quickstart.html#gpu-config-options
        std::vector<uint8_t> config{0x32,0x9,0x9,0x9a,0x99,0x99,0x99,0x99,0x99,0xb9,0x3f};
        // Create new options with your configuration
        TFE_ContextOptions* options = TFE_NewContextOptions();
        TFE_ContextOptionsSetConfig(options, config.data(), config.size(), cppflow::context::get_status());
        // Replace the global context with your options
        cppflow::get_global_context() = cppflow::context(options);
#endif 
        
        /* step 1: load the model */ 
        static thread_local cppflow::model model(CONF_FILTER_MODEL);
        /* step 2: Load the image & convert to tensor */
        const TriggerCascadeNoStoreWithStringKey::ObjectType *tcss_value = reinterpret_cast<const TriggerCascadeNoStoreWithStringKey::ObjectType *>(value_ptr);
        FrameData *frame = reinterpret_cast<FrameData*>(tcss_value->blob.bytes);
        dbg_default_trace("frame photoid is: "+std::to_string(frame->photo_id));
        dbg_default_trace("frame timestamp is: "+std::to_string(frame->timestamp));
    
        std::vector<float> tensor_buf(FILTER_TENSOR_BUFFER_SIZE);
        std::memcpy(static_cast<void*>(tensor_buf.data()),static_cast<const void*>(frame->data), sizeof(frame->data));
        cppflow::tensor input_tensor(std::move(tensor_buf), {IMAGE_WIDTH,IMAGE_HEIGHT,3});
        input_tensor = cppflow::expand_dims(input_tensor, 0);
        
        /* step 3: Predict */
        cppflow::tensor output = model({{"serving_default_conv2d_3_input:0", input_tensor}},{"StatefulPartitionedCall:0"})[0];
        
        /* step 4: Send intermediate results to the next tier if image frame is meaningful */
        // prediction < 0.35 indicates strong possibility that the image frame captures full contour of the cow
        float prediction = output.get_data<float>()[0];
        std::cout << "prediction: " << prediction << std::endl;
        if (prediction < FILTER_THRESHOLD) {
            std::string frame_idx = key_string.substr(prefix_length);
            for (auto iter = outputs.begin(); iter != outputs.end(); ++iter) {
                std::string obj_key = iter->first + frame_idx;
                VolatileCascadeStoreWithStringKey::ObjectType obj(obj_key,tcss_value->blob.bytes,tcss_value->blob.size);
                std::lock_guard<std::mutex> lock(p2p_send_mutex);
                
                // if true, use trigger put; otherwise, use normal put
                if (iter->second) {
                    auto result = typed_ctxt->get_service_client_ref().template trigger_put<VolatileCascadeStoreWithStringKey>(obj);
                    result.get();
                    dbg_default_debug("finish put obj with key({})", obj_key);
                } 
                else {
                    auto result = typed_ctxt->get_service_client_ref().template put<VolatileCascadeStoreWithStringKey>(obj);
                    for (auto& reply_future:result.get()) {
                        auto reply = reply_future.second.get();
                        dbg_default_debug("node({}) replied with version:({:x},{}us)",reply_future.first,std::get<0>(reply),std::get<1>(reply));
                    }
                }
            }
        }
    }

    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
public:
    static void initialize() {
        if(!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<DairyFarmFilterOCDPO>();
        }
    }
    static auto get() {
        return ocdpo_ptr;
    }
};

std::shared_ptr<OffCriticalDataPathObserver> DairyFarmFilterOCDPO::ocdpo_ptr;

void initialize(ICascadeContext* ctxt) {
    DairyFarmFilterOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer() {
    return DairyFarmFilterOCDPO::get();
}

void release(ICascadeContext* ctxt) {
    // nothing to release
    return;
}

} // namespace cascade
} // namespace derecho
