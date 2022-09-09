#include <cascade/user_defined_logic_interface.hpp>
#include <iostream>
#include <mutex>
#include <vector>
#include <opencv2/opencv.hpp>
#include <tensorflow/c/c_api.h>
#include <tensorflow/c/eager/c_api.h>
#include "demo_common.hpp"
#include "time_probes.hpp"
#include "config.h"

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
#define CONF_FILTER_MODEL             "filter-model"

#define CHECK_STATUS(tfs) \
    if (TF_GetCode(tfs) != TF_OK) { \
        std::runtime_error rerr(TF_Message(tfs)); \
        TF_DeleteStatus(tfs); \
        throw rerr; \
    }

class DairyFarmFilterOCDPO: public OffCriticalDataPathObserver {
    std::mutex p2p_send_mutex;

    virtual void operator () (const node_id_t,
                              const std::string& key_string,
                              const uint32_t prefix_length,
                              persistent::version_t version,
                              const mutils::ByteRepresentable* const value_ptr,
                              const std::unordered_map<std::string,bool>& outputs,
                              ICascadeContext* ctxt,
                              uint32_t worker_id) override {
        // test if there is a cow in the incoming frame.
        auto* typed_ctxt = dynamic_cast<DefaultCascadeContextType*>(ctxt);
#ifdef ENABLE_EVALUATION
        if (std::is_base_of<IHasMessageID,ObjectWithStringKey>::value) {
            global_timestamp_logger.log(TLT_FRONTEND_TRIGGERED,
                                        typed_ctxt->get_service_client_ref().get_my_id(),
                                        reinterpret_cast<const ObjectWithStringKey*>(value_ptr)->get_message_id(),
                                        get_walltime());
        }
#endif
        /* step 1: load the model */
        static thread_local std::unique_ptr<TF_Graph,decltype(TF_DeleteGraph)&> graph = {TF_NewGraph(), TF_DeleteGraph};
        static thread_local std::unique_ptr<TF_SessionOptions,decltype(TF_DeleteSessionOptions)&> session_options{TF_NewSessionOptions(), TF_DeleteSessionOptions};
        static thread_local std::unique_ptr<TF_Buffer,decltype(TF_DeleteBuffer)&> run_options{TF_NewBufferFromString("",0), TF_DeleteBuffer};
        static thread_local std::unique_ptr<TF_Buffer,decltype(TF_DeleteBuffer)&> meta_graph{TF_NewBuffer(), TF_DeleteBuffer};
        static thread_local auto session_deleter = [](TF_Session* sess) {
            auto tf_status = TF_NewStatus();
            TF_DeleteSession(sess, tf_status);
            CHECK_STATUS(tf_status);
            TF_DeleteStatus(tf_status);
        };
        static thread_local int tag_len = 1;
        static thread_local const char* tag = "serve";
        static thread_local std::unique_ptr<TF_Status,decltype(TF_DeleteStatus)&> tf_status{TF_NewStatus(),TF_DeleteStatus};
        static thread_local std::unique_ptr<TF_Session,decltype(session_deleter)&> session = 
            {
                TF_LoadSessionFromSavedModel(session_options.get(), run_options.get(), CONF_FILTER_MODEL,
                                             &tag, tag_len, graph.get(), meta_graph.get(), tf_status.get()),
                session_deleter
            };
        CHECK_STATUS(tf_status.get());
        static thread_local TF_Output input_op = {
            .oper = TF_GraphOperationByName(graph.get(),"serving_default_conv2d_3_input"),
            .index = 0
        };
        if (!input_op.oper) {
            throw std::runtime_error("No operation with name 'serving_default_conv2d_3_input' is found.");
        }
        static thread_local TF_Output output_op = {
            .oper = TF_GraphOperationByName(graph.get(),"StatefulPartitionedCall"),
            .index = 0
        };
        if (!output_op.oper) {
            throw std::runtime_error("No operation with name 'StatefulPartitionedCall' is found.");
        }

        /* step 2: Load the image & convert to tensor */
        static thread_local std::vector<int64_t> shape = {1,IMAGE_WIDTH,IMAGE_HEIGHT,3};
        const ObjectWithStringKey *tcss_value = reinterpret_cast<const ObjectWithStringKey *>(value_ptr);
        const FrameData *frame = reinterpret_cast<const FrameData*>(tcss_value->blob.bytes);
        dbg_default_trace("frame photoid is: "+std::to_string(frame->photo_id));
        dbg_default_trace("frame timestamp is: "+std::to_string(frame->timestamp));

        /* We do this copy because frame->data is const, which cannot be wrapped in a Tensor. */
        static thread_local float buf[FILTER_TENSOR_BUFFER_SIZE];
        std::memcpy(buf,frame->data,sizeof(frame->data));
  
        auto input_tensor = TF_NewTensor(TF_FLOAT,shape.data(),shape.size(),buf,sizeof(buf),
                                         [](void*,size_t,void*){/*do nothing for stack memory,.*/},nullptr);
        assert(input_tensor);
        
        /* step 3: Predict */
        static thread_local auto output_vals = std::make_unique<TF_Tensor*[]>(1);
        TF_SessionRun(session.get(),nullptr,&input_op, &input_tensor, 1,
                      &output_op, output_vals.get(), 1,
                      nullptr,0,nullptr,tf_status.get());
        CHECK_STATUS(tf_status.get());
        
        /* step 4: Send intermediate results to the next tier if image frame is meaningful */
        // prediction < 0.35 indicates strong possibility that the image frame captures full contour of the cow
        auto raw_data = TF_TensorData(*output_vals.get());
        float prediction = *static_cast<float*>(raw_data);
        TF_DeleteTensor(input_tensor);
        // std::cout << "prediction: " << prediction << std::endl;
#ifdef ENABLE_EVALUATION
        if (std::is_base_of<IHasMessageID,ObjectWithStringKey>::value) {
            global_timestamp_logger.log(TLT_FRONTEND_PREDICTED,
                                        typed_ctxt->get_service_client_ref().get_my_id(),
                                        tcss_value->get_message_id(),
                                        get_walltime());
        }
#endif
        if (prediction < FILTER_THRESHOLD) {
            std::string frame_idx = key_string.substr(prefix_length);
            for (auto iter = outputs.begin(); iter != outputs.end(); ++iter) {
                std::string obj_key = iter->first + frame_idx;
                ObjectWithStringKey obj(obj_key,tcss_value->blob.bytes,tcss_value->blob.size);
#ifdef ENABLE_EVALUATION
                if (std::is_base_of<IHasMessageID,std::decay_t<ObjectWithStringKey>>::value) {
                    obj.set_message_id(tcss_value->get_message_id());
                }
#endif
                std::lock_guard<std::mutex> lock(p2p_send_mutex);
                
                // if true, use trigger put; otherwise, use normal put
                if (iter->second) {
                    if (std::is_base_of<IHasMessageID,ObjectWithStringKey>::value) {
                        dbg_default_trace("trigger put output obj (key:{}, id:{}).", obj.get_key_ref(), obj.get_message_id());
                    }
                    auto result = typed_ctxt->get_service_client_ref().trigger_put(obj);
                    result.get();
                    if (std::is_base_of<IHasMessageID,ObjectWithStringKey>::value) {
                        dbg_default_trace("finish trigger put obj (key:{}, id{}).", obj.get_key_ref(), obj.get_message_id());
                    }
                } 
                else {
                    if (std::is_base_of<IHasMessageID,ObjectWithStringKey>::value) {
                        dbg_default_trace("put output obj (key:{}, id:{}).", obj.get_key_ref(), obj.get_message_id());
                    }
                    typed_ctxt->get_service_client_ref().put_and_forget(obj);
                    if (std::is_base_of<IHasMessageID,ObjectWithStringKey>::value) {
                        dbg_default_trace("finish put obj (key:{}, id{}).", obj.get_key_ref(), obj.get_message_id());
                    }
                }
            }
        }
#ifdef ENABLE_EVALUATION
        if (std::is_base_of<IHasMessageID,ObjectWithStringKey>::value) {
            global_timestamp_logger.log(TLT_FRONTEND_FORWARDED,
                                        typed_ctxt->get_service_client_ref().get_my_id(),
                                        tcss_value->get_message_id(),
                                        get_walltime());
        }
#endif
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
#ifdef ENABLE_GPU
    auto* typed_ctxt = dynamic_cast<DefaultCascadeContextType*>(ctxt);
    /* Configure GPU context for tensorflow */
    if (typed_ctxt->resource_descriptor.gpus.size()==0) {
        dbg_default_error("GPU is requested but no GPU found...giving up on processing data.");
        return;
    }
    std::cout << "Configuring tensorflow GPU context" << std::endl;
    initialize_tf_context();
#endif 
    DairyFarmFilterOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext*,const nlohmann::json&) {
    return DairyFarmFilterOCDPO::get();
}

void release(ICascadeContext* ctxt) {
    // nothing to release
    return;
}

} // namespace cascade
} // namespace derecho
