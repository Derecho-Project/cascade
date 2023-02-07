#include <cascade/user_defined_logic_interface.hpp>
#include <iostream>
#include <fstream>
#include <vector>
#include <thread>
#include <opencv2/opencv.hpp>
#include <torch/script.h>
#include <ANN/ANN.h>
#include <tensorflow/c/c_api.h>
#include <tensorflow/c/eager/c_api.h>
#include "demo_common.hpp"
#include "time_probes.hpp"
#include "config.h"

namespace derecho{
namespace cascade{

#define MY_UUID     "6793c66c-9d92-11eb-9aa9-0242ac110002"
#define MY_DESC     "The Dairy Farm DEMO inference UDL."

std::string get_uuid() {
    return MY_UUID;
}

std::string get_description() {
    return MY_DESC;
}

#define K                    (5)			// number of nearest neighbors
#define DIM		             (128)			// dimension
#define EPS		             (0)			// error bound
#define MAX_PTS		         (5000)		    // maximum number of data points
#define COW_ID_IMAGE_WIDTH   (224)
#define COW_ID_IMAGE_HEIGHT  (224)
#define CONF_COWID_MODULE       "cow-id-model/resnet50_rtl.pt"
#define CONF_COWID_KNN          "cow-id-model/trainedKNN.dmp"
#define CONF_COWID_LABEL        "cow-id-model/synset.txt"

class InferenceEngine {
private: 
    torch::jit::script::Module module;
    
    torch::NoGradGuard no_grad;     // ensures that autograd is off
    
    int labels[MAX_PTS];

    // image embedding
    ANNpoint img_emb;

    // near neighbor indices
    ANNidxArray nnIdx = nullptr;

    // near neighbor distances
	ANNdistArray dists = nullptr;
    
    // search structure
	ANNkd_tree*	kdTree = nullptr;

    static std::mutex init_mutex;

    at::Tensor to_tensor(const cv::Mat& mat, bool unsqueeze=false, int unsqueeze_dim=0) {
        at::Tensor tensor_image = torch::from_blob(mat.data, {mat.rows,mat.cols,3}, at::kByte);
        if (unsqueeze) {
            tensor_image.unsqueeze_(unsqueeze_dim);
        }
        return tensor_image;
    }

    void load_module(const std::string& module_file) {
        try {
            module = torch::jit::load(module_file);
            module.eval();                          // turn off dropout and other training-time layers/functions
            dbg_default_trace("loaded module: "+module_file);
        } catch (const c10::Error& e) {
            std::cerr << "Error loading model\n";
            std::cerr << e.what_without_backtrace();
            return;
        }
    }

    void load_knn(const std::string& knn_file) {
        img_emb = annAllocPt(DIM);					// allocate image embedding
	    nnIdx = new ANNidx[K];						// allocate near neighbor indices
	    dists = new ANNdist[K];						// allocate near neighbor dists
        std::ifstream trainedKNN(knn_file);
        kdTree = new ANNkd_tree(trainedKNN);        // restore a copy of the old tree
        trainedKNN.close();
        dbg_default_trace("loaded knn: "+knn_file);
    }

    void load_labels(const std::string& label_file) {
        std::ifstream label_fs(label_file);
        if (!label_fs.is_open()) {
            std::cerr << "Could not open the file - '" << label_file << "'" << std::endl;
            return;
        }
        int count = 0;
        int x;
        while (count < MAX_PTS && label_fs >> x) {
            labels[count++] = x;
        }
        label_fs.close();
        dbg_default_trace("loaded label file: "+label_file);
    }
    
public:
    InferenceEngine(const std::string& module_file, const std::string& knn_file, const std::string& label_file) {
        std::lock_guard lck(init_mutex);
        load_module(module_file);
        load_knn(knn_file);
        load_labels(label_file);
    }

    uint32_t infer(const cv::Mat& mat) {
        // convert to tensor
        at::Tensor tensor = to_tensor(mat);
        tensor = tensor.toType(c10::kFloat).div(255);
        tensor = tensor.permute({2, 0, 1});
        //add batch dim (an inplace operation just like in pytorch)
        tensor.unsqueeze_(0);
        std::vector<torch::jit::IValue> inputs;
        inputs.push_back(tensor);
        dbg_default_trace("image is loaded");
        // execute model and package output as tensor
        at::Tensor output = module.forward(inputs).toTensor();
        for (int i = 0; i < DIM; i++) {
            img_emb[i] = output[0][i].item().to<double>();
        }
        kdTree->annkSearch(img_emb, K, nnIdx, dists, EPS);							
        // use the most nearest neighbor, could find mode of K nearest neighbor
        return static_cast<uint32_t>(labels[nnIdx[0]]);
    } 

    ~InferenceEngine() {
        debug_enter_func();
        if (nnIdx) {
            delete[] nnIdx;
        }
        if (dists) {
            delete[] dists;
        }
        if (kdTree) {
            delete kdTree;
        }
        annClose();
        debug_leave_func();
    }
};

std::mutex InferenceEngine::init_mutex;

void infer_cow_id(uint32_t* cow_id, const void* img_buf, size_t img_size) {
    static thread_local InferenceEngine cow_id_ie(CONF_COWID_MODULE, CONF_COWID_KNN, CONF_COWID_LABEL);
    std::vector<unsigned char> out_buf(img_size);
    std::memcpy(static_cast<void*>(out_buf.data()),img_buf,img_size);
    cv::Mat mat(240,352,CV_32FC3,out_buf.data());
    // resize to desired dimension matching with the model
    cv::Mat resized;
    cv::resize(mat, resized, cv::Size(COW_ID_IMAGE_WIDTH,COW_ID_IMAGE_HEIGHT));
    *cow_id = cow_id_ie.infer(resized);
}

#define BCS_IMAGE_HEIGHT           (300)
#define BCS_IMAGE_WIDTH            (300)
#define BCS_TENSOR_BUFFER_SIZE     (BCS_IMAGE_HEIGHT*BCS_IMAGE_WIDTH*3)
#define CONF_INFER_BCS_MODEL       "bcs-model"

#define CHECK_STATUS(tfs) \
    if (TF_GetCode(tfs) != TF_OK) { \
        std::runtime_error rerr(TF_Message(tfs)); \
        TF_DeleteStatus(tfs); \
        throw rerr; \
    }

void infer_bcs(float* bcs, const void* img_buf, size_t img_size) {
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
    static thread_local std::unique_ptr<TF_Session,decltype(session_deleter)&> session = {
        TF_LoadSessionFromSavedModel(session_options.get(), run_options.get(), CONF_INFER_BCS_MODEL,
                &tag, tag_len, graph.get(), meta_graph.get(), tf_status.get()),
        session_deleter
    };
    CHECK_STATUS(tf_status.get());
    static thread_local TF_Output input_op = {
        .oper = TF_GraphOperationByName(graph.get(),"serving_default_conv2d_5_input"),
        .index = 0
    };
    if (!input_op.oper) {
        throw std::runtime_error("No operation with name 'serving_default_conv2d_5_input' is found.");
    }
    static thread_local TF_Output output_op = {
        .oper = TF_GraphOperationByName(graph.get(),"StatefulPartitionedCall"),
        .index = 0
    };
    if (!output_op.oper) {
        throw std::runtime_error("No operation with name 'StatefulPartitionedCall' is found.");
    }

    /* step 2: Load the image & convert to tensor */
    static thread_local std::vector<unsigned char> out_buf(img_size);
    std::memcpy(static_cast<void*>(out_buf.data()),img_buf,img_size);
    cv::Mat mat(240,352,CV_32FC3,out_buf.data());
    static thread_local cv::Mat resized;
    // resize to desired dimension matching with the model & convert to tensor
    cv::resize(mat, resized, cv::Size(BCS_IMAGE_WIDTH,BCS_IMAGE_HEIGHT));
    static thread_local std::vector<int64_t> shape = {1,BCS_IMAGE_WIDTH,BCS_IMAGE_HEIGHT,3};
    auto input_tensor = TF_NewTensor(TF_FLOAT,shape.data(),shape.size(),resized.data,BCS_TENSOR_BUFFER_SIZE*sizeof(float),
                                     [](void*,size_t,void*){/*do nothing for stack memory,.*/},nullptr);

    /* step 3: Predict */
    static thread_local auto output_vals = std::make_unique<TF_Tensor*[]>(1);
    TF_SessionRun(session.get(),nullptr,&input_op, &input_tensor, 1,
                  &output_op, output_vals.get(), 1,
                  nullptr,0,nullptr,tf_status.get());
    CHECK_STATUS(tf_status.get());

    auto raw_data = TF_TensorData(*output_vals.get());
    float prediction = *static_cast<float*>(raw_data);
    TF_DeleteTensor(input_tensor);

    *bcs = prediction;
    dbg_default_trace("bcs prediction is: {}", prediction);
}

class DairyFarmInferOCDPO: public OffCriticalDataPathObserver {
private:
    mutable std::mutex p2p_send_mutex;

    virtual void operator () (const node_id_t,
                              const std::string& key_string,
                              const uint32_t prefix_length,
                              persistent::version_t version,
                              const mutils::ByteRepresentable* const value_ptr,
                              const std::unordered_map<std::string,bool>& outputs,
                              ICascadeContext* ctxt,
                              uint32_t worker_id) override {
        auto* typed_ctxt = dynamic_cast<DefaultCascadeContextType*>(ctxt);
#ifdef ENABLE_EVALUATION
        if (std::is_base_of<IHasMessageID,ObjectWithStringKey>::value) {
            TimestampLogger::log(TLT_COMPUTE_TRIGGERED,
                                        typed_ctxt->get_service_client_ref().get_my_id(),
                                        reinterpret_cast<const ObjectWithStringKey*>(value_ptr)->get_message_id(),
                                        get_walltime());
        }
#endif

        const VolatileCascadeStoreWithStringKey::ObjectType *vcss_value = reinterpret_cast<const VolatileCascadeStoreWithStringKey::ObjectType *>(value_ptr);
        const FrameData *frame = reinterpret_cast<const FrameData*>(vcss_value->blob.bytes);
        if (std::is_base_of<IHasMessageID,std::decay_t<decltype(*vcss_value)>>::value) {
            dbg_default_trace("frame photo {} (message id:{}) @ {}", frame->photo_id, vcss_value->get_message_id(), frame->timestamp);
        }

        // Inference threads
        uint32_t cow_id;
#ifdef ENABLE_EVALUATION
        if (std::is_base_of<IHasMessageID,ObjectWithStringKey>::value) {
            cow_id = static_cast<uint32_t>(reinterpret_cast<const ObjectWithStringKey*>(value_ptr)->get_message_id());
        } else {
            cow_id = 37;
        }
#else
        cow_id = 37;
#endif
        float bcs;
        // std::thread cow_id_inference(infer_cow_id, &cow_id, frame->data, sizeof(frame->data));
        // infer_cow_id(&cow_id, frame->data, sizeof(frame->data));
        // std::thread bcs_inference(infer_bcs, &bcs, frame->data, sizeof(frame->data));
        infer_bcs(&bcs,frame->data, sizeof(frame->data));
        // cow_id_inference.join();
        // bcs_inference.join();

        if (std::is_base_of<IHasMessageID,std::decay_t<decltype(*vcss_value)>>::value) {
            dbg_default_trace("frame photo {} (message id:{}) is processed.", frame->photo_id, vcss_value->get_message_id());
        }
#ifdef ENABLE_EVALUATION
        if (std::is_base_of<IHasMessageID,ObjectWithStringKey>::value) {
            TimestampLogger::log(TLT_COMPUTE_INFERRED,
                                        typed_ctxt->get_service_client_ref().get_my_id(),
                                        vcss_value->get_message_id(),
                                        get_walltime());
        }
#endif
        // put the result to next tier
        std::string frame_key = key_string.substr(prefix_length);
        std::string obj_value = std::to_string(bcs) + "_" + std::to_string(frame->timestamp);
        for (auto iter = outputs.begin(); iter != outputs.end(); ++iter) {
            std::string obj_key = iter->first + frame_key + PATH_SEPARATOR + std::to_string(cow_id);
            PersistentCascadeStoreWithStringKey::ObjectType obj(obj_key,reinterpret_cast<const uint8_t*>(obj_value.c_str()),obj_value.size());
#ifdef ENABLE_EVALUATION
            if (std::is_base_of<IHasMessageID,ObjectWithStringKey>::value) {
                obj.set_message_id(vcss_value->get_message_id());
            }
#endif
            std::lock_guard<std::mutex> lock(p2p_send_mutex);

            // if true, use trigger put; otherwise, use normal put
            if (iter->second) {
#ifdef ENABLE_EVALUATION
                if (std::is_base_of<IHasMessageID,ObjectWithStringKey>::value) {
                    dbg_default_trace("trigger put output obj (key:{}, id:{}).", obj.get_key_ref(), obj.get_message_id());
                }
#endif
                auto result = typed_ctxt->get_service_client_ref().trigger_put(obj);
                result.get();
#ifdef ENABLE_EVALUATION
                if (std::is_base_of<IHasMessageID,ObjectWithStringKey>::value) {
                    dbg_default_trace("finish trigger put obj (key:{}, id:{}).", obj.get_key_ref(), obj.get_message_id());
                }
#endif
            } 
            else {
#ifdef ENABLE_EVALUATION
                if (std::is_base_of<IHasMessageID,ObjectWithStringKey>::value) {
                    dbg_default_trace("put output obj (key:{}, id:{}).", obj.get_key_ref(), obj.get_message_id());
                }
#endif
                typed_ctxt->get_service_client_ref().put_and_forget(obj);
#ifdef ENABLE_EVALUATION
                if (std::is_base_of<IHasMessageID,ObjectWithStringKey>::value) {
                    dbg_default_trace("finish put obj (key:{}, id:{}).", obj.get_key_ref(), obj.get_message_id());
                }
#endif
            }
        }

#ifdef ENABLE_EVALUATION
        if (std::is_base_of<IHasMessageID,ObjectWithStringKey>::value) {
            TimestampLogger::log(TLT_COMPUTE_FORWARDED,
                                        typed_ctxt->get_service_client_ref().get_my_id(),
                                        reinterpret_cast<const ObjectWithStringKey*>(value_ptr)->get_message_id(),
                                        get_walltime());
        }
#endif

    }

    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
public:
    static void initialize() {
        if(!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<DairyFarmInferOCDPO>();
        }
    }
    static auto get() {
        return ocdpo_ptr;
    }
};

std::shared_ptr<OffCriticalDataPathObserver> DairyFarmInferOCDPO::ocdpo_ptr;

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
    DairyFarmInferOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext*,const nlohmann::json&) {
    return DairyFarmInferOCDPO::get();
}

void release(ICascadeContext* ctxt) {
    // nothing to release
    return;
}

} // namespace cascade
} // namespace derecho
