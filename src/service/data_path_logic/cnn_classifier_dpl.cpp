#include <cascade/service_server_api.hpp>
#include <iostream>
#include <mxnet-cpp/MxNetCpp.h>
#include <opencv2/opencv.hpp>
#include <vector>

/**
 * This is an example for the filter/trigger data path logic with ML model serving. In this example, we process incoming
 * photos/video frames based on its keys. If a key matches "pet/...", this object will trigger a categorizer to tell
 * the breed of the pet; If a key matches "flower/...", this object will trigger a categorizer to tell the name of the
 * flower picture. The result will be stored in a persisted cascade subgroup.
 *
 * We create an environment with the following layout:
 * - Subgroup VCSU:0 - not used.
 * - Subgroup VCSS:0 - categorizer subgroup processes the incoming data and send the result to PCSS:0. VCSS:0 has one
 *                     two-nodes shard for this subgroup. The two nodes process partition the key space bashed on
 *                     a hash function.
 * - Subgroup PCSU:0 - not used.
 * - Subgroup PCSS:0 - persisted tag shard, which stores all the tags. The keys mirror those in VCSS:0. PCSS:0 has one
 *                     three-node shard for this subgroup.
 *
 * TODO: critical data path --> filter/dispatcher; off critical data path --> trigger
 */
namespace derecho{
namespace cascade{
void on_cascade_initialization() {
    std::cout << "[cnn_classifier example]: initialize the data path library here." << std::endl;
}

void on_cascade_exit() {
    std::cout << "[cnn_classifier example]: destroy data path environment before exit." << std::endl;
}


#define AT_UNKNOWN      (0)
#define AT_PET_BREED    (1)
#define AT_FLOWER_NAME  (2)
/**
 * StaticActionTable translates the key to action type:
 *
 */
class StaticActionTable {
    struct _static_action_table_entry {
        const std::string prefix;
        const uint64_t action_id;
    };
    /* mapping from prefix to ACTION TYPE */
    const std::vector<struct _static_action_table_entry> table;
public:
    StaticActionTable() : table({{"pet",AT_PET_BREED},{"flower",AT_FLOWER_NAME}}) {}
    uint64_t to_action(const std::string& key) {
        uint64_t aid = AT_UNKNOWN;
        for (const auto& entry: table) {
            if (key.find(entry.prefix) == 0) {
                aid = entry.action_id;
                break;
            }
        }
        return aid;
    }
};
static StaticActionTable static_action_table;
/*
 * The image frame data in predefined 224x224 pixel format.
 */
class ImageFrame: public ActionData,public Blob {
public:
    std::string key;
    ImageFrame(const std::string& k, const Blob& other): Blob(other), key(k) {}
};

enum TypeFlag {
    kFloat32 = 0,
    kFloat64 = 1,
    kFloat16 = 2,
    kUint8 = 3,
    kInt32 = 4,
    kInt8 = 5,
    kInt64 = 6,
};

template <typename CascadeType>
class ClassifierFilter: public CriticalDataPathObserver<CascadeType> {
    virtual void operator() (const uint32_t sgidx,
                             const uint32_t shidx,
                             const typename CascadeType::KeyType& key,
                             const typename CascadeType::ObjectType& value, ICascadeContext* cascade_ctxt) {
        std::cout << "[cnn_classifier filter] I saw data: ["
                  << "KT = " << typeid(typename CascadeType::KeyType).name()
                  << ", VT = " << typeid(typename CascadeType::ObjectType).name()
                  << "] in subgroup(" << sgidx
                  << "), shard(" << shidx
                  << "). key = " << key
                  << " and value = " << value
                  << " . cascade_ctxt = " << cascade_ctxt 
                  << std::endl;
        auto* ctxt = dynamic_cast<CascadeContext<VCSU,VCSS,PCSU,PCSS>*>(cascade_ctxt);

        // skip non VCSS subgroups
        if constexpr (std::is_same<CascadeType,VCSS>::value) {
            // skip irrelevant subgroups and shards
            if (sgidx != 0 || shidx !=0) {
                return;
            }
            // filter by hash.
            std::size_t hash = std::hash<typename CascadeType::KeyType>{}(key);
            auto members = ctxt->get_service_client_ref().template get_shard_members<CascadeType>(sgidx,shidx);
            if (members[hash % members.size()] == ctxt->get_service_client_ref().get_my_id()) {
                Action act;
                act.action_type = static_action_table.to_action(value.get_key_ref());
                // act.immediate_data is not used.
                // TODO:Can we avoid this copy?
                act.action_data = std::make_unique<ImageFrame>(value.get_key_ref(),value.blob);
                ctxt->post(std::move(act));
            }
        }
    }
};

template <>
std::shared_ptr<CriticalDataPathObserver<VCSU>> get_critical_data_path_observer<VCSU>() {
    return std::make_shared<ClassifierFilter<VCSU>>();
}

template <>
std::shared_ptr<CriticalDataPathObserver<PCSU>> get_critical_data_path_observer<PCSU>() {
    return std::make_shared<ClassifierFilter<PCSU>>();
}

template <>
std::shared_ptr<CriticalDataPathObserver<VCSS>> get_critical_data_path_observer<VCSS>() {
    return std::make_shared<ClassifierFilter<VCSS>>();
}

template <>
std::shared_ptr<CriticalDataPathObserver<PCSS>> get_critical_data_path_observer<PCSS>() {
    return std::make_shared<ClassifierFilter<PCSS>>();
}

#define DPL_CONF_FLOWER_SYNSET  "CASCADE/flower_synset"
#define DPL_CONF_FLOWER_SYMBOL  "CASCADE/flower_symbol"
#define DPL_CONF_FLOWER_PARAMS  "CASCADE/flower_params"
#define DPL_CONF_PET_SYNSET  "CASCADE/pet_synset"
#define DPL_CONF_PET_SYMBOL  "CASCADE/pet_symbol"
#define DPL_CONF_PET_PARAMS  "CASCADE/pet_params"

class ClassifierTrigger: public OffCriticalDataPathObserver {
private:
    /**
     * the synset explains inference result.
     */
    std::vector<std::string> synset_vector;
    /**
     * symbol
     */
    mxnet::cpp::Symbol net;
    /**
     * argument parameters
     */
    std::map<std::string, mxnet::cpp::NDArray> args_map;
    /**
     * auxliary parameters
     */
    std::map<std::string, mxnet::cpp::NDArray> aux_map;
    /**
     * global ctx
     */
    mxnet::cpp::Context global_ctx;
    /**
     * the input shape
     */
    mxnet::cpp::Shape input_shape;
    /**
     * argument arrays
     */
    std::vector<mxnet::cpp::NDArray> arg_arrays;
    /**
     * gradient arrays
     */
    std::vector<mxnet::cpp::NDArray> grad_arrays;
    /**
     * ??
     */
    std::vector<mxnet::cpp::OpReqType> grad_reqs;
    /**
     * auxliary array
     */
    std::vector<mxnet::cpp::NDArray> aux_arrays;
    /**
     * client data
     */
    mxnet::cpp::NDArray client_data;
    /**
     * the work horse: mxnet executor
     */
    std::unique_ptr<mxnet::cpp::Executor> executor_pointer;    
    
    void load_synset() {
        dbg_default_trace("synset file="+derecho::getConfString(DPL_CONF_FLOWER_SYNSET));
        std::ifstream fin(derecho::getConfString(DPL_CONF_FLOWER_SYNSET));
        synset_vector.clear();
        for(std::string syn;std::getline(fin,syn);) {
            synset_vector.push_back(syn);
        }
        fin.close();
    }

    void load_symbol() {
        dbg_default_trace("symbol file="+derecho::getConfString(DPL_CONF_FLOWER_SYMBOL));
        this->net = mxnet::cpp::Symbol::Load(derecho::getConfString(DPL_CONF_FLOWER_SYMBOL));
    }

    void load_params() {
        auto parameters = mxnet::cpp::NDArray::LoadToMap(derecho::getConfString(DPL_CONF_FLOWER_PARAMS));
        for (const auto& kv : parameters) {
            if (kv.first.substr(0, 4) == "aux:") {
                auto name = kv.first.substr(4, kv.first.size() - 4);
                this->aux_map[name] = kv.second.Copy(global_ctx);
            } else if (kv.first.substr(0, 4) == "arg:") {
                auto name = kv.first.substr(4, kv.first.size() - 4);
                this->args_map[name] = kv.second.Copy(global_ctx);
            }
        }
        mxnet::cpp::NDArray::WaitAll();
        this->args_map["data"] = mxnet::cpp::NDArray(input_shape, global_ctx, false, kFloat32);
        mxnet::cpp::Shape label_shape(input_shape[0]);
        this->args_map["softmax_label"] = mxnet::cpp::NDArray(label_shape, global_ctx, false);
        this->client_data = mxnet::cpp::NDArray(input_shape, global_ctx, false, kFloat32);
    }

    bool load_model() {
		try {
            dbg_default_trace("loading synset.");
            load_synset();
            dbg_default_trace("loading symbol.");
            load_symbol();
            dbg_default_trace("loading params.");
            load_params();

            dbg_default_trace("waiting for loading.");
            mxnet::cpp::NDArray::WaitAll();

            dbg_default_trace("creating executor.");
            this->net.InferExecutorArrays(
                    global_ctx, &arg_arrays, &grad_arrays, &grad_reqs, &aux_arrays,
                    args_map, std::map<std::string, mxnet::cpp::NDArray>(),
                    std::map<std::string, mxnet::cpp::OpReqType>(), aux_map);
            for(auto& i : grad_reqs)
                i = mxnet::cpp::OpReqType::kNullOp;
            this->executor_pointer.reset(new mxnet::cpp::Executor(
                    net, global_ctx, arg_arrays, grad_arrays, grad_reqs, aux_arrays));
            dbg_default_trace("load_model() finished.");
			return true;
		} catch(const std::exception& e) {
            std::cerr << "Load model failed with exception " << e.what() << std::endl;
			return false;
        } catch(...) {
            std::cerr << "Load model failed with unknown exception." << std::endl;
            return false;
        }
    }
public:
    ClassifierTrigger (): 
        OffCriticalDataPathObserver(),
        global_ctx(mxnet::cpp::DeviceType::kCPU,0), // TODO: get resources from CascadeContext
        input_shape(std::vector<mxnet::cpp::index_t>({1, 3, 224, 224})) {
        dbg_default_trace("loading model begin.");
        load_model();
        dbg_default_trace("loading model end.");
    }
    virtual void operator () (Action&& action, ICascadeContext* cascade_ctxt) {
        std::cout << "[cnn_classifier trigger] I received an Action with type=" << std::hex << action.action_type << "; immediate_data=" << action.immediate_data << std::endl;
        if (action.action_type == AT_FLOWER_NAME) {
            // do the inference.
			ImageFrame* frame = dynamic_cast<ImageFrame*>(action.action_data.get());
            std::vector<unsigned char> decode_buf(frame->size);
            std::memcpy(static_cast<void*>(decode_buf.data()),
                        static_cast<const void*>(frame->bytes),
                        frame->size);
            cv::Mat mat = cv::imdecode(decode_buf, cv::IMREAD_COLOR);
            std::vector<mx_float> array;
            // transform to fit 3x224x224 input layer
            cv::resize(mat, mat, cv::Size(256, 256));
            for(int c = 0; c < 3; c++) {            // channels GBR->RGB
                for(int i = 0; i < 224; i++) {      // height
                    for(int j = 0; j < 224; j++) {  // width
                        int _i = i + 16;
                        int _j = j + 16;
                        array.push_back(
                                static_cast<float>(mat.data[(_i * 256 + _j) * 3 + (2 - c)]) / 256);
                    }
                }
            }
            // copy to input layer:
            args_map["data"].SyncCopyFromCPU(array.data(), input_shape.Size());
        
            this->executor_pointer->Forward(false);
            mxnet::cpp::NDArray::WaitAll();
            // extract the result
            auto output_shape = executor_pointer->outputs[0].GetShape();
            mx_float max = -1e10;
            int idx = -1;
            for(unsigned int jj = 0; jj < output_shape[1]; jj++) {
                if(max < executor_pointer->outputs[0].At(0, jj)) {
                    max = executor_pointer->outputs[0].At(0, jj);
                    idx = static_cast<int>(jj);
                }
            }

            std::cout << "[cnn_classifier trigger] " << frame->key << " -> " << synset_vector[idx] << "(" << max << ")" << std::endl;
            auto* ctxt = dynamic_cast<CascadeContext<VCSU,VCSS,PCSU,PCSS>*>(cascade_ctxt);
            PCSS::ObjectType obj(frame->key,synset_vector[idx].c_str(),synset_vector[idx].size());
            auto result = ctxt->get_service_client_ref().template put<PCSS>(obj);
            for (auto& reply_future:result.get()) {
                auto reply = reply_future.second.get();
                dbg_default_debug("node({}) replied with version:({:x},{}us)",reply_future.first,std::get<0>(reply),std::get<1>(reply));
            }
        } else {
            std::cerr << "WARNING:" << action.action_type << " to be supported yet." << std::endl;
        }
    }
};

std::shared_ptr<OffCriticalDataPathObserver> get_off_critical_data_path_observer() {
    return std::make_shared<ClassifierTrigger>();
}

} // namespace cascade
} // namespace derecho
