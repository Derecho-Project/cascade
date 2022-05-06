#include "demo_common.hpp"
#include <memory>
#include <vector>
#include <tensorflow/c/c_api.h>
#include <tensorflow/c/eager/c_api.h>

class TensorflowContext {
private:
    TFE_Context* tfe_context{nullptr};
public:
    explicit TensorflowContext(TFE_ContextOptions* opts = nullptr) {
        auto tf_status = TF_NewStatus();
        if (opts == nullptr) {
            std::unique_ptr<TFE_ContextOptions, decltype(&TFE_DeleteContextOptions)> new_opts(TFE_NewContextOptions(), &TFE_DeleteContextOptions);
            this->tfe_context = TFE_NewContext(new_opts.get(), tf_status);
        } else {
            this->tfe_context = TFE_NewContext(opts, tf_status);
        }
#define CHECK_STATUS(tfs) \
        if (TF_GetCode(tfs) != TF_OK) { \
            std::runtime_error rerr(TF_Message(tfs)); \
            TF_DeleteStatus(tfs); \
            throw rerr; \
        }
        CHECK_STATUS(tf_status);
        TF_DeleteStatus(tf_status);
    }

    TensorflowContext(TensorflowContext const&) = delete;
    TensorflowContext& operator = (TensorflowContext const&) = delete;

    TensorflowContext(TensorflowContext&& rhs) noexcept:
        tfe_context(std::exchange(rhs.tfe_context, nullptr)) {}

    TensorflowContext& operator = (TensorflowContext&& rhs) {
        tfe_context = std::exchange(rhs.tfe_context, tfe_context);
        return *this;
    }

    TFE_Context* get() {
        return this->tfe_context;
    }

    static TensorflowContext& get_global_context() {
        static TensorflowContext tfc;
        return tfc;
    }
};

void initialize_tf_context() {
    static std::mutex global_context_mutex;
    std::lock_guard<std::mutex> lck(global_context_mutex);
    
    if (TensorflowContext::get_global_context().get()) {
        /* already initialized */
        return;
    }

    std::vector<uint8_t> config{INIT_100PCT_GROWTH};
    TFE_ContextOptions* options = TFE_NewContextOptions();
    auto tf_status = TF_NewStatus();
    TFE_ContextOptionsSetConfig(options, config.data(), config.size(), tf_status);
    CHECK_STATUS(tf_status);

    TensorflowContext::get_global_context() = TensorflowContext(options);
}

