#pragma once

class DefaultOffCriticalDataPathObserver : public IDefaultOffCriticalDataPathObserver, public OffCriticalDataPathObserver {
public:
    virtual void operator() (
            const node_id_t sender,
            const std::string& full_key_string,
            const uint32_t prefix_length,
            persistent::version_t version,
            const mutils::ByteRepresentable* const value_ptr,
            const std::unordered_map<std::string,bool>& outputs,
            ICascadeContext* ctxt,
            uint32_t worker_id) override;

    virtual void ocdpo_handler (
            const node_id_t                 sender,
            const std::string&              object_pool_pathname,
            const std::string&              key_string,
            const ObjectWithStringKey&      object,
            const std::function<void(const std::string&, const Blob&)>& emit,
            DefaultCascadeContextType*      typed_ctxt,
            uint32_t                        worker_id) = 0;
};
