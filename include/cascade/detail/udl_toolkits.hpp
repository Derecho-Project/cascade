#pragma once
/**
 * @file udl_toolkit.hpp
 *
 * The UDL toolkit
 */

/**
 * @brief DefaultOffCriticalDataPathObserver
 * A wrapper around OffCrticalDataPathObserver with application friendly parameters.
 */
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

    /**
     * @brief offcritical data path handler
     * 
     * @param[in]   sender                  The node id of the object sender
     * @param[in]   object_pool_pathname    The path prefix(object pool) of the object
     * @param[in]   key_string              The key string
     * @param[in]   object                  The object
     * @param[in]   emit                    Function to send the output
     * @param[in]   typed_ctxt              Typed Cascade Context
     * @param[in]   worker_id               Worker thread id.
     */
    virtual void ocdpo_handler (
            const node_id_t                 sender,
            const std::string&              object_pool_pathname,
            const std::string&              key_string,
            const ObjectWithStringKey&      object,
            const emit_func_t&              emit,
            DefaultCascadeContextType*      typed_ctxt,
            uint32_t                        worker_id) = 0;
};
