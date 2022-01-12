#pragma once

#include "cascade/cascade.hpp"
#include "cascade/service.hpp"
#include "cascade/service_types.hpp"
#include "cascade/object.hpp"

#include <string>
#include <type_traits>


#ifndef NDEBUG
inline void dump_layout(const json& layout) {
    int tid = 0;
    for (const auto& pertype:layout) {
        int sidx = 0;
        for (const auto& persubgroup:pertype ) {
            dbg_default_trace("subgroup={}.{},layout={}.",tid,sidx,persubgroup.dump());
            sidx ++;
        }
        tid ++;
    }
}
#endif//NDEBUG

using derecho::cascade::ICascadeContext;
using derecho::cascade::CriticalDataPathObserver;

/**
 * Define the CDPO for the Cascade service
 * @tparam CascadeType  the subgroup type (PersistentCascadeStore, VolatileCascadeStore, etc.)
 * @tparam CascadeContextType  the specific type of CascadeContext used in this Cascade Service,
 * e.g. DefaultCascadeContextType. This should be a template specialization of CascadeContext<T...>
 */
template <typename CascadeType, typename CascadeContextType>
class CascadeServiceCDPO: public CriticalDataPathObserver<CascadeType> {
    virtual void operator() (const uint32_t sgidx,
                             const uint32_t shidx,
                             const typename CascadeType::KeyType& key,
                             const typename CascadeType::ObjectType& value,
                             ICascadeContext* cascade_ctxt,
                             bool is_trigger = false) override {
        if constexpr (std::is_convertible<typename CascadeType::KeyType,std::string>::value) {

            using namespace derecho::cascade;

            auto* ctxt = dynamic_cast<CascadeContextType*>(cascade_ctxt);
            size_t pos = key.rfind(PATH_SEPARATOR);
            std::string prefix;
            if (pos != std::string::npos) {
                // important: we need to keep the trailing PATH_SEPARATOR
                prefix = key.substr(0,pos+1);
            }
            auto handlers = ctxt->get_prefix_handlers(prefix);
            if (handlers.empty()) {
                return;
            }
            // filter for normal put (put/put_and_forget)
            bool new_actions = is_trigger;
            if (!is_trigger) {
                auto shard_members = ctxt->get_service_client_ref().template get_shard_members<CascadeType>(sgidx,shidx);
                bool icare = (shard_members[std::hash<std::string>{}(key)%shard_members.size()] == ctxt->get_service_client_ref().get_my_id());
                for(auto& per_prefix: handlers) {
                    // per_prefix.first is the matching prefix
                    // per_prefix.second is a set of handlers
                    for (auto it=per_prefix.second.cbegin();it!=per_prefix.second.cend();) {
                        // it->first is handler uuid
                        // it->second is a 3-tuple of shard dispatcher,ocdpo,and outputs;
                        switch(std::get<0>(it->second)) {
                        case DataFlowGraph::VertexShardDispatcher::ONE:
                            if (icare) {
                                new_actions = true;
                                it++;
                            } else {
                                per_prefix.second.erase(it++);
                            }
                            break;
                        case DataFlowGraph::VertexShardDispatcher::ALL:
                            new_actions = true;
                            it++;
                            break;
                        default:
                            per_prefix.second.erase(it++);
                            break;
                        }
                    }
                }
            }
            if (!new_actions) {
                return;
            }
            // copy data
            auto value_ptr = std::make_shared<typename CascadeType::ObjectType>(value);
            // create actions
            for(auto& per_prefix : handlers) {
                // per_prefix.first is the matching prefix
                // per_prefix.second is a set of handlers
                for (const auto& handler : per_prefix.second) {
                    // handler.first is handler uuid
                    // handler.second is a 3-tuple of shard dispatcher,ocdpo,and outputs;
                    Action action(
                            key,
                            per_prefix.first.size(),
                            value.get_version(),
                            std::get<1>(handler.second), // ocdpo
                            value_ptr,
                            std::get<2>(handler.second)  // outputs
                    );
                    ctxt->post(std::move(action),is_trigger);
                }
            }
        }
    }
};
