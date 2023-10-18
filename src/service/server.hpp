#pragma once

#include <cascade/cascade.hpp>
#include <cascade/object.hpp>
#include <cascade/service.hpp>
#include <cascade/service_types.hpp>
#include <cascade/utils.hpp>

#include <string>
#include <type_traits>

#ifndef NDEBUG
inline void dump_layout(const json& layout) {
    int tid = 0;
    for(const auto& pertype : layout) {
        int sidx = 0;
        for(const auto& persubgroup : pertype) {
            dbg_default_trace("subgroup={}.{},layout={}.", tid, sidx, persubgroup.dump());
            sidx++;
        }
        tid++;
    }
}
#endif  // NDEBUG

using derecho::cascade::CriticalDataPathObserver;
using derecho::cascade::ICascadeContext;

/**
 * Define the CDPO for the Cascade service
 * @tparam CascadeType  the subgroup type (PersistentCascadeStore, VolatileCascadeStore, etc.)
 * @tparam CascadeContextType  the specific type of CascadeContext used in this Cascade Service,
 * e.g. DefaultCascadeContextType. This should be a template specialization of CascadeContext<T...>
 */
template <typename CascadeType, typename CascadeContextType>
class CascadeServiceCDPO : public CriticalDataPathObserver<CascadeType> {
    virtual void operator()(const uint32_t sgidx,
                            const uint32_t shidx,
                            const node_id_t sender_id,
                            const typename CascadeType::KeyType& key,
                            const typename CascadeType::ObjectType& value,
                            ICascadeContext* cascade_ctxt,
                            bool is_trigger = false) override {
        if constexpr(std::is_convertible<typename CascadeType::KeyType, std::string>::value) {
            using namespace derecho::cascade;

            auto* ctxt = dynamic_cast<CascadeContextType*>(cascade_ctxt);
            size_t pos = key.rfind(PATH_SEPARATOR);
            std::string prefix;
            if(pos != std::string::npos) {
                // important: we need to keep the trailing PATH_SEPARATOR
                prefix = key.substr(0, pos + 1);
            }
            auto handlers = ctxt->get_prefix_handlers(prefix);
            if(handlers.empty()) {
                return;
            }
            // filter for normal put (put/put_and_forget)
            bool new_actions = false;
            {
                auto shard_members = ctxt->get_service_client_ref().template get_shard_members<CascadeType>(sgidx, shidx);
                bool icare = (shard_members[std::hash<std::string>{}(key) % shard_members.size()] == ctxt->get_service_client_ref().get_my_id());
                for(auto& per_prefix : handlers) {
                    // per_prefix.first is the matching prefix
                    // per_prefix.second is an object of prefix_entry_t
                    for(auto& dfg_ocdpos : per_prefix.second) {
                        // dfg_ocdpos.first is dfg_id
                        // dfg_ocdpos.second is a set of ocdpo info object of type prefix_ocdpo_info_t.
                        for(auto oiit = dfg_ocdpos.second.begin(); oiit != dfg_ocdpos.second.end();) {
                            // each oi is a 6/7-tuple of 
                            // (0)udl_id, (1)config_string, (2)shard dispatcher, [(3)stateful], (4)hook, (5)ocdpo, and (6)outputs;
                            DataFlowGraph::VertexHook hook = std::get<4>(*oiit);
                            if((hook != DataFlowGraph::VertexHook::BOTH) && (
                                (hook == DataFlowGraph::VertexHook::ORDERED_PUT && is_trigger) || 
                                (hook == DataFlowGraph::VertexHook::TRIGGER_PUT && !is_trigger))) {
                                // not my hook, skip it.
                                oiit = dfg_ocdpos.second.erase(oiit);
                            } else if (is_trigger) {
                                new_actions = true;
                                oiit++;
                            } else {
                                // matched ordered put data path
                                // test dispatcher:
                                switch(std::get<2>(*oiit)) {
                                    case DataFlowGraph::VertexShardDispatcher::ONE:
                                        if(icare) {
                                            new_actions = true;
                                            oiit++;
                                        } else {
                                            oiit = dfg_ocdpos.second.erase(oiit);
                                        }
                                        break;
                                    case DataFlowGraph::VertexShardDispatcher::ALL:
                                        new_actions = true;
                                        oiit++;
                                        break;
                                    default:
                                        // unknown dispatcher.
                                        oiit = dfg_ocdpos.second.erase(oiit);
                                        break;
                                }
                            }
                        }
                    }
                }
            }
            if(!new_actions) {
                return;
            }
            // copy data
            auto value_ptr = std::make_shared<typename CascadeType::ObjectType>(value);
            // create actions
            for(auto& per_prefix : handlers) {
                // per_prefix.first is the matching prefix
                // per_prefix.second is an object of prefix_entry_t
                for(const auto& dfg_ocdpos : per_prefix.second) {
                    // dfg_ocdpos.first is dfg_id
                    // dfg_ocdpos.second is a set of ocdpo info object of type prefix_ocdpo_info_t.
                    for (const auto& oi : dfg_ocdpos.second) {
                        // each oi is a 6/7-tuple of 
                        // (0)udl_id, (1)config_string, (2)shard dispatcher, [(3)stateful], (4)hook, (5)ocdpo, and (6)outputs;
                        Action action(
                                sender_id,
                                key,
                                per_prefix.first.size(),
                                value.get_version(),
                                std::get<5>(oi),  // ocdpo
                                value_ptr,
                                std::get<6>(oi)  // outputs
                        );
    
#ifdef ENABLE_EVALUATION
                        ActionPostExtraInfo apei;
                        apei.uint64_val = 0;
                        apei.info.is_trigger = is_trigger;
#endif
    
#ifdef ENABLE_EVALUATION
                        apei.info.stateful = std::get<3>(oi);
#endif
                        TimestampLogger::log(TLT_ACTION_POST_START,
                                             ctxt->get_service_client_ref().get_my_id(),
                                             dynamic_cast<const IHasMessageID*>(&value)->get_message_id(),
                                             get_time_ns(),
                                             apei.uint64_val);
                        ctxt->post(std::move(action), std::get<3>(oi), is_trigger);
                        TimestampLogger::log(TLT_ACTION_POST_END,
                                             ctxt->get_service_client_ref().get_my_id(),
                                             dynamic_cast<const IHasMessageID*>(&value)->get_message_id(),
                                             get_time_ns(),
                                             apei.uint64_val);
                    }
                }
            }
        }
    }
};
