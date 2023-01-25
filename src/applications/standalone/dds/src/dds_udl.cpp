#include <cascade/user_defined_logic_interface.hpp>
#include <cascade_dds/dds.hpp>
#include <iostream>
#include <fstream>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

namespace derecho{
namespace cascade{

#define MY_DESC     "Cascade DDS UDL"

std::string get_uuid() {
    return UDL_UUID;
}

std::string get_description() {
    return MY_DESC;
}

class DDSOCDPO: public OffCriticalDataPathObserver {
    /* configuration */
    std::shared_ptr<DDSConfig> config;
    /* control plane suffix */
    std::string control_plane_suffix;
    /* subscriber registry */
    std::unordered_map<std::string,std::unordered_set<node_id_t>> subscriber_registry;
#ifdef USE_DDS_TIMESTAMP_LOG
#define INIT_TIMESTAMP_SLOTS    (262144)
    /* log the server timestamp, they are grouped by topic name */
    std::unordered_map<std::string,std::vector<uint64_t>> server_timestamp;
#endif
    /* Is shared_mutex fast enough? */
    mutable std::shared_mutex subscriber_registry_mutex;

    virtual void operator () (const node_id_t sender,
                              const std::string& key_string,
                              const uint32_t prefix_length,
                              persistent::version_t, // version
                              const mutils::ByteRepresentable* const value_ptr,
                              const std::unordered_map<std::string,bool>&, // output
                              ICascadeContext* ctxt,
                              uint32_t /*worker_id*/) override {
        auto* typed_ctxt = dynamic_cast<DefaultCascadeContextType*>(ctxt);
        if (key_string.size() <= prefix_length) {
            dbg_default_warn("{}: skipping invalid key_string:{}.", __PRETTY_FUNCTION__, key_string);
            return;
        }
        std::string key_without_prefix = key_string.substr(prefix_length);
        dbg_default_trace("{}: key_without_prefix={}.", __PRETTY_FUNCTION__, key_without_prefix);
        const ObjectWithStringKey* object = dynamic_cast<const ObjectWithStringKey*>(value_ptr);
            if (key_without_prefix == control_plane_suffix) {
            // control plane
            mutils::deserialize_and_run(
                    nullptr, 
                    object->blob.bytes, 
#ifdef USE_DDS_TIMESTAMP_LOG
                    [&sender,&key_string,&typed_ctxt,this](const DDSCommand& command){
#else
                    [&sender,this](const DDSCommand& command){
#endif
                        if (command.command_type == DDSCommand::SUBSCRIBE) {
                            std::unique_lock<std::shared_mutex> wlock(this->subscriber_registry_mutex);
                            if (subscriber_registry.find(command.topic) == subscriber_registry.end()) {
                                subscriber_registry.emplace(command.topic,std::unordered_set<node_id_t>{});
#ifdef USE_DDS_TIMESTAMP_LOG
                                // only add log for new topic.
                                server_timestamp.emplace(command.topic,std::vector<uint64_t>{});
                                server_timestamp.at(command.topic).reserve(INIT_TIMESTAMP_SLOTS);
#endif
                            }
                            subscriber_registry.at(command.topic).emplace(sender);
                            dbg_default_trace("Sender {} subscribes to topic:{}",sender,command.topic);
                        } else if (command.command_type == DDSCommand::UNSUBSCRIBE){
                            std::unique_lock<std::shared_mutex> wlock(this->subscriber_registry_mutex);
                            if (subscriber_registry.find(command.topic) != subscriber_registry.end()) {
                                subscriber_registry.at(command.topic).erase(sender);
#ifdef USE_DDS_TIMESTAMP_LOG
                                // remove it if nobody is listening to this topic.
                                if (subscriber_registry.at(command.topic).empty()) {
                                    server_timestamp.erase(command.topic);
                                }
#endif
                            }
                            dbg_default_trace("Sender {} unsubscribed from topic:{}",sender,command.topic);
#ifdef USE_DDS_TIMESTAMP_LOG
                        } else if (command.command_type == DDSCommand::FLUSH_TIMESTAMP_TRIGGER){
                            DDSCommand ordered_flush_command(DDSCommand::CommandType::FLUSH_TIMESTAMP_ORDERED,command.topic);
                            std::size_t buffer_size = mutils::bytes_size(ordered_flush_command);
                            uint8_t stack_buffer[buffer_size];
                            mutils::to_bytes(ordered_flush_command,stack_buffer);
                            ObjectWithStringKey obj(key_string,Blob(stack_buffer,buffer_size,true));
                            typed_ctxt->get_service_client_ref().put_and_forget(obj);
                            dbg_default_trace("Sender {} triggered flush timestamp for topic:{}",sender,command.topic);
                        } else if (command.command_type == DDSCommand::FLUSH_TIMESTAMP_ORDERED){
                            auto outfile = std::ofstream(command.topic+".log");
                            outfile << "# seqno timestamp(us)" << std::endl;
                            if (server_timestamp.find(command.topic) != server_timestamp.cend())
                            {
                                uint64_t seqno = 0;
                                for(auto& ts : server_timestamp.at(command.topic)) {
                                    outfile << seqno << " " << ts << std::endl;
                                    seqno ++;
                                }
                                server_timestamp.at(command.topic).clear();
                            }
                            outfile.close();
                            dbg_default_trace("flush timestamp for topic:{}",command.topic);
#endif
                        } else {
                            dbg_default_warn("Unknown DDS command Received: type={},topic='{}'",
                                    command.command_type,command.topic);
                        }
                    });
        } else {
            // data plane
            std::shared_lock<std::shared_mutex> rlck(subscriber_registry_mutex);
            if (subscriber_registry.find(key_without_prefix) != subscriber_registry.cend()) {
                dbg_default_trace("Key:{} is found in subscriber_registry.", key_without_prefix);
                for (const auto& client_id: subscriber_registry.at(key_without_prefix)) {
                    dbg_default_trace("Forward a message of {} bytes from topic '{}' to external client {}.",
                            object->blob.size, key_without_prefix, client_id);
                    typed_ctxt->get_service_client_ref().notify(object->blob,key_string.substr(0,prefix_length-1),client_id);
                }
#ifdef USE_DDS_TIMESTAMP_LOG
                // topic(key_without_prefix) may not exists in server_timestamp map if 
                // subscriber_registry[topic] is empty.
                if (server_timestamp.find(key_without_prefix) != server_timestamp.cend()) {
                    server_timestamp.at(key_without_prefix).emplace_back(get_time_us());
                }
#else
                // Please note that, different from the DDS timestamp log, cascade timestamp log use nanoseconds.
                TimestampLogger::log(TLT_DDS_NOTIFYING_SUBSCRIBER, typed_ctxt->get_service_client_ref().get_my_id(),
                        dynamic_cast<const IHasMessageID*>(object)->get_message_id(),get_time_ns());
#endif
            } else {
                dbg_default_trace("Key:{} is not found in subscriber_registry.", key_without_prefix);
            }
        }
    }

    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
public:
    /**
     * Constructor
     */
    DDSOCDPO() {
        config = DDSConfig::get();
        control_plane_suffix = config->get_control_plane_suffix();
    }

    static void initialize() {
        if(!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<DDSOCDPO>();
        }
    }

    static auto get() {
        return ocdpo_ptr;
    }
};

/* singleton design: all prefixes will use the same ocdpo */
std::shared_ptr<OffCriticalDataPathObserver> DDSOCDPO::ocdpo_ptr;

void initialize(ICascadeContext* ctxt) {
    DDSOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext*,const nlohmann::json&) {
    return DDSOCDPO::get();
}

void release(ICascadeContext* ctxt) {
    // nothing to release
    return;
}

} // namespace cascade
} // namespace derecho
