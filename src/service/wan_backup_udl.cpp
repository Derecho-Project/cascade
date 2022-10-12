#include <cascade/service_types.hpp>
#include <cascade/user_defined_logic_interface.hpp>

#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <nlohmann/json.hpp>
#include <tuple>
#include <wan_agent.hpp>

// JSON key names for the DFG options list
// Use this option to tell wan_backup_udl where to find its configuration file
#define WAN_UDL_OPTION_CONF_FILE "conf_file"

namespace derecho {
namespace cascade {

class WanAgentBackupObserver : public OffCriticalDataPathObserver {
    static std::shared_ptr<OffCriticalDataPathObserver> singleton_ptr;
    static std::atomic<uint32_t> initialize_state;
    static constexpr uint32_t UNINITIALIZED = 0;
    static constexpr uint32_t INITIALIZING = 1;
    static constexpr uint32_t INITIALIZED = 2;
    /**
     * The WanAgent instance running in this UDL. Constructed the first time
     * get_observer() is called, not when initialize() is called, because only
     * get_observer() can pass in configuration options.
     */
    std::unique_ptr<wan_agent::WanAgent> wanagent;
    /**
     * A copy of the CascadeContext pointer that should point to the CascadeContext
     * for the node running this UDL. Cached from the pointer given to the get()
     * function the first time it is called. Hopefully this won't become invalid
     * because the CascadeContext is uniquely owned by the Service and the Service
     * object outlives everything else.
     */
    ICascadeContext* cascade_context;
    /**
     * Maps each WanAgent message ID to the (key, version) pair for the object
     * sent in that WanAgent message. Used to determine which object has finished
     * being backed up when a WanAgent message is acknowledged.
     */
    std::map<uint64_t, std::pair<std::string, persistent::version_t>> key_for_message_id;

    static void atomic_initialize(ICascadeContext* context, const nlohmann::json& config_options) {
        uint32_t expected_uninit = UNINITIALIZED;
        if(initialize_state.compare_exchange_strong(expected_uninit, INITIALIZING, std::memory_order_acq_rel)) {
            singleton_ptr = std::make_shared<WanAgentBackupObserver>(context, config_options);
            initialize_state.store(INITIALIZED, std::memory_order_acq_rel);
        }
        // In case execution reaches here because initialize_state was INITIALIZING, wait for the concurrent caller to finish
        while(initialize_state.load(std::memory_order_acquire) != INITIALIZED) {
        }
    }

public:
    void agent_stability_callback(const std::map<site_id_t, uint64_t>& ack_table) {
        uint64_t min_msg_num = ack_table.begin()->second;
        for(const auto& entry : ack_table) {
            if(entry.second < min_msg_num) {
                min_msg_num = entry.second;
            }
        }
        // Message number must be in the map, since we had to send it before we can receive an ack for it
        auto [key_string, version] = key_for_message_id.at(min_msg_num);
        std::cout << "Message " << min_msg_num << ", corresponding to " << key_string << " at version " << version << " has been received by all backups" << std::endl;
        // Send a notification message to the client that submitted the update,
        // indicating that the object has reached "backup stability"
        // Message format: object key, object version (for now, we will assume all StandardCascadeNotifications indicate backup stability)
        std::size_t message_size = mutils::bytes_size(key_string) + mutils::bytes_size(version);
        // Problem: There is no way to construct a Blob without copying data into the buffer twice
        uint8_t* temp_buffer_for_blob = new uint8_t[message_size];
        std::size_t body_offset = 0;
        body_offset += mutils::to_bytes(key_string, temp_buffer_for_blob + body_offset);
        body_offset += mutils::to_bytes(version, temp_buffer_for_blob + body_offset);
        Blob message_body(temp_buffer_for_blob, message_size);
        delete[] temp_buffer_for_blob;
        // PROBLEM: The UDL doesn't know which client submitted the update, and even if the client
        // "subscribes" by sending a message to the PersistentStore/SignatureStore, the UDL won't know
        // about the list of subscribed clients.

        // node_id_t client_id = ???
        // auto typed_context = dynamic_cast<DefaultCascadeContextType*>(cascade_context);
        // typed_context->get_service_client_ref().notify(message_body, get_pathname<std::string>(key_string), client_id);
    }
    void agent_remote_message_callback(const uint32_t sender, const uint8_t* msg, const size_t size) {
        // This UDL's WanAgent should not be getting remote messages from the backup sites; it only sends to them
        std::cout << "WARNING: Got a WanAgent message from backup site with ID " << sender << ", size = " << size << std::endl;
    }
    WanAgentBackupObserver(ICascadeContext* context, const nlohmann::json& config_options)
            : cascade_context(context) {
        auto test_context = dynamic_cast<DefaultCascadeContextType*>(context);
        if(test_context == nullptr) {
            std::cerr << "ERROR: WanAgentBackupObserver was constructed on a server where the context type does not match DefaultCascadeContextType!" << std::endl;
        }
        // Read the location of WanAgent's configuration file from the UDL config options list
        std::string config_file_location = config_options.at(WAN_UDL_OPTION_CONF_FILE);
        std::ifstream wanagent_config_file(config_file_location);
        nlohmann::json wanagent_config = nlohmann::json::parse(wanagent_config_file);
        wanagent = wan_agent::WanAgent::create(
                wanagent_config,
                [this](const std::map<site_id_t, uint64_t>& table) { agent_stability_callback(table); },
                [this](const uint32_t sender, const uint8_t* msg, const size_t size) { agent_remote_message_callback(sender, msg, size); });
    }
    virtual void operator()(const node_id_t sender,
                            const std::string& key_string,
                            const uint32_t prefix_length,
                            persistent::version_t version,
                            const mutils::ByteRepresentable* const value_ptr,
                            const std::unordered_map<std::string, bool>& outputs,
                            ICascadeContext* context,
                            uint32_t worker_id) override {
        // Serialize the object to a new buffer
        std::vector<uint8_t> serialized_object(mutils::bytes_size(*value_ptr));
        mutils::to_bytes(*value_ptr, serialized_object.data());
        // Send it with WanAgent
        uint64_t msg_num = wanagent->send(serialized_object.data(), serialized_object.size());
        std::cout << "Sent an object to the backup sites in message number " << msg_num << std::endl;
        // Save the message number with the object's key and version
        key_for_message_id.emplace(msg_num, std::make_pair(key_string, version));
    }

    static void initialize(ICascadeContext* context) {
        // Do nothing; initialization must wait until we get the config options
    }

    static std::shared_ptr<OffCriticalDataPathObserver> get(ICascadeContext* context, const nlohmann::json& config_options) {
        atomic_initialize(context, config_options);
        return singleton_ptr;
    }
};

/* ----------------------- UDL Interface ----------------------- */

std::string get_uuid() {
    // Generated by uuidtools.com. I'm not sure where these are supposed to come from for Cascade's purposes.
    return "2d3347bc-d450-4cd2-bf3a-e882b1e8351e";
}

std::string get_description() {
    return "UDL module bundled with CascadeChain that forwards the data it receives to a backup site using WanAgent.";
}

void initialize(ICascadeContext* context) {
    WanAgentBackupObserver::initialize(context);
}

void release(ICascadeContext* context) {
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext* context, const nlohmann::json& config) {
    return WanAgentBackupObserver::get(context, config);
}

}  // namespace cascade
}  // namespace derecho