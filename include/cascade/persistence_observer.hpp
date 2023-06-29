#pragma once

#include <derecho/core/derecho.hpp>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <list>
#include <map>
#include <mutex>
#include <queue>
#include <set>
#include <thread>
#include <tuple>

namespace derecho {
namespace cascade {

struct PersistenceEvent {
    subgroup_id_t subgroup_id;
    persistent::version_t version;
    bool is_global;
};

inline bool operator<(const PersistenceEvent& lhs, const PersistenceEvent& rhs) {
    return std::tie(lhs.subgroup_id, lhs.version, lhs.is_global)
           < std::tie(rhs.subgroup_id, rhs.version, rhs.is_global);
}
inline bool operator==(const PersistenceEvent& lhs, const PersistenceEvent& rhs) {
    return lhs.subgroup_id == rhs.subgroup_id
           && lhs.version == rhs.version
           && lhs.is_global == rhs.is_global;
}

class PersistenceObserver {
private:
    std::thread callback_worker;
    std::atomic<bool> thread_shutdown;
    /**
     * For each persistence event, contains a list of action functions that
     * should be run when that persistence event happens. Shared between the
     * callback worker thread and any other threads that register actions.
     */
    std::map<PersistenceEvent, std::list<std::function<void()>>> registered_actions;
    /**
     * A list of action functions that were registered after their corresponding
     * persistent event had already happened, and thus should fire ASAP.
     */
    std::list<std::function<void()>> past_due_actions;
    /**
     * For each subgroup ID, stores the largest version number that has reached
     * local persistence. Updated by the callback worker thread when it handles
     * a new persistence event.
     */
    std::map<subgroup_id_t, persistent::version_t> local_persistence_frontier;
    /**
     * For each subgroup ID, stores the largest version number that has reached
     * global persistence. Updated by the callback worker thread.
     */
    std::map<subgroup_id_t, persistent::version_t> global_persistence_frontier;
    /**
     * A queue of PersistenceEvent objects, in the order they were generated by
     * Derecho persistence callbacks. Shared between the Derecho predicates thread
     * (which calls the persistence callbacks) and the callback worker thread.
     */
    std::queue<PersistenceEvent> persistence_callback_events;
    /** Mutex to guard both persistent_callback_events and the frontier maps */
    std::mutex persistence_events_mutex;
    std::condition_variable events_to_handle;
    /** Mutex to guard registered_actions and past_due_actions */
    std::mutex registered_actions_mutex;
    /**
     * The function that implements the callback worker thread; wakes up and
     * executes any registered actions when a persistence callback arrives.
     */
    void process_callback_actions();

public:
    PersistenceObserver();
    ~PersistenceObserver();
    /**
     * A callback function to register as Derecho's local persistence callback
     * in the Derecho Group constructor.
     *
     * @param subgroup_id The Derecho-internal Subgroup ID of the subgroup in which persistence finished
     * @param version The version that finished persisting.
     */
    void derecho_local_persistence_callback(subgroup_id_t subgroup_id, persistent::version_t version);
    /**
     * A callback function to register as Derecho's global persistence callback
     * in the Derecho Group constructor.
     *
     * @param subgroup_id The Derecho-internal Subgroup ID of the subgroup in which persistence finished
     * @param version The version that finished persisting.
     */
    void derecho_global_persistence_callback(subgroup_id_t subgroup_id, persistent::version_t version);
    /**

     * Registers an action that should run when a particular persistence event happens.
     * Persistent events are identified by a subgroup ID (as returned by Replicated<T>::get_subgroup_id()),
     * a version number, and a boolean indicating whether local persistence or global
     * persistence has occurred.
     *
     * @param subgroup_id The subgroup ID that the caller is interested in
     * @param version The version that the caller is interested in
     * @param is_global Whether the action should run when this version reaches global persistence
     * (true) or local persistence (false)
     * @param action The function to execute when the persistence event happens.
     */
    void register_persistence_action(subgroup_id_t subgroup_id, persistent::version_t version, bool is_global, std::function<void()> action);
};

}  // namespace cascade
}  // namespace derecho
