#include "cascade/persistence_observer.hpp"

#include <derecho/core/derecho.hpp>

#include <condition_variable>
#include <functional>
#include <list>
#include <mutex>
#include <thread>

namespace derecho {
namespace cascade {

PersistenceObserver::PersistenceObserver()
        : thread_shutdown(false) {
    callback_worker = std::thread(&PersistenceObserver::process_callback_actions, this);
}

PersistenceObserver::~PersistenceObserver() {
    thread_shutdown = true;
    events_to_handle.notify_all();
    if(callback_worker.joinable()) {
        callback_worker.join();
    }
}

void PersistenceObserver::derecho_local_persistence_callback(subgroup_id_t subgroup_id, persistent::version_t version) {
    std::lock_guard<std::mutex> queue_lock(persistence_events_mutex);
    persistence_callback_events.emplace(PersistenceEvent{subgroup_id, version, false});
    events_to_handle.notify_all();
}

void PersistenceObserver::derecho_global_persistence_callback(subgroup_id_t subgroup_id, persistent::version_t version) {
    std::lock_guard<std::mutex> queue_lock(persistence_events_mutex);
    persistence_callback_events.emplace(PersistenceEvent{subgroup_id, version, true});
    events_to_handle.notify_all();
}

void PersistenceObserver::process_callback_actions() {
    pthread_setname_np(pthread_self(), "pers_observer");
    PersistenceEvent current_event;
    while(!thread_shutdown) {
        bool has_current_event = false;
        std::list<std::function<void()>> past_due_actions_copy;
        {
            std::unique_lock<std::mutex> lock(persistence_events_mutex);
            events_to_handle.wait(lock, [&]() {
                return !persistence_callback_events.empty() || !past_due_actions.empty() || thread_shutdown;
            });
            if(thread_shutdown)
                break;
            if(!persistence_callback_events.empty()) {
                current_event = persistence_callback_events.front();
                persistence_callback_events.pop();
                has_current_event = true;
            }
            // Update the persistence frontiers
            if(current_event.is_global) {
                global_persistence_frontier[current_event.subgroup_id] = current_event.version;
            } else {
                local_persistence_frontier[current_event.subgroup_id] = current_event.version;
            }
            // This should be cheap since std::function is cheap to copy
            past_due_actions_copy = past_due_actions;
            past_due_actions.clear();
        }
        if(has_current_event) {
            dbg_default_debug("PersistenceObserver: Handling a persistence event for version {}, is_global={}", current_event.version, current_event.is_global);
            // Since persistence callbacks can be batched, there may not be one callback per version
            // Find all of the registered_actions entries with the same subgroup and versions <= the current event
            std::list<std::function<void()>> action_list;
            {
                std::unique_lock<std::mutex> actions_lock(registered_actions_mutex);
                auto event_entry = registered_actions.lower_bound(current_event);
                if(event_entry != registered_actions.end()) {
                    // lower_bound may return the next key after the requested one
                    if(event_entry->first == current_event) {
                        action_list = event_entry->second;
                        event_entry = registered_actions.erase(event_entry);
                    }
                    // Search backwards until either the beginning of the map or a key with a different subgroup
                    // Since the map is sorted by subgroup_id, then version, all keys with the same subgroup are adjacent
                    while(event_entry != registered_actions.begin()) {
                        event_entry--;
                        if(event_entry->first.subgroup_id == current_event.subgroup_id
                           && event_entry->first.is_global == current_event.is_global
                           && event_entry->first.version < current_event.version) {
                            dbg_default_debug("PersistenceObserver: Adding actions for skipped event (subgroup {}, version {}, {})", event_entry->first.subgroup_id, event_entry->first.version, event_entry->first.is_global);
                            action_list.splice(action_list.begin(), event_entry->second);
                            event_entry = registered_actions.erase(event_entry);
                        } else {
                            break;
                        }
                    }
                    // handle the begin() entry separately, because there's no pre-begin() iterator to compare to in the loop
                    if(event_entry == registered_actions.begin()
                       && event_entry->first.subgroup_id == current_event.subgroup_id
                       && event_entry->first.is_global == current_event.is_global
                       && event_entry->first.version < current_event.version) {
                        dbg_default_debug("PersistenceObserver: Adding actions for skipped event (subgroup {}, version {}, {})", event_entry->first.subgroup_id, event_entry->first.version, event_entry->first.is_global);
                        action_list.splice(action_list.begin(), event_entry->second);
                        event_entry = registered_actions.erase(event_entry);
                    }
                }
                // else action_list remains an empty list
            }
            // Release the action lock before firing the actions
            dbg_default_debug("PersistenceObserver: Firing {} actions for the persistence event", action_list.size());
            for(const auto& action_function : action_list) {
                action_function();
            }
        }
        if(!past_due_actions_copy.empty()) {
            dbg_default_debug("PersistenceObserver: Firing {} past-due actions", past_due_actions_copy.size());
            for(const auto& action_function : past_due_actions_copy) {
                action_function();
            }
        }
    }
    dbg_default_debug("PersistenceObserver thread shutting down");
}

void PersistenceObserver::register_persistence_action(subgroup_id_t subgroup_id, persistent::version_t version, bool is_global, std::function<void()> action) {
    bool already_happened = false;
    {
        std::unique_lock<std::mutex> lock(persistence_events_mutex);
        if((is_global && global_persistence_frontier[subgroup_id] >= version)
           || (!is_global && local_persistence_frontier[subgroup_id] >= version))
            already_happened = true;
    }
    {
        std::unique_lock<std::mutex> lock(registered_actions_mutex);
        if(already_happened) {
            dbg_default_debug("PersistenceObserver: Registered an action for subgroup {}, version {} but it has already finished persisting", subgroup_id, version);
            past_due_actions.emplace_back(action);
        } else {
            dbg_default_debug("PersistenceObserver: Registered an action for subgroup {}, version {}, is_global={}", subgroup_id, version, is_global);
            // operator[] is intentional: Create a new empty list if this key is not in the map
            registered_actions[PersistenceEvent{subgroup_id, version, is_global}].emplace_back(action);
        }
    }
    if(already_happened) {
        events_to_handle.notify_all();
    }
}
}  // namespace cascade
}  // namespace derecho