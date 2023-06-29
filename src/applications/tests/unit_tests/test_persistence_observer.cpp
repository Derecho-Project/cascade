#include <cascade/persistence_observer.hpp>

#include <iostream>

using namespace derecho;

int main(int argc, char** argv) {
    derecho::Conf::initialize(argc, argv);
    LoggerFactory::getDefaultLogger()->info("Starting test_persistence_observer");
    cascade::PersistenceObserver pers_observer;
    // Register some actions for global persistence for subgroups 0-2 and versions 2-22
    for(subgroup_id_t subgroup = 0; subgroup < 3; ++subgroup) {
        // Skip every other version so there isn't an action for every version
        for(persistent::version_t version = 2; version <= 22; version += 2) {
            pers_observer.register_persistence_action(
                    subgroup, version, true,
                    [=]() {
                        LoggerFactory::getDefaultLogger()->info("Subgroup {}, version {} action", subgroup, version);
                    });
        }
    }
    // Fire local and global persistence events like Derecho would: local before global, batches of 2-4
    pers_observer.derecho_local_persistence_callback(0, 1);
    pers_observer.derecho_local_persistence_callback(1, 1);
    pers_observer.derecho_local_persistence_callback(2, 1);
    pers_observer.derecho_local_persistence_callback(0, 2);
    pers_observer.derecho_local_persistence_callback(1, 2);
    pers_observer.derecho_local_persistence_callback(2, 2);
    // This should fire the action at the beginning of the event map, since version 2 is the first with an action
    pers_observer.derecho_global_persistence_callback(0, 2);
    pers_observer.derecho_global_persistence_callback(1, 2);
    pers_observer.derecho_global_persistence_callback(2, 2);
    // local persistence one at a time up to version 8
    for(persistent::version_t version = 3; version <= 8; ++version) {
        for(subgroup_id_t subgroup = 0; subgroup < 3; ++subgroup) {
            pers_observer.derecho_local_persistence_callback(subgroup, version);
        }
    }
    // Global persistence should fire a batch of 3 actions (4, 6, and 8)
    LoggerFactory::getDefaultLogger()->info("Calling global persistence for version 8");
    pers_observer.derecho_global_persistence_callback(0, 8);
    pers_observer.derecho_global_persistence_callback(1, 8);
    pers_observer.derecho_global_persistence_callback(2, 8);
    // local persistence in batches up to version 20
    for(persistent::version_t version = 10; version <= 20; version += 2) {
        for(subgroup_id_t subgroup = 0; subgroup < 3; ++subgroup) {
            pers_observer.derecho_local_persistence_callback(subgroup, version);
        }
    }
    // Fire global persistence in reverse subgroup order to make sure map searching works
    LoggerFactory::getDefaultLogger()->info("Calling global persistence for subgroup 2, version 20");
    pers_observer.derecho_global_persistence_callback(2, 20);
    LoggerFactory::getDefaultLogger()->info("Calling global persistence for subgroup 1, version 20");
    pers_observer.derecho_global_persistence_callback(1, 20);
    LoggerFactory::getDefaultLogger()->info("Calling global persistence for subgroup 0, version 20");
    pers_observer.derecho_global_persistence_callback(0, 20);
    // Wait for the observer thread to process all the callbacks
    std::this_thread::sleep_for(std::chrono::seconds(1));
    // Register some past-due actions, then deliver another callback
    pers_observer.register_persistence_action(0, 11, true,
                                              []() { LoggerFactory::getDefaultLogger()->info("Subgroup 0, version 11, late action"); });
    pers_observer.register_persistence_action(2, 20, true,
                                              []() { LoggerFactory::getDefaultLogger()->info("Subgroup 2, version 20, late action"); });
    pers_observer.derecho_local_persistence_callback(0, 22);
    pers_observer.derecho_local_persistence_callback(1, 22);
    pers_observer.derecho_local_persistence_callback(2, 22);
    pers_observer.derecho_global_persistence_callback(0, 22);
    pers_observer.derecho_global_persistence_callback(1, 22);
    pers_observer.derecho_global_persistence_callback(2, 22);
    std::cout << "Waiting for PersistenceObserver thread to finish. Press enter to terminate." << std::endl;
    std::cin.get();
}