#pragma once
#include <cascade/cascade.hpp>
#include <cascade/object.hpp>
#include <derecho/conf/conf.hpp>

#define CONF_VCS_UINT64KEY_LAYOUT "CASCADE/VOLATILECASCADESTORE/UINT64/layout"
#define CONF_VCS_STRINGKEY_LAYOUT "CASCADE/VOLATILECASCADESTORE/STRING/layout"
#define CONF_PCS_UINT64KEY_LAYOUT "CASCADE/PERSISTENTCASCADESTORE/UINT64/layout"
#define CONF_PCS_STRINGKEY_LAYOUT "CASCADE/PERSISTENTCASCADESTORE/STRING/layout"

namespace derecho
{
    namespace cascade
    {

        using VCSU = VolatileCascadeStore<uint64_t, ObjectWithUInt64Key, &ObjectWithUInt64Key::IK, &ObjectWithUInt64Key::IV>;
        using VCSS = VolatileCascadeStore<std::string, ObjectWithStringKey, &ObjectWithStringKey::IK, &ObjectWithStringKey::IV>;
        using PCSU = PersistentCascadeStore<uint64_t, ObjectWithUInt64Key, &ObjectWithUInt64Key::IK, &ObjectWithUInt64Key::IV, ST_FILE>;
        using PCSS = PersistentCascadeStore<std::string, ObjectWithStringKey, &ObjectWithStringKey::IK, &ObjectWithStringKey::IV, ST_FILE>;

        // using WPCSU = WANPersistentCascadeStoreInheritance<uint64_t, ObjectWithUInt64Key, &ObjectWithUInt64Key::IK, &ObjectWithUInt64Key::IV, ST_FILE, 2>;
        // using WPCSS = WANPersistentCascadeStoreInheritance<std::string, ObjectWithStringKey, &ObjectWithStringKey::IK, &ObjectWithStringKey::IV, ST_FILE, 2>;

        using WPCSU = WANPersistentCascadeStore<uint64_t, ObjectWithUInt64Key, &ObjectWithUInt64Key::IK, &ObjectWithUInt64Key::IV, ST_FILE>;
        using WPCSS = WANPersistentCascadeStore<std::string, ObjectWithStringKey, &ObjectWithStringKey::IK, &ObjectWithStringKey::IV, ST_FILE>;

        using UCW = CascadeWatcher<uint64_t, ObjectWithUInt64Key, &ObjectWithUInt64Key::IK, &ObjectWithUInt64Key::IV>;
        using SCW = CascadeWatcher<std::string, ObjectWithStringKey, &ObjectWithStringKey::IK, &ObjectWithStringKey::IV>;

    } // namespace cascade
} // namespace derecho
