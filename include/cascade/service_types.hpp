#pragma once
#include <cascade/cascade.hpp>
#include <cascade/object.hpp>
#include <cascade/object_pool_metadata.hpp>
#include <derecho/conf/conf.hpp>

#define CONF_VCS_UINT64KEY_LAYOUT "CASCADE/VOLATILECASCADESTORE/UINT64/layout"
#define CONF_VCS_STRINGKEY_LAYOUT "CASCADE/VOLATILECASCADESTORE/STRING/layout"
#define CONF_PCS_UINT64KEY_LAYOUT "CASCADE/PERSISTENTCASCADESTORE/UINT64/layout"
#define CONF_PCS_STRINGKEY_LAYOUT "CASCADE/PERSISTENTCASCADESTORE/STRING/layout" 

namespace derecho {
namespace cascade {

// using VCSU = VolatileCascadeStore<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV>;
using VolatileCascadeStoreWithStringKey = VolatileCascadeStore<std::string,ObjectWithStringKey,&ObjectWithStringKey::IK,&ObjectWithStringKey::IV>;
// using PCSU = PersistentCascadeStore<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV,ST_FILE>;
using PersistentCascadeStoreWithStringKey = PersistentCascadeStore<std::string,ObjectWithStringKey,&ObjectWithStringKey::IK,&ObjectWithStringKey::IV,ST_FILE>;
// using TCSU = TriggerCascadeNoStore<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV>;
using TriggerCascadeNoStoreWithStringKey = TriggerCascadeNoStore<std::string,ObjectWithStringKey,&ObjectWithStringKey::IK,&ObjectWithStringKey::IV>;

} // namespace cascade
} // namespace derecho
