#pragma once
#include <cascade/cascade.hpp>
#include <cascade/object.hpp>
#include <cascade/object_pool_metadata.hpp>
#include <cascade/service.hpp>
#include <derecho/conf/conf.hpp>

#include <type_traits>
#include <utility>

#define CONF_VCS_UINT64KEY_LAYOUT "CASCADE/VOLATILECASCADESTORE/UINT64/layout"
#define CONF_VCS_STRINGKEY_LAYOUT "CASCADE/VOLATILECASCADESTORE/STRING/layout"
#define CONF_PCS_UINT64KEY_LAYOUT "CASCADE/PERSISTENTCASCADESTORE/UINT64/layout"
#define CONF_PCS_STRINGKEY_LAYOUT "CASCADE/PERSISTENTCASCADESTORE/STRING/layout"

namespace derecho {
namespace cascade {

// using VCSU = VolatileCascadeStore<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV>;
using VolatileCascadeStoreWithStringKey = VolatileCascadeStore<
                                                std::remove_cv_t<std::remove_reference_t<decltype(std::declval<ObjectWithStringKey>().get_key_ref())>>,
                                                ObjectWithStringKey,
                                                &ObjectWithStringKey::IK,
                                                &ObjectWithStringKey::IV>;
// using PCSU = PersistentCascadeStore<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV,ST_FILE>;
using PersistentCascadeStoreWithStringKey = PersistentCascadeStore<
                                                std::remove_cv_t<std::remove_reference_t<decltype(std::declval<ObjectWithStringKey>().get_key_ref())>>,
                                                ObjectWithStringKey,
                                                &ObjectWithStringKey::IK,
                                                &ObjectWithStringKey::IV,ST_FILE>;
// using TCSU = TriggerCascadeNoStore<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV>;
using TriggerCascadeNoStoreWithStringKey = TriggerCascadeNoStore<
                                                std::remove_cv_t<std::remove_reference_t<decltype(std::declval<ObjectWithStringKey>().get_key_ref())>>,
                                                ObjectWithStringKey,
                                                &ObjectWithStringKey::IK,
                                                &ObjectWithStringKey::IV>;

using DefaultServiceType = Service<VolatileCascadeStoreWithStringKey,
                                   PersistentCascadeStoreWithStringKey,
                                   TriggerCascadeNoStoreWithStringKey>;

using DefaultCascadeContextType = CascadeContext<VolatileCascadeStoreWithStringKey,
                                                 PersistentCascadeStoreWithStringKey,
                                                 TriggerCascadeNoStoreWithStringKey>;
} // namespace cascade
} // namespace derecho
