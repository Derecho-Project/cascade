#pragma once
#include "cascade/cascade.hpp"
#include "cascade/object.hpp"
#include "cascade/object_pool_metadata.hpp"
#include "cascade/service.hpp"

#include <derecho/conf/conf.hpp>

#define CONF_VCS_UINT64KEY_LAYOUT "CASCADE/VOLATILECASCADESTORE/UINT64/layout"
#define CONF_VCS_STRINGKEY_LAYOUT "CASCADE/VOLATILECASCADESTORE/STRING/layout"
#define CONF_PCS_UINT64KEY_LAYOUT "CASCADE/PERSISTENTCASCADESTORE/UINT64/layout"
#define CONF_PCS_STRINGKEY_LAYOUT "CASCADE/PERSISTENTCASCADESTORE/STRING/layout"

namespace derecho {
namespace cascade {

// using VCSU = VolatileCascadeStore<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV>;
using VolatileCascadeStoreWithStringKey = VolatileCascadeStore<
                                                std::remove_cv_t<std::remove_reference_t<decltype(((ObjectWithStringKey*)nullptr)->get_key_ref())>>,
                                                ObjectWithStringKey,
                                                &ObjectWithStringKey::IK,
                                                &ObjectWithStringKey::IV>;
// using PCSU = PersistentCascadeStore<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV,ST_FILE>;
using PersistentCascadeStoreWithStringKey = PersistentCascadeStore<
                                                std::remove_cv_t<std::remove_reference_t<decltype(((ObjectWithStringKey*)nullptr)->get_key_ref())>>,
                                                ObjectWithStringKey,
                                                &ObjectWithStringKey::IK,
                                                &ObjectWithStringKey::IV,ST_FILE>;
// using TCSU = TriggerCascadeNoStore<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV>;
using TriggerCascadeNoStoreWithStringKey = TriggerCascadeNoStore<
                                                std::remove_cv_t<std::remove_reference_t<decltype(((ObjectWithStringKey*)nullptr)->get_key_ref())>>,
                                                ObjectWithStringKey,
                                                &ObjectWithStringKey::IK,
                                                &ObjectWithStringKey::IV>;

using SignatureCascadeStoreWithStringKey = SignatureCascadeStore<
                                                std::remove_cv_t<std::remove_reference_t<decltype(((ObjectWithStringKey*)nullptr)->get_key_ref())>>,
                                                ObjectWithStringKey,
                                                &ObjectWithStringKey::IK,
                                                &ObjectWithStringKey::IV,ST_FILE>;

using DefaultServiceType = Service<VolatileCascadeStoreWithStringKey,
                                   PersistentCascadeStoreWithStringKey,
                                   SignatureCascadeStoreWithStringKey,
                                   TriggerCascadeNoStoreWithStringKey>;

using DefaultCascadeContextType = CascadeContext<VolatileCascadeStoreWithStringKey,
                                                 PersistentCascadeStoreWithStringKey,
                                                 SignatureCascadeStoreWithStringKey,
                                                 TriggerCascadeNoStoreWithStringKey>;

// Specializations for CascadeChain
// For now, these will not be used, and we'll just add SignatureCascadeStore to the default service
using ChainServiceType = Service<PersistentCascadeStoreWithStringKey,
                                 SignatureCascadeStoreWithStringKey>;

using ChainContextType = CascadeContext<PersistentCascadeStoreWithStringKey,
                                        SignatureCascadeStoreWithStringKey>;

} // namespace cascade
} // namespace derecho
