#pragma once
#include <cascade/cascade.hpp>
#include <cascade/object.hpp>
#include <cascade/object_pool_metadata.hpp>
#include <cascade/service.hpp>
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
                                   TriggerCascadeNoStoreWithStringKey>;

using DefaultCascadeContextType = CascadeContext<VolatileCascadeStoreWithStringKey,
                                                 PersistentCascadeStoreWithStringKey,
                                                 TriggerCascadeNoStoreWithStringKey>;

// Specializations for CascadeChain
/*
 * It would be nice to hide the SignatureCascadeStore from the Service's template
 * parameters, and only add it to the Group's template parameters (from inside the
 * Service constructor), but I can't figure out how to do that without needing to
 * copy-and-paste the entire implementation of Service into a template specialization
 */
using ChainServiceType = Service<PersistentCascadeStoreWithStringKey,
                                 SignatureCascadeStoreWithStringKey>;

using ChainContextType = CascadeContext<PersistentCascadeStoreWithStringKey,
                                        SignatureCascadeStoreWithStringKey>;

} // namespace cascade
} // namespace derecho
