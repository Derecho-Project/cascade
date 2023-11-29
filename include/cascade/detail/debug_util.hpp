#pragma once
#include "cascade/config.h"
#include "cascade/utils.hpp"

#include <derecho/conf/conf.hpp>
#include <map>
#include <memory>
#include <type_traits>

#ifdef ENABLE_EVALUATION
#include <derecho/utils/time.h>
#endif

namespace derecho {
namespace cascade {

/**
 * get_pathname(): retrieve the pathname, a.k.a prefix from a key.
 * A pathname identifies the object pool this object belongs to.
 *
 * @tparam KeyType - Type of the Key
 * @param  key     - key
 *
 * @return pathname. An empty string returns for invalid key types and invalid keys.
 */
template <typename KeyType>
inline std::string get_pathname(const KeyType& key);

#ifdef ENABLE_EVALUATION

/**
 * This is a hidden API.
 * TestValueTypeConstructor is to check if the ValueType has a public constructor support such a call:
 * VT(KT,uint8_t*,uint32_t)
 */
template <typename KT, typename VT>
struct TestVTConstructor {
    template <class X, class = decltype(X(KT{}, static_cast<uint8_t*>(nullptr), 0))>
    static std::true_type test(X*);
    template <class X>
    static std::false_type test(...);

    static constexpr bool value = decltype(test<VT>(0))::value;
};

template <typename KT, typename VT>
void make_workload(uint32_t payload_size, uint32_t num_distinct_objects, const KT& key_prefix, std::vector<VT>& objects) {
    if constexpr(TestVTConstructor<KT, VT>::value) {
        const uint32_t buf_size = payload_size - 128 - sizeof(key_prefix);
        uint8_t* buf = (uint8_t*)malloc(buf_size);
        memset(buf, 'A', buf_size);
        for(uint32_t i = 0; i < num_distinct_objects; i++) {
            if constexpr(std::is_convertible_v<KT, std::string>) {
                objects.emplace_back(key_prefix + std::to_string(i), buf, buf_size);
            } else if constexpr(std::is_integral_v<KT>) {
                objects.emplace_back(key_prefix + i, buf, buf_size);
            } else {
                dbg_default_error("Cannot make workload for key type:{}", typeid(KT).name());
                break;
            }
        }
        free(buf);
    } else {
        dbg_default_error("Cannot make workload for key type:{} and value type:{}, because it does not support constructor:VT(KT,uint8_t*,uint32_t)", typeid(KT).name(), typeid(VT).name());
    }
}

#if __cplusplus > 201703L
// C++ 20
#define LOG_TIMESTAMP_BY_TAG(t, g, v, ...)                                                      \
    if constexpr(std::is_base_of<IHasMessageID, std::decay_t<decltype(v)>>::value) {            \
        TimestampLogger::log(t,                                                                  \
                                    g->get_my_id(),                                             \
                                    dynamic_cast<const IHasMessageID*>(&(v))->get_message_id(), \
                                    get_walltime()                                              \
                                            __VA_OPT__(, ) __VA_ARGS__);                        \
    }
#else
// C++ 17
#define LOG_TIMESTAMP_BY_TAG(t, g, v)                                                           \
    if constexpr(std::is_base_of<IHasMessageID, std::decay_t<decltype(v)>>::value) {            \
        TimestampLogger::log(t,                                                                  \
                                    g->get_my_id(),                                             \
                                    dynamic_cast<const IHasMessageID*>(&(v))->get_message_id(), \
                                    get_walltime());                                            \
    }

#define LOG_TIMESTAMP_BY_TAG_EXTRA(t, g, v, e)                                                  \
    if constexpr(std::is_base_of<IHasMessageID, std::decay_t<decltype(v)>>::value) {            \
        TimestampLogger::log(t,                                                                  \
                                    g->get_my_id(),                                             \
                                    dynamic_cast<const IHasMessageID*>(&(v))->get_message_id(), \
                                    get_walltime(), e);                                         \
    }

#endif  //__cplusplus > 201703L

#else

#if __cplusplus > 201703L
#define LOG_TIMESTAMP_BY_TAG(t, g, v, ...)
#else
#define LOG_TIMESTAMP_BY_TAG(t, g, v)
#define LOG_TIMESTAMP_BY_TAG_EXTRA(t, g, v, e)
#endif

#endif  // ENABLE_EVALUATION

template <typename KeyType>
std::string get_pathname(const std::enable_if_t<std::is_convertible<KeyType, std::string>::value, KeyType>& key) {
    const std::string* pstr = dynamic_cast<const std::string*>(&key);
    size_t pos = pstr->rfind(PATH_SEPARATOR);
    if(pos != std::string::npos) {
        return pstr->substr(0, pos);
    }
    return "";
}

template <typename KeyType>
std::string get_pathname(const std::enable_if_t<!std::is_convertible<KeyType, std::string>::value, KeyType>& key) {
    return "";
}

}  // namespace cascade
}  // namespace derecho
