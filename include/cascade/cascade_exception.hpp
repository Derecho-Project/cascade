#pragma once

#include <string>
#include <derecho/core/derecho_exception.hpp>
#include <derecho/persistent/PersistentInterface.hpp>

namespace derecho {
namespace cascade {

struct cascade_exception : public derecho_exception {
    cascade_exception(const std::string& message) : derecho_exception(message) {}
};

struct invalid_value_exception : public cascade_exception {
    invalid_value_exception(const std::string& message) : cascade_exception(message) {}
};

struct invalid_version_exception : cascade_exception {
    invalid_version_exception (persistent::version_t previous_version,
                               persistent::version_t previous_version_by_key) :
        cascade_exception("Invalid version found while test against version:" + std::to_string(previous_version) + "/"
                          + std::to_string(previous_version_by_key)) {}
};

}
}
