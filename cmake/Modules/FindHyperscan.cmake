# The Hyperscan library is built with CMake, but for some reason does not provide
# a CMake package config file with imported targets. This module finds it the
# old-fashioned way, until the Hyperscan developers start supporting modern CMake.

# This module defines
# Hyperscan_FOUND, if false, the Hyperscan library is not installed
# Hyperscan_LIBRARIES, the name of the library to link against
# Hyperscan_INCLUDE_DIRS, where to find hs/hs.h
include(FindPackageHandleStandardArgs)

# Search for the path containing library's headers
find_path(Hyperscan_ROOT_DIR
    NAMES include/hs/hs.h
)

# Search for include directory
find_path(Hyperscan_INCLUDE_DIR
    NAMES hs/hs.h
    HINTS ${Hyperscan_ROOT_DIR}/include
)

# Search for library
find_library(Hyperscan_LIBRARY
    NAMES hs
    HINTS ${Hyperscan_ROOT_DIR}/lib
)

# Conditionally set Hyperscan_FOUND value
find_package_handle_standard_args(Hyperscan DEFAULT_MSG
    Hyperscan_LIBRARY Hyperscan_INCLUDE_DIR)

# Hide these variables in cmake GUIs
mark_as_advanced(
    Hyperscan_ROOT_DIR
    Hyperscan_INCLUDE_DIR
    Hyperscan_LIBRARY
)

if(Hyperscan_FOUND)
    set(Hyperscan_INCLUDE_DIRS ${Hyperscan_INCLUDE_DIR})
    set(Hyperscan_LIBRARIES ${Hyperscan_LIBRARY})
endif()
