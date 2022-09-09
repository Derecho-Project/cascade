cmake_minimum_required(VERSION 3.12.4)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(cascade CXX)

# Version
set(cascade_VERSION 1.0rc0)
set(cascade_build_VERSION 1.0rc0)

# C++ STANDARD
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
if (${USE_VERBS_API})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_VERBS_API")
endif()
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -Wall -ggdb -gdwarf-3 -ftemplate-backtrace-limit=0 -DEVALUATION")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wall -DEVALUATION -fcompare-debug-second")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -Wall -ggdb -gdwarf-3")

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/Modules")

include(GNUInstallDirs)

if ( NOT DEFINED CMAKE_INSTALL_LIBDIR )
    set( CMAKE_INSTALL_LIBDIR lib )
endif ()

if ( NOT DEFINED CMAKE_INSTALL_INCLUDEDIR )
    set( CMAKE_INSTALL_INCLUDEDIR include )
endif ( )

# The mutils package exports its location information in the "old" way,
# with INCLUDE_DIRS and LIBRARIES variables

# mutils_FOUND
# mutils_INCLUDE_DIRS
# mutils_LIBRARIES
find_package(mutils REQUIRED)
if(mutils_FOUND)
    message(STATUS "Found mutils in ${mutils_INCLUDE_DIRS}")
    include_directories(${mutils_INCLUDE_DIRS})
endif()

# Cascade includes a custom Find module for GNU Readline (required by client.cpp)
find_package(Readline REQUIRED)

# spdlog_FOUND
find_package(spdlog 1.3.1 REQUIRED)

# openssl_FOUND
find_package(OpenSSL REQUIRED)

# derecho
find_package(derecho CONFIG REQUIRED)

# Derecho exports an IMPORT target, following the "new" way of
# doing things, but for some reason we still need to do this
# in order for some of the sub-components of Cascade to compile.
# Ideally, if every target that needs the Derecho headers declares
# its dependency on the IMPORT target derecho::derecho (i.e. with
# target_link_libraries), they will each automatically include the
# Derecho headers from the location specified by that target
if(derecho_FOUND)
    include_directories(${derecho_INCLUDE_DIRS})
endif()

# json
find_package(nlohmann_json 3.2.0 REQUIRED)

set(CMAKE_REQUIRED_DEFINITIONS -DFUSE_USE_VERSION=30)
include(CheckIncludeFiles)
include(CheckIncludeFileCXX)

# stateful udl support
set(HAS_STATEFUL_UDL_SUPPORT 1)

# fuse
CHECK_INCLUDE_FILES("fuse3/fuse.h;fuse3/fuse_lowlevel.h" HAS_FUSE)

# boolinq
CHECK_INCLUDE_FILE_CXX("boolinq/boolinq.h" HAS_BOOLINQ)

# enable evaluation
set (ENABLE_EVALUATION 1)
set (DUMP_TIMESTAMP_WORKAROUND 1)

if (ENABLE_EVALUATION)
    # provides the import target rpclib::rpc
    find_package(rpclib 2.3.0 REQUIRED)
endif()

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/cascade/config.h)

add_subdirectory(src/core)
add_subdirectory(src/utils)
add_subdirectory(src/service)
add_subdirectory(src/applications/tests)

# make libcascade.so
add_library(cascade SHARED
    $<TARGET_OBJECTS:core>
    $<TARGET_OBJECTS:service>
    $<TARGET_OBJECTS:utils>
)
# not ready yet:
#    $<TARGET_OBJECTS:utils>)

target_link_libraries(cascade
    derecho::derecho
    spdlog::spdlog
    ${mutils_LIBRARIES}
    OpenSSL::Crypto)
set_target_properties(cascade PROPERTIES
    SOVERSION ${cascade_VERSION}
    VERSION ${cascade_build_VERSION}
)
add_dependencies(cascade core)
#    utils core)

# make install
install(TARGETS cascade EXPORT cascadeTargets
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY
        ${CMAKE_CURRENT_SOURCE_DIR}/include/cascade
        ${CMAKE_CURRENT_BINARY_DIR}/include/cascade
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        FILES_MATCHING
        PATTERN "*.h"
        PATTERN "*.hpp")

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/cascade/cascadeConfigVersion.cmake"
    VERSION ${cascade_VERSION}
    COMPATIBILITY AnyNewerVersion
)

export (EXPORT cascadeTargets
    FILE "${CMAKE_CURRENT_BINARY_DIR}/cascade/cascadeTargets.cmake"
)

#configure_file (cascadeConfig.cmake
#    "${CMAKE_CURRENT_BINARY_DIR}/cascade/cascadeConfig.cmake"
#)

set(ConfigPackageLocation ${CMAKE_INSTALL_LIBDIR}/cmake/cascade)

configure_package_config_file(cascadeConfig.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/cascade/cascadeConfig.cmake"
    INSTALL_DESTINATION ${ConfigPackageLocation}
    PATH_VARS CMAKE_INSTALL_LIBDIR CMAKE_INSTALL_INCLUDEDIR ConfigPackageLocation
)

install(EXPORT cascadeTargets
    FILE cascadeTargets.cmake
    NAMESPACE derecho::
    DESTINATION ${ConfigPackageLocation}
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/cascade/cascadeConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/cascade/cascadeConfigVersion.cmake"
    DESTINATION ${ConfigPackageLocation}
)
