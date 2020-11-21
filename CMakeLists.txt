cmake_minimum_required(VERSION 3.10.0)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(cascade CXX)

# Version
set(cascade_VERSION 0.9)
set(cascade_build_VERSION 0.9.1)

# C++ STANDARD
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-fPIC")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -Wall -ggdb -gdwarf-3")
set(CMAKE_CXX_FLAGS_RELEASE "-Wall")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-Wall -ggdb -gdwarf-3")

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/Modules")

if ( NOT DEFINED CMAKE_INSTALL_LIBDIR )
    set( CMAKE_INSTALL_LIBDIR lib )
endif ()

# mutils_FOUND 
# mutils_INCLUDE_DIRS
# mutils_LIBRARIES
find_package(mutils REQUIRED)

# spdlog_FOUND
find_package(spdlog REQUIRED)

# openssl_FOUND
find_package(OpenSSL REQUIRED)

# derecho
find_package(derecho REQUIRED)

# json
find_package(nlohmann_json 3.2.0 REQUIRED)

# predicate_generator
find_package(predicate_generator REQUIRED)

set(CMAKE_REQUIRED_DEFINITIONS -DFUSE_USE_VERSION=30)
include(CheckIncludeFiles)

# fuse
CHECK_INCLUDE_FILES("fuse3/fuse.h;fuse3/fuse_lowlevel.h" HAS_FUSE)
# boolinq
CHECK_INCLUDE_FILES("boolinq/boolinq.h" HAS_BOOLINQ)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/cascade/config.h)

add_subdirectory(src/core)
add_subdirectory(src/utils)
add_subdirectory(src/service)
add_subdirectory(src/test)
add_subdirectory(src/wan_agent)

# make libcascade.so
add_library(cascade SHARED
    $<TARGET_OBJECTS:core>
    $<TARGET_OBJECTS:service>
)
# not ready yet:
#    $<TARGET_OBJECTS:utils>)

target_link_libraries(cascade
    ${derecho_LIBRARIES}
    ${mutils_LIBRARIES} 
    ${OPENSSL_LIBRARIES}
    predicate_generator
    wan_agent)
set_target_properties(cascade PROPERTIES
    SOVERSION ${cascade_VERSION}
    VERSION ${cascade_build_VERSION}
)
add_dependencies(cascade core)
#    utils core)

# make install
install(TARGETS wan_logger EXPORT wan_logger
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY
        include/wan_logger
        DESTINATION include)
install(TARGETS wan_agent_utils EXPORT wan_agent_utils
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY
        include/wan_agent_utils
        DESTINATION include)
install(TARGETS wan_agent EXPORT wan_agent
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY
        include/wan_agent
        DESTINATION include)

install(TARGETS cascade EXPORT cascade
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY
        include/cascade
        DESTINATION include)
install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/include/cascade/config.h
        DESTINATION include/cascade)

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/cascade/cascadeConfigVersion.cmake"
    VERSION ${cascade_VERSION}
    COMPATIBILITY AnyNewerVersion
)

export (EXPORT wan_logger
    FILE "${CMAKE_CURRENT_BINARY_DIR}/wan_logger/wan_loggerTargets.cmake")
export (EXPORT wan_agent_utils
    FILE "${CMAKE_CURRENT_BINARY_DIR}/wan_agent_utils/wan_agent_utilsTargets.cmake")
export (EXPORT wan_agent
    FILE "${CMAKE_CURRENT_BINARY_DIR}/wan_agent/wan_agentTargets.cmake")

export (EXPORT cascade
    FILE "${CMAKE_CURRENT_BINARY_DIR}/cascade/cascadeTargets.cmake"
)

configure_file (cascadeConfig.cmake
    "${CMAKE_CURRENT_BINARY_DIR}/cascade/cascadeConfig.cmake"
)

set(ConfigPackageLocation lib/cmake/cascade)
install(EXPORT wan_logger
    FILE wan_loggerTargets.cmake
    DESTINATION ${ConfigPackageLocation}
)
install(EXPORT wan_agent_utils
    FILE wan_agent_utilsTargets.cmake
    DESTINATION ${ConfigPackageLocation}
)
install(EXPORT wan_agent
    FILE wan_agentTargets.cmake
    DESTINATION ${ConfigPackageLocation}
)
install(EXPORT cascade
    FILE cascadeTargets.cmake
    DESTINATION ${ConfigPackageLocation}
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/cascade/cascadeConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/cascade/cascadeConfigVersion.cmake"
    DESTINATION ${ConfigPackageLocation}
)
