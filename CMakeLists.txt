cmake_minimum_required(VERSION 3.15)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(cascade CXX)

# Version
set(cascade_VERSION 1.0.2)
set(cascade_build_VERSION 1.0.2)

# C++ STANDARD
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
if (${USE_VERBS_API})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_VERBS_API")
endif()
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Og -Wall -ggdb -gdwarf-3 -ftemplate-backtrace-limit=0 -DEVALUATION")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Ofast -Wall -DEVALUATION -fcompare-debug-second")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -Ofast -Wall -ggdb -gdwarf-3")

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/Modules")

include(GNUInstallDirs)

if ( NOT DEFINED CMAKE_INSTALL_BINDIR )
    set( CMAKE_INSTALL_BINDIR bin )
endif ()

if ( NOT DEFINED CMAKE_INSTALL_LIBDIR )
    set( CMAKE_INSTALL_LIBDIR lib )
endif ()

if ( NOT DEFINED CMAKE_INSTALL_INCLUDEDIR )
    set( CMAKE_INSTALL_INCLUDEDIR include )
endif ( )

set(ENABLE_MPROC 1)

find_package(mutils REQUIRED)
if(mutils_FOUND)
    message(STATUS "Found mutils in ${mutils_INCLUDE_DIRS}")
endif()

# Cascade includes a custom Find module for GNU Readline (required by client.cpp)
find_package(Readline REQUIRED)

# spdlog_FOUND
find_package(spdlog 1.12.0 REQUIRED)

# openssl_FOUND
find_package(OpenSSL REQUIRED)

# derecho
find_package(derecho CONFIG REQUIRED)

# json
find_package(nlohmann_json 3.9.1 REQUIRED)

# provides the import target rpclib::rpc
find_package(rpclib 2.3.0 REQUIRED)

# detect libwsong library, which is required for the mproc feature
find_package(libwsong REQUIRED)

# Hyperscan, which isn't packaged correctly and needs a custom Find module
find_package(Hyperscan REQUIRED)
if(Hyperscan_FOUND)
    include_directories(${Hyperscan_INCLUDE_DIRS})
endif()

# dotnet
find_program(DOTNET_CMD dotnet)


# Doxygen, optional to generate documentation HTML
find_package(Doxygen)

set(CMAKE_REQUIRED_DEFINITIONS -DFUSE_USE_VERSION=30)
include(CheckIncludeFiles)
include(CheckIncludeFileCXX)

# fuse
CHECK_INCLUDE_FILES("fuse3/fuse.h;fuse3/fuse_lowlevel.h" HAS_FUSE)

# boolinq
CHECK_INCLUDE_FILE_CXX("boolinq/boolinq.h" HAS_BOOLINQ)

# enable evaluation
set (ENABLE_EVALUATION 1)
set (DUMP_TIMESTAMP_WORKAROUND 1)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/cascade/config.h)

add_subdirectory(src/core)
add_subdirectory(src/utils)
add_subdirectory(src/service)
add_subdirectory(src/udl_zoo/mproc)
add_subdirectory(src/udl_zoo/csharp)
add_subdirectory(src/udl_zoo/python)
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
    PUBLIC
    derecho::derecho
    spdlog::spdlog
    mutils::mutils
    OpenSSL::Crypto
    rpclib::rpc
    libwsong::perf
    ${Hyperscan_LIBRARIES})
set_target_properties(cascade PROPERTIES
    SOVERSION ${cascade_VERSION}
    VERSION ${cascade_build_VERSION}
)
add_dependencies(cascade core service utils)
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
    "${CMAKE_CURRENT_BINARY_DIR}/cmake/cascadeConfigVersion.cmake"
    VERSION ${cascade_build_VERSION}
    COMPATIBILITY AnyNewerVersion
)

export (EXPORT cascadeTargets
    FILE "${CMAKE_CURRENT_BINARY_DIR}/cmake/cascadeTargets.cmake"
)

set(ConfigPackageLocation ${CMAKE_INSTALL_LIBDIR}/cmake/cascade)

configure_package_config_file(cascadeConfig.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/cmake/cascadeConfig.cmake"
    INSTALL_DESTINATION ${ConfigPackageLocation}
    PATH_VARS CMAKE_INSTALL_LIBDIR CMAKE_INSTALL_INCLUDEDIR
)

install(EXPORT cascadeTargets
    FILE cascadeTargets.cmake
    NAMESPACE derecho::
    DESTINATION ${ConfigPackageLocation}
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/cmake/cascadeConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/cmake/cascadeConfigVersion.cmake"
    DESTINATION ${ConfigPackageLocation}
)

set (DOXYGEN_EXCLUDE src/applications src/service/cs src/service/python src/service/java)
set (DOXYGEN_PROJECT_NUMBER ${cascade_VERSION})
set (DOXYGEN_TAB_SIZE 4)
set (DOXYGEN_WARNINGS NO)
set (DOXYGEN_WARN_IF_UNDOCUMENTED NO)
set (DOXYGEN_WARN_NO_PARAMDOC NO)
if (DOXYGEN_FOUND)
    # create docs
    doxygen_add_docs(docs
        ${CMAKE_SOURCE_DIR}/README.md
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/src
        ALL
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    )
endif()
