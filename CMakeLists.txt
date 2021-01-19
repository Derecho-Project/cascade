cmake_minimum_required(VERSION 3.12.4)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(cascade CXX)

# Version
set(cascade_VERSION 0.9)
set(cascade_build_VERSION 0.9.1)

# C++ STANDARD
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-fPIC")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -Wall -ggdb -gdwarf-3 -ftemplate-backtrace-limit=0 -DEVALUATION")
set(CMAKE_CXX_FLAGS_RELEASE "-Wall -DEVALUATION")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-Wall -ggdb -gdwarf-3")

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/Modules")

include(GNUInstallDirs)

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

#opencv
find_package(OpenCV QUIET)

set(CMAKE_REQUIRED_DEFINITIONS -DFUSE_USE_VERSION=30)
set(CMAKE_REQUIRED_LIBRARIES mxnet)
include(CheckIncludeFiles)
include(CheckIncludeFileCXX)

# fuse
CHECK_INCLUDE_FILES("fuse3/fuse.h;fuse3/fuse_lowlevel.h" HAS_FUSE)

# boolinq
CHECK_INCLUDE_FILE_CXX("boolinq/boolinq.h" HAS_BOOLINQ)

#mxnet
CHECK_INCLUDE_FILE_CXX("mxnet-cpp/MxNetCpp.h" HAS_MXNET_CPP)

# enable gpu
set (HAS_NVIDIA_GPU 0)

# enable evaluation
set (ENABLE_EVALUATION 1)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/cascade/config.h)

add_subdirectory(src/core)
add_subdirectory(src/utils)
add_subdirectory(src/service)
add_subdirectory(src/test)

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
    ${OPENSSL_LIBRARIES})
set_target_properties(cascade PROPERTIES
    SOVERSION ${cascade_VERSION}
    VERSION ${cascade_build_VERSION}
)
add_dependencies(cascade core)
#    utils core)

# make install
install(TARGETS cascade EXPORT cascade
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
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

export (EXPORT cascade
    FILE "${CMAKE_CURRENT_BINARY_DIR}/cascade/cascadeTargets.cmake"
)

configure_file (cascadeConfig.cmake
    "${CMAKE_CURRENT_BINARY_DIR}/cascade/cascadeConfig.cmake"
)

set(ConfigPackageLocation ${CMAKE_INSTALL_LIBDIR}/cmake/cascade)
install(EXPORT cascade
    FILE cascadeTargets.cmake
    DESTINATION ${ConfigPackageLocation}
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/cascade/cascadeConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/cascade/cascadeConfigVersion.cmake"
    DESTINATION ${ConfigPackageLocation}
)
