cmake_minimum_required(VERSION 3.16)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

# cascade object
add_library(core OBJECT object.cpp persistent_observer.cpp)
target_include_directories(core PRIVATE
    $<BUILD_INTERFACE:${derecho_INCLUDE_DIRS}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)
