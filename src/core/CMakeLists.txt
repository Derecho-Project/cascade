# cascade object
add_library(core OBJECT object.cpp persistence_observer.cpp)
target_include_directories(core PRIVATE
    $<BUILD_INTERFACE:${derecho_INCLUDE_DIRS}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)
