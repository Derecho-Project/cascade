# cascade object
add_library(core OBJECT object.cpp)
target_include_directories(core PRIVATE
    $<BUILD_INTERFACE:${derecho_INCLUDE_DIRS}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)
target_link_libraries(core derecho::derecho)