cmake_minimum_required(VERSION 3.10.0)

set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

add_library(cascade_client_cs SHARED cascade_client_cs.cpp)
target_include_directories(cascade_client_cs PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)
add_dependencies(cascade_client_cs cascade)

install(TARGETS cascade_client_cs RUNTIME DESTINATION ${CMAKE_INSTALL_LIBDIR})
