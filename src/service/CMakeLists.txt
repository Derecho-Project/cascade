cmake_minimum_required(VERSION 3.10.1)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

# service object
#add_library(core OBJECT object.cpp)
#target_include_directories(core PRIVATE
#    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
#    $<BUILD_INTERFACE:${CMAKE_CURRENT_DIR}>
#)

add_executable(service service.cpp)
target_include_directories(service PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(service cascade)
