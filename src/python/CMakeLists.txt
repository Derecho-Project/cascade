cmake_minimum_required(VERSION 2.8.12)
project(cascadePyToy)

set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

set(CMAKE_CXX_FLAGS "-std=c++1z -Wall -g -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -ggdb")

find_package(pybind11)

pybind11_add_module(cascade_py src/wrapper/cascade_py.cpp)

target_include_directories(cascade_py PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

target_link_libraries(cascade_py PUBLIC cascade)

add_custom_command(TARGET cascade_py POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/testNodes ${CMAKE_CURRENT_BINARY_DIR}/testNodes
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/interactiveTest.py ${CMAKE_CURRENT_BINARY_DIR}/interactiveTest.py
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/perf_test.py ${CMAKE_CURRENT_BINARY_DIR}/perf_test.py
)

