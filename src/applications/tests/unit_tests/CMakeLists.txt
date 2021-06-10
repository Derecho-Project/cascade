cmake_minimum_required(VERSION 3.12.4)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(cascade CXX)


add_executable(dfg dfg.cpp)
target_include_directories(dfg PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(dfg cascade)
add_custom_command(TARGET dfg POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/dfgs.json
    ${CMAKE_CURRENT_BINARY_DIR}/dfgs.json
)

add_executable(object_pool_metadata object_pool_metadata.cpp)
target_include_directories(object_pool_metadata PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(object_pool_metadata cascade)
