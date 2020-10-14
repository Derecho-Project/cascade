cmake_minimum_required(VERSION 3.10.1)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

add_executable(wanagent_server wanagent_server.cpp)
target_include_directories(wanagent_server PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(wanagent_server pthread)

add_custom_command(TARGET wanagent_server POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/wan.json
        ${CMAKE_CURRENT_BINARY_DIR}/wan.json
)