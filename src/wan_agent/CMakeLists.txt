cmake_minimum_required(VERSION 3.10.1)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

add_executable(wan_agent_server wan_agent_server.cpp)
target_include_directories(wan_agent_server PUBLIC
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(wan_agent_server pthread)
add_custom_command(TARGET wan_agent_server POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/wan_server.json
        ${CMAKE_CURRENT_BINARY_DIR}/wan_server.json
)

add_executable(wan_agent_client wan_agent_client.cpp)
target_include_directories(wan_agent_client PUBLIC
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(wan_agent_client pthread)
add_custom_command(TARGET wan_agent_client POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/wan_client.json
        ${CMAKE_CURRENT_BINARY_DIR}/wan_client.json
)

add_executable(cascade_wan_test cascade_wan_test.cpp)
target_include_directories(cascade_wan_test PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(cascade_wan_test cascade)
add_custom_command(TARGET cascade_wan_test POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/cfg
    ${CMAKE_CURRENT_BINARY_DIR}/cfg
)

# add_executable(cascade_wan_client cascade_wan_client.cpp)
# target_include_directories(cascade_wan_client PRIVATE
#     $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
#     $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
#     $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
# )
# target_link_libraries(cascade_wan_client cascade readline)
