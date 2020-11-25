cmake_minimum_required(VERSION 3.10.1)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

add_library(wan_logger OBJECT logger_impl.cpp)
target_include_directories(wan_logger PUBLIC
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

add_library(wan_agent_utils OBJECT wan_agent_utils.cpp)
target_include_directories(wan_agent_utils PUBLIC
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

add_library(wan_agent SHARED 
    wan_agent_impl.cpp
    $<TARGET_OBJECTS:wan_logger>
    $<TARGET_OBJECTS:wan_agent_utils>)
target_include_directories(wan_agent PUBLIC
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
# target_link_libraries(wan_agent wan_logger wan_agent_utils)
target_link_libraries(wan_agent predicate_generator)

add_executable(wan_agent_server wan_agent_server.cpp)
target_include_directories(wan_agent_server PUBLIC
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(wan_agent_server wan_agent pthread predicate_generator)

add_executable(wan_agent_sender wan_agent_sender.cpp)
target_include_directories(wan_agent_sender PUBLIC
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(wan_agent_sender wan_agent pthread predicate_generator)

add_executable(cascade_wan_sender cascade_wan_sender.cpp)
target_include_directories(cascade_wan_sender PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
# cascade has linked lib wan_agent already, so maybe we do not link again here.
target_link_libraries(cascade_wan_sender cascade dl predicate_generator)
add_custom_command(TARGET cascade_wan_sender POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/cfg
    ${CMAKE_CURRENT_BINARY_DIR}/cfg
)
add_custom_command(TARGET cascade_wan_sender POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/larger_cfg
    ${CMAKE_CURRENT_BINARY_DIR}/larger_cfg
)

add_executable(cascade_wan_client cascade_wan_client.cpp)
target_include_directories(cascade_wan_client PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
# cascade has linked lib wan_agent already, so maybe we do not link again here.
target_link_libraries(cascade_wan_client cascade readline predicate_generator)

add_library(cascade_wan_ondata_example SHARED cascade_wan_ondata_library_example.cpp)
target_include_directories(cascade_wan_ondata_example PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

add_executable(perf_with_wan perf_with_wan.cpp)
target_include_directories(perf_with_wan PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
# cascade has linked lib wan_agent already, so maybe we do not link again here.
target_link_libraries(perf_with_wan cascade predicate_generator)
