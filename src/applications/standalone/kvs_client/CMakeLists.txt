cmake_minimum_required(VERSION 3.12.0)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(kvs_client CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(nlohmann_json 3.2.0 REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(spdlog 1.3.1 REQUIRED)
find_package(derecho CONFIG REQUIRED)
find_package(cascade CONFIG REQUIRED)

add_executable(kvs_client kvs_client.cpp)
set_target_properties(kvs_client PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${cascade_INCLUDE_DIRS}
)
target_link_libraries(kvs_client derecho::cascade)
add_custom_command(TARGET kvs_client POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/cfg
        ${CMAKE_CURRENT_BINARY_DIR}/cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n0/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n1/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n2/layout.json
    )
