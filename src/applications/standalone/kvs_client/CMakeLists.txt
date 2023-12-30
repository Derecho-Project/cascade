cmake_minimum_required(VERSION 3.12.0)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(kvs_client CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(cascade CONFIG REQUIRED)

find_package(Java 1.11 QUIET)

add_executable(kvs_client kvs_client.cpp)
target_link_libraries(kvs_client derecho::cascade pthread)
add_custom_command(TARGET kvs_client POST_BUILD
    COMMAND ln -sf ${CMAKE_CURRENT_SOURCE_DIR}/kvs_client.py
        ${CMAKE_CURRENT_BINARY_DIR}/kvs_client.py
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/cfg
        ${CMAKE_CURRENT_BINARY_DIR}/cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n0/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n1/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n2/layout.json
    )

if (Java_FOUND)
    include(UseJava)

    set(CMAKE_JAVA_COMPILE_FLAGS "-source" "11" "-target" "11")

    add_jar(kvs_client_jar
            KVSClient.java
        ENTRY_POINT
            KVSClient
        OUTPUT_NAME
            kvs_client
        MANIFEST
            Manifest.txt
        INCLUDE_JARS
            jars/cascade.jar
    )
    add_custom_command(TARGET kvs_client_jar
        COMMAND ln -sf ${CMAKE_CURRENT_SOURCE_DIR}/jars/cascade.jar ${CMAKE_CURRENT_BINARY_DIR}/cascade.jar
    )
else()
    message(STATUS "Java not found - skipping Java Demo")
endif (Java_FOUND)
