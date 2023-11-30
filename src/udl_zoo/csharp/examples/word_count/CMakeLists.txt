cmake_minimum_required(VERSION 3.12.0)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(word_count_cs_udl CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_library(csharp_udl SHARED ../../csharp_udl.cpp ../../gateway_to_managed.cpp ./word_count_cs_client.cpp)

find_package(derecho CONFIG REQUIRED)
find_package(nlohmann_json 3.2.0 REQUIRED)
find_package(cascade CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(spdlog 1.3.1 REQUIRED)

add_executable(word_count_client word_count_cs_client.cpp)
target_include_directories(word_count_client PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)
target_link_libraries(word_count_client derecho::cascade)

target_include_directories(csharp_udl PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)

add_custom_command(TARGET csharp_udl POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/cfg
        ${CMAKE_CURRENT_BINARY_DIR}/cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n0/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n1/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n2/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/dfgs.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n0/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/dfgs.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n1/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/dfgs.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n2/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/udl_dlls.cfg.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n0/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/udl_dlls.cfg.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n1/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/udl_dlls.cfg.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n2/udl_dlls.cfg
    COMMAND mcs -unsafe -t:library ${CMAKE_CURRENT_SOURCE_DIR}/../../api/*.cs -out:GatewayLib.dll
    COMMAND mcs -unsafe -t:library ${CMAKE_CURRENT_SOURCE_DIR}/WordCountMapperUDL.cs ${CMAKE_CURRENT_SOURCE_DIR}/../../api/UserDefinedLogic.cs -out:WordCountMapperUDL.dll
    COMMAND mcs -unsafe -t:library ${CMAKE_CURRENT_SOURCE_DIR}/WordCountReducerUDL.cs ${CMAKE_CURRENT_SOURCE_DIR}/../../api/UserDefinedLogic.cs -out:WordCountReducerUDL.dll
    COMMAND mcs -unsafe -t:library ${CMAKE_CURRENT_SOURCE_DIR}/WordCountReportUDL.cs ${CMAKE_CURRENT_SOURCE_DIR}/../../api/UserDefinedLogic.cs -out:WordCountReportUDL.dll
    COMMAND mv WordCountMapperUDL.dll ${CMAKE_CURRENT_BINARY_DIR}/cfg/
    COMMAND mv WordCountReducerUDL.dll ${CMAKE_CURRENT_BINARY_DIR}/cfg/
    COMMAND mv WordCountReportUDL.dll ${CMAKE_CURRENT_BINARY_DIR}/cfg/
    COMMAND mv GatewayLib.dll ${CMAKE_CURRENT_BINARY_DIR}/cfg/
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/cfg/layout.json.tmp
            ${CMAKE_CURRENT_SOURCE_DIR}/cfg/dfgs.json.tmp
            ${CMAKE_CURRENT_SOURCE_DIR}/cfg/udl_dlls.cfg.tmp
            ${CMAKE_CURRENT_SOURCE_DIR}/WordCountMapperUDL.cs
            ${CMAKE_CURRENT_SOURCE_DIR}/WordCountReducerUDL.cs
            ${CMAKE_CURRENT_SOURCE_DIR}/WordCountReportUDL.cs
            ${CMAKE_CURRENT_SOURCE_DIR}/../../api/UserDefinedLogic.cs
            ${CMAKE_CURRENT_SOURCE_DIR}/../../api/Gateway.cs
            ${CMAKE_CURRENT_SOURCE_DIR}/../../api/ReflectionLogic.cs
    COMMENT "prepare csharp word count udl and cascade configuration"
)