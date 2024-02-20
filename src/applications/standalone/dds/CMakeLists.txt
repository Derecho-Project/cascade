cmake_minimum_required(VERSION 3.12.0)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(cascade_dds CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/Modules")

find_package(cascade CONFIG REQUIRED)
find_package(Readline REQUIRED)

find_package(Java 1.11 QUIET)

set(UDL_UUID 94f8509c-a6e6-11ec-a9f5-0242ac110002)

set(DISABLE_DDS_COPY 0)

# enabling per topic timestamps and disable cascade timestamping infrastructure, aka TimestampLogger::log()
set(USE_DDS_TIMESTAMP_LOG 0)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/cascade_dds/config.h)

add_library(cascade_dds SHARED src/dds.cpp)
target_include_directories(cascade_dds PUBLIC
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
target_link_libraries(cascade_dds derecho::cascade pthread)

add_executable(cascade_dds_client src/client.cpp)
target_include_directories(cascade_dds PUBLIC
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
target_link_libraries(cascade_dds_client derecho::cascade cascade_dds readline pthread)

add_library(cascade_dds_udl SHARED src/dds_udl.cpp)
target_include_directories(cascade_dds_udl PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)
add_custom_command(TARGET cascade_dds_udl POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/cfg
        ${CMAKE_CURRENT_BINARY_DIR}/cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n0/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n1/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n2/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n3/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/dfgs.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n0/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/dfgs.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n1/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/dfgs.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n2/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/dfgs.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n3/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/udl_dlls.cfg
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n0/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/udl_dlls.cfg
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n1/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/udl_dlls.cfg
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n2/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/udl_dlls.cfg
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n3/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/dds.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n0/dds.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/dds.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n1/dds.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/dds.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n2/dds.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/dds.json
        ${CMAKE_CURRENT_BINARY_DIR}/cfg/n3/dds.json
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/cfg/run.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/cfg/layout.json
            ${CMAKE_CURRENT_SOURCE_DIR}/cfg/dfgs.json
            ${CMAKE_CURRENT_SOURCE_DIR}/cfg/udl_dlls.cfg
            ${CMAKE_CURRENT_SOURCE_DIR}/src/dds.json
    COMMENT "prepare cascade_dds demo configuration"
)
target_link_libraries(cascade_dds_udl derecho::cascade cascade_dds)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/src/gen_dfgs.py.in ${CMAKE_CURRENT_BINARY_DIR}/cfg/gen_dfgs.py)
