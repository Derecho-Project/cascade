cmake_minimum_required(VERSION 3.10.0)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

add_library(console_printer_udl SHARED console_printer_udl.cpp)
target_include_directories(console_printer_udl PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)
target_link_libraries(console_printer_udl cascade)
add_custom_command(TARGET console_printer_udl POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/console_printer_cfg
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/run.sh.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n0/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/run.sh.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n1/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/run.sh.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n2/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/run.sh.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n3/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/run.sh.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n4/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/layout.json.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n0/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/layout.json.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n1/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/layout.json.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n2/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/layout.json.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n3/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/layout.json.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n4/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/dfgs.json.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n0/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/dfgs.json.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n1/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/dfgs.json.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n2/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/dfgs.json.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n3/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/dfgs.json.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n4/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/udl_dlls.cfg.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n0/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/udl_dlls.cfg.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n1/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/udl_dlls.cfg.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n2/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/udl_dlls.cfg.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n3/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/udl_dlls.cfg.tmp
    ${CMAKE_CURRENT_BINARY_DIR}/console_printer_cfg/n4/udl_dlls.cfg
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/console_printer_cfg/run.sh.tmp
            ${CMAKE_CURRENT_SOURCE_DIR}/console_printer_cfg/layout.json.tmp
            ${CMAKE_CURRENT_SOURCE_DIR}/console_printer_cfg/dfgs.json.tmp
            ${CMAKE_CURRENT_SOURCE_DIR}/console_printer_cfg/udl_dlls.cfg.tmp
    COMMENT "prepare console_printer configuration"
)
