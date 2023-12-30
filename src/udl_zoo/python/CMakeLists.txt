cmake_minimum_required(VERSION 3.12.0)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(python_udl CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Version
set(python_udl_VERSION 1.0)

# find_package(nlohmann_json 3.2.0 REQUIRED)
# find_package(OpenSSL REQUIRED)
# find_package(spdlog 1.3.1 REQUIRED)
# find_package(derecho CONFIG REQUIRED)
# find_package(cascade CONFIG REQUIRED)
find_package(Python3 3.10.0 QUIET COMPONENTS Interpreter Development Development.Module Development.Embed NumPy)

if (Python3_FOUND)

    # Configurations
    set(ENABLE_PYTHON_GIL_SWITCH 1)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/config.h)
    
    add_library(python_udl SHARED python_udl.cpp)
    target_include_directories(python_udl PRIVATE
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
    )
    target_link_libraries(python_udl cascade Python3::Python Python3::NumPy)
    add_dependencies(python_udl cascade)
    
    add_custom_command(TARGET python_udl POST_BUILD
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
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/python_udls
            ${CMAKE_CURRENT_BINARY_DIR}/cfg/n0/python_udls
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/python_udls
            ${CMAKE_CURRENT_BINARY_DIR}/cfg/n1/python_udls
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/python_udls
            ${CMAKE_CURRENT_BINARY_DIR}/cfg/n2/python_udls
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/cfg/run.sh.tmp
                ${CMAKE_CURRENT_SOURCE_DIR}/cfg/layout.json.tmp
                ${CMAKE_CURRENT_SOURCE_DIR}/cfg/dfgs.json.tmp
                ${CMAKE_CURRENT_SOURCE_DIR}/cfg/udl_dlls.cfg.tmp
                ${CMAKE_CURRENT_SOURCE_DIR}/cfg/python_udls/sample_udls.py
        COMMENT "prepare python_udl configuration"
    )

endif()
