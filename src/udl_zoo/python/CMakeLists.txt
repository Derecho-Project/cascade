cmake_minimum_required(VERSION 3.12.0)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(python_udl CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Version
set(python_udl_VERSION 1.0)

find_package(Python3 3.10.0 QUIET COMPONENTS Interpreter Development Development.Module Development.Embed NumPy)

if (Python3_FOUND)

    # Configurations
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/config.h)

    # main target
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
        COMMAND echo \'\#!/usr/bin/env bash\' > ${CMAKE_CURRENT_BINARY_DIR}/show_cascade_python_udl.sh
        COMMAND echo "PYTHONUDL=${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}/$<TARGET_FILE_NAME:python_udl>" >> ${CMAKE_CURRENT_BINARY_DIR}/show_cascade_python_udl.sh
        COMMAND echo \'echo $$\{PYTHONUDL\}\' >> ${CMAKE_CURRENT_BINARY_DIR}/show_cascade_python_udl.sh
        COMMAND chmod 755 ${CMAKE_CURRENT_BINARY_DIR}/show_cascade_python_udl.sh
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/cfg/run.sh.tmp
                ${CMAKE_CURRENT_SOURCE_DIR}/cfg/layout.json.tmp
                ${CMAKE_CURRENT_SOURCE_DIR}/cfg/dfgs.json.tmp
                ${CMAKE_CURRENT_SOURCE_DIR}/cfg/udl_dlls.cfg.tmp
                ${CMAKE_CURRENT_SOURCE_DIR}/cfg/python_udls/sample_udls.py
        COMMENT "prepare python_udl configuration"
    )

    # make install
    install(TARGETS python_udl EXPORT cascade_python_udlTargets
            LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
    include(CMakePackageConfigHelpers)
    write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/cascade_python_udlConfigVersion.cmake"
        VERSION ${python_udl_VERSION}
        COMPATIBILITY AnyNewerVersion
    )

    export (EXPORT cascade_python_udlTargets
        FILE "${CMAKE_CURRENT_BINARY_DIR}/cascade_python_udlTargets.cmake"
    )
    
    set(ConfigPackageLocation ${CMAKE_INSTALL_LIBDIR}/cmake/cascade_python_udl)
    
    configure_package_config_file(cascade_python_udlConfig.cmake.in
        "${CMAKE_CURRENT_BINARY_DIR}/cascade_python_udlConfig.cmake"
        INSTALL_DESTINATION ${ConfigPackageLocation}
        PATH_VARS CMAKE_INSTALL_LIBDIR ConfigPackageLocation
    )
    
    install(EXPORT cascade_python_udlTargets
        FILE cascade_python_udlTargets.cmake
        NAMESPACE derecho::
        DESTINATION ${ConfigPackageLocation}
    )
    
    install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/cascade_python_udlConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/cascade_python_udlConfigVersion.cmake"
        DESTINATION ${ConfigPackageLocation}
    )
    
    set(INSTALLED_PYTHON_UDL_LIB_FILE "${CMAKE_INSTALL_LIBDIR}/$<TARGET_FILE_NAME:python_udl>")
    
    install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/show_cascade_python_udl.sh"
        DESTINATION ${CMAKE_INSTALL_BINDIR}
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE) 

endif()
