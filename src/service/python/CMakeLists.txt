find_package(Python 3.10 COMPONENTS Interpreter Development QUIET)
find_package(pybind11 QUIET)

if (pybind11_FOUND)
    message(NOTICE "Pybind11 found. Python client enabled.")
    # test if some python dependencies are included
    execute_process(COMMAND pip3 show build RESULT_VARIABLE HAS_PYTHON_BUILD OUTPUT_QUIET ERROR_QUIET)
    if(NOT ${HAS_PYTHON_BUILD} EQUAL 0)
        message(WARNING "pip3 failed to find 'build' package. If you didn't install it with another package manager, please install it by 'pip3 install build'")
    endif()

    execute_process(COMMAND pip3 show setuptools RESULT_VARIABLE HAS_PYTHON_SETUPTOOLS OUTPUT_QUIET ERROR_QUIET)
    if(NOT ${HAS_PYTHON_SETUPTOOLS} EQUAL 0)
        message(WARNING "pip3 failed to find 'setuptools' package. If you didn't install it with another package manager, please install it by 'pip3 install setuptools'")
    endif()

    execute_process(COMMAND pip3 show virtualenv RESULT_VARIABLE HAS_PYTHON_VIRTUALENV OUTPUT_QUIET ERROR_QUIET)
    if(NOT ${HAS_PYTHON_VIRTUALENV} EQUAL 0)
        message(WARNING "pip3 failed to find 'virtualenv' package. If you didn't install it with another package manager, please install it by 'pip3 install python-venv' or 'pip3 install virtualenv'")
    endif()

    configure_file(setup.py.in ${CMAKE_CURRENT_BINARY_DIR}/setup.py)
    configure_file(pipinstall.cmake.in ${CMAKE_CURRENT_BINARY_DIR}/pipinstall.cmake)

    # pybind11 is found, so we compile the python bindings.
    pybind11_add_module(external_client_py cascade_client_py.cpp)
    target_include_directories(external_client_py PRIVATE
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    )
    target_compile_definitions(external_client_py PUBLIC __EXTERNAL_CLIENT__)
    target_link_libraries(external_client_py PUBLIC cascade)
    add_custom_command(TARGET external_client_py PRE_LINK
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/derecho ${CMAKE_CURRENT_BINARY_DIR}/derecho)
    # Use the Python_EXECUTABLE variable defined by FindPython since some systems call it "python3" and some call it "python"
    add_custom_command(TARGET external_client_py POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/README.md ${CMAKE_CURRENT_BINARY_DIR}/README.md
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/cascade_client.py ${CMAKE_CURRENT_BINARY_DIR}/cascade_client.py
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/cascade_perf.py ${CMAKE_CURRENT_BINARY_DIR}/cascade_perf.py
        COMMAND ${Python_EXECUTABLE} -m build
        DEPENDS cascade_client.py cascade_perf.py
    )
    set_target_properties(external_client_py PROPERTIES
        OUTPUT_NAME derecho/cascade/external_client
    )
    add_dependencies(external_client_py cascade)

    pybind11_add_module(member_client_py cascade_client_py.cpp)
    target_include_directories(member_client_py PRIVATE
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    )
    target_link_libraries(member_client_py PUBLIC cascade)
    add_custom_command(TARGET member_client_py PRE_LINK
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/derecho ${CMAKE_CURRENT_BINARY_DIR}/derecho)
    add_custom_command(TARGET member_client_py POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/README.md ${CMAKE_CURRENT_BINARY_DIR}/README.md
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/cascade_client.py ${CMAKE_CURRENT_BINARY_DIR}/cascade_client.py
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/cascade_perf.py ${CMAKE_CURRENT_BINARY_DIR}/cascade_perf.py
        COMMAND ${Python_EXECUTABLE} -m build
        DEPENDS cascade_client.py cascade_perf.py
    )
    set_target_properties(member_client_py PROPERTIES
        OUTPUT_NAME derecho/cascade/member_client
    )
    add_dependencies(member_client_py cascade external_client_py)

    install(SCRIPT ${CMAKE_CURRENT_BINARY_DIR}/pipinstall.cmake)

    install(FILES cascade_client.py cascade_perf.py
            TYPE BIN
            PERMISSIONS OWNER_WRITE OWNER_READ GROUP_READ WORLD_READ OWNER_EXECUTE GROUP_EXECUTE WORLD_EXECUTE
    )
else()
    message(NOTICE "Pybind11 not found. Python client is disabled.")
endif()

