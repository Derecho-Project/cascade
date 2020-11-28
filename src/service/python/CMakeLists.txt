cmake_minimum_required(VERSION 3.10.0)

set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

find_package(pybind11 QUIET)

if (pybind11_FOUND)
    # pybind11 is found, so we compile the python bindings.
    pybind11_add_module(cascade_py cascade_py.cpp)
    target_include_directories(cascade_py PRIVATE
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    )
    target_link_libraries(cascade_py PUBLIC cascade)
    add_custom_command(TARGET cascade_py POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/interactive_test.py ${CMAKE_CURRENT_BINARY_DIR}/interactive_test.py
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/perf_test.py ${CMAKE_CURRENT_BINARY_DIR}/perf_test.py
    )
    install(TARGETS cascade_py EXPORT cascade_py
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
    install(FILES interactive_test.py perf_test.py
            TYPE BIN
            PERMISSIONS OWNER_WRITE OWNER_READ GROUP_READ WORLD_READ OWNER_EXECUTE GROUP_EXECUTE WORLD_EXECUTE)
endif()

