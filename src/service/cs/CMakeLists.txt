cmake_minimum_required(VERSION 3.10.0)

set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)



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
add_custom_command(TARGET external_client_py POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/README.md ${CMAKE_CURRENT_BINARY_DIR}/README.md
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/cascade_client.py ${CMAKE_CURRENT_BINARY_DIR}/cascade_client.py
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/cascade_perf.py ${CMAKE_CURRENT_BINARY_DIR}/cascade_perf.py
    COMMAND python -m build
    DEPENDS cascade_client.py cascade_perf.py
)
set_target_properties(external_client_py PROPERTIES
    OUTPUT_NAME derecho/cascade/external_client
)

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
    COMMAND python -m build
    DEPENDS cascade_client.py cascade_perf.py
)
set_target_properties(member_client_py PROPERTIES
    OUTPUT_NAME derecho/cascade/member_client
)

install(FILES cascade_client.py cascade_perf.py
        TYPE BIN
        PERMISSIONS OWNER_WRITE OWNER_READ GROUP_READ WORLD_READ OWNER_EXECUTE GROUP_EXECUTE WORLD_EXECUTE
)
