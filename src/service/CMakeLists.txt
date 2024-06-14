set(UDL_SIGNATURE_HEADER ${CMAKE_BINARY_DIR}/include/cascade/detail/udl_signature.hpp)
set(UDL_SIGNATURE_BOOTSTRAPPING_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/bootstrapping_udl_signature.hpp)

#add_custom_target(bootstrapping_udl_signature
#    COMMAND /bin/sh -c "if [ ! -f ${UDL_SIGNATURE_HEADER} ]; then echo recreating ${UDL_SIGNATURE_HEADER}; ${CMAKE_COMMAND} -E copy ${UDL_SIGNATURE_BOOTSTRAPPING_HEADER} ${UDL_SIGNATURE_HEADER}; fi"
#    VERBATIM
#    BYPRODUCTS ${UDL_SIGNATURE_HEADER}
#)

add_library(udl_signature SHARED dummy_udl.cpp)
target_include_directories(udl_signature PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(udl_signature derecho::derecho)
set_target_properties(udl_signature PROPERTIES
    PREFIX  _
    OUTPUT_NAME dummy
    SUFFIX _)
# add_dependencies(udl_signature bootstrapping_udl_signature)
add_custom_command(TARGET udl_signature POST_BUILD
    COMMAND echo "\\\#pragma once" > udl_signature.hpp
    COMMAND ${CMAKE_NM} _dummy_ | grep cascade | grep get_uuid | awk '{print \"\#define GET_UUID_SIG \\\"\" \$\$3 \"\\\"\"}' >>
    udl_signature.hpp
    COMMAND ${CMAKE_NM} _dummy_ | grep cascade | grep get_description | awk '{print \"\#define GET_DESCRIPTION_SIG \\\"\" \$\$3 \"\\\"\"}' >>
    udl_signature.hpp
    COMMAND ${CMAKE_NM} _dummy_ | grep cascade | grep initialize      | awk '{print \"\#define INITIALIZE_SIG \\\"\" \$\$3 \"\\\"\"}' >>
    udl_signature.hpp
    COMMAND ${CMAKE_NM} _dummy_ | grep cascade | grep get_observer    | awk '{print \"\#define GET_OBSERVER_SIG \\\"\" \$\$3 \"\\\"\"}' >>
    udl_signature.hpp
    COMMAND ${CMAKE_NM} _dummy_ | grep cascade | grep release         | awk '{print \"\#define RELEASE_SIG \\\"\" \$\$3 \"\\\"\"}' >>
    udl_signature.hpp
    COMMAND ${CMAKE_COMMAND} -E remove ${CMAKE_BINARY_DIR}/include/cascade/detail/udl_signature.hpp
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/udl_signature.hpp ${CMAKE_BINARY_DIR}/include/cascade/detail/udl_signature.hpp
)

if (ENABLE_MPROC)
add_library(service OBJECT
    service.cpp data_flow_graph.cpp _user_defined_logic_interface.cpp
    mproc/mproc_manager_client.cpp)
else ()
add_library(service OBJECT
    service.cpp data_flow_graph.cpp _user_defined_logic_interface.cpp)
endif()
target_include_directories(service PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(service derecho::derecho libwsong::perf)
add_dependencies(service udl_signature)

if (ENABLE_MPROC)
add_executable(mproc_manager mproc/mproc_manager.cpp)
target_include_directories(mproc_manager PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)
target_link_libraries(mproc_manager rpclib::rpc libwsong::ipc dl pthread)
set_target_properties(mproc_manager PROPERTIES OUTPUT_NAME cascade_mproc_manager)
endif()

add_executable(server server.cpp)
target_include_directories(server PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)
target_link_libraries(server cascade dl pthread libwsong::perf)
target_link_options(server PUBLIC -rdynamic)
set_target_properties(server PROPERTIES OUTPUT_NAME cascade_server)
add_custom_command(TARGET server POST_BUILD
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
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json
                   ${CMAKE_CURRENT_BINARY_DIR}/cfg/n4/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json
                   ${CMAKE_CURRENT_BINARY_DIR}/cfg/n5/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json
                   ${CMAKE_CURRENT_BINARY_DIR}/cfg/n6/layout.json
    COMMENT "prepare configuration"
)

add_executable(client client.cpp perftest.cpp)
target_include_directories(client PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)
target_include_directories(client PUBLIC
    $<BUILD_INTERFACE:${Readline_INCLUDE_DIRS}>
)
target_link_libraries(client cascade ${Readline_LIBRARIES} pthread libwsong::perf)
if(ENABLE_EVALUATION)
    target_link_libraries(client rpclib::rpc)
endif()
set_target_properties(client PROPERTIES OUTPUT_NAME cascade_client)

# install
install(TARGETS client server
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

add_subdirectory(python)
add_subdirectory(java)
add_subdirectory(cs)
add_subdirectory(fuse)
