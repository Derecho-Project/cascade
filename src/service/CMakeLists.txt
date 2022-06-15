add_library(service OBJECT service.cpp data_flow_graph.cpp)
target_include_directories(service PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

add_executable(server server.cpp)
target_include_directories(server PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)
target_link_libraries(server cascade dl)
set_target_properties(server PROPERTIES OUTPUT_NAME cascade_server)
add_custom_command(TARGET server POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/cfg
    ${CMAKE_CURRENT_BINARY_DIR}/cfg
)

# add_executable(chain_server chain_server.cpp)
# target_include_directories(chain_server PRIVATE
#     $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
#     $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
#     $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
#     $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
# )
# target_link_libraries(chain_server cascade dl)
# set_target_properties(chain_server PROPERTIES OUTPUT_NAME cascadechain_server)
# add_custom_command(TARGET chain_server POST_BUILD
#     COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/cfg
#     ${CMAKE_CURRENT_BINARY_DIR}/cfg
# )


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
target_link_libraries(client cascade ${Readline_LIBRARIES})
if(ENABLE_EVALUATION)
    target_link_libraries(client rpclib::rpc)
endif()
set_target_properties(client PROPERTIES OUTPUT_NAME cascade_client)

# add_executable(chain_client chain_client.cpp perftest.cpp)
# target_include_directories(chain_client PRIVATE
#     $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
#     $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
#     $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
#     $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
# )
# target_link_libraries(chain_client cascade ${Readline_LIBRARIES})
# if(ENABLE_EVALUATION)
#     target_link_libraries(chain_client rpclib::rpc)
# endif()
# set_target_properties(chain_client PROPERTIES OUTPUT_NAME cascadechain_client)


add_library(sha_hash_udl SHARED sha_hash_udl.cpp)
target_include_directories(sha_hash_udl PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)
target_link_libraries(sha_hash_udl derecho::derecho OpenSSL::Crypto)

# install
install(TARGETS client server
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

add_subdirectory(python)
add_subdirectory(java)
add_subdirectory(fuse)
