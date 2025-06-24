add_executable(chain_only_client chain_only_client.cpp)
target_include_directories(chain_only_client PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)
target_include_directories(chain_only_client PUBLIC
    $<BUILD_INTERFACE:${Readline_INCLUDE_DIRS}>
)
target_link_libraries(chain_only_client cascade ${Readline_LIBRARIES})
if(ENABLE_EVALUATION)
    target_link_libraries(chain_only_client rpclib::rpc)
endif()

# add_custom_target(cascade_chain_config ALL
add_custom_command(TARGET chain_only_client POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/chain_pipeline_cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n0/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n1/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n2/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n3/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n4/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n5/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n6/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n7/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n0/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n1/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n2/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n3/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n4/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/backup_dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n5/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/backup_dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n6/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/backup_dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n7/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n0/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n1/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n2/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n3/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n4/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/backup_udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n5/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/backup_udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n6/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/backup_udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n7/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n0/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n1/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n2/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n3/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n4/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/backup_layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n5/backup_layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/backup_layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n6/backup_layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/backup_layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n7/backup_layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/derecho-fractus.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n0/derecho-fractus.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/derecho-fractus.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n1/derecho-fractus.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/derecho-fractus.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n2/derecho-fractus.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/derecho-fractus.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n3/derecho-fractus.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/derecho-fractus.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n4/derecho-fractus.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/backup_derecho-fractus.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n5/derecho-fractus.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/backup_derecho-fractus.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n6/derecho-fractus.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/backup_derecho-fractus.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n7/derecho-fractus.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/derecho-local.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n0/derecho-local.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/derecho-local.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n1/derecho-local.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/derecho-local.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n2/derecho-local.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/derecho-local.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n3/derecho-local.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/derecho-local.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n4/derecho-local.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/backup_derecho-local.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n5/derecho-local.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/backup_derecho-local.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n6/derecho-local.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/backup_derecho-local.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n7/derecho-local.cfg
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/chain_pipeline_cfg/run.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/chain_pipeline_cfg/gen_keys.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/chain_pipeline_cfg/dfgs.json
            ${CMAKE_CURRENT_SOURCE_DIR}/chain_pipeline_cfg/udl_dlls.cfg
            ${CMAKE_CURRENT_SOURCE_DIR}/chain_pipeline_cfg/layout.json
            ${CMAKE_CURRENT_SOURCE_DIR}/chain_pipeline_cfg/backup_layout.json
            ${CMAKE_CURRENT_SOURCE_DIR}/chain_pipeline_cfg/backup_dfgs.json
            ${CMAKE_CURRENT_SOURCE_DIR}/chain_pipeline_cfg/derecho-fractus.cfg
            ${CMAKE_CURRENT_SOURCE_DIR}/chain_pipeline_cfg/backup_derecho-fractus.cfg
            ${CMAKE_CURRENT_SOURCE_DIR}/chain_pipeline_cfg/derecho-local.cfg
            ${CMAKE_CURRENT_SOURCE_DIR}/chain_pipeline_cfg/backup_derecho-local.cfg
    COMMENT "prepare cascade_chain configuration"
)

add_custom_command(TARGET chain_only_client POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/primary_test_cfg
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n0/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n1/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n2/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n3/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n4/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n5/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n6/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n7/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n0/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n1/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n2/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n3/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n4/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n5/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n6/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n0/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n1/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n2/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n3/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n4/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n5/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n6/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n0/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n1/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n2/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n3/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n4/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n5/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n6/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n7/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/derecho-fractus.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n0/derecho-fractus.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/derecho-fractus.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n1/derecho-fractus.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/derecho-fractus.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n2/derecho-fractus.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/derecho-fractus.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n3/derecho-fractus.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/derecho-fractus.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n4/derecho-fractus.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/derecho-fractus.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n5/derecho-fractus.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/derecho-fractus.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n6/derecho-fractus.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/derecho-fractus.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/primary_test_cfg/n7/derecho-fractus.cfg
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/primary_test_cfg/run.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/primary_test_cfg/dfgs.json
            ${CMAKE_CURRENT_SOURCE_DIR}/primary_test_cfg/udl_dlls.cfg
            ${CMAKE_CURRENT_SOURCE_DIR}/primary_test_cfg/layout.json
            ${CMAKE_CURRENT_SOURCE_DIR}/primary_test_cfg/derecho-fractus.cfg
    COMMENT "prepare chain primary_test configuration"
)

add_custom_command(TARGET chain_only_client POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/docker_test_cfg
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n0/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n1/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n2/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n3/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n4/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n5/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n6/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/run.sh
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n7/run.sh
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n0/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n1/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n2/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n3/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/backup_dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n4/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/backup_dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n5/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/backup_dfgs.json
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n6/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n0/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n1/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n2/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n3/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/backup_udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n4/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/backup_udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n5/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/backup_udl_dlls.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n6/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n0/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n1/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n2/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n3/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/backup_layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n4/backup_layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/backup_layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n5/backup_layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/backup_layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n6/backup_layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/layout.json
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n7/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/derecho.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n0/derecho.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/derecho.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n1/derecho.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/derecho.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n2/derecho.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/derecho.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n3/derecho.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/backup_derecho.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n4/derecho.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/backup_derecho.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n5/derecho.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/backup_derecho.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n6/derecho.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/derecho.cfg
            ${CMAKE_CURRENT_BINARY_DIR}/docker_test_cfg/n7/derecho.cfg
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/docker_test_cfg/run.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/docker_test_cfg/gen_keys.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/docker_test_cfg/dfgs.json
            ${CMAKE_CURRENT_SOURCE_DIR}/docker_test_cfg/udl_dlls.cfg
            ${CMAKE_CURRENT_SOURCE_DIR}/docker_test_cfg/layout.json
            ${CMAKE_CURRENT_SOURCE_DIR}/docker_test_cfg/backup_layout.json
            ${CMAKE_CURRENT_SOURCE_DIR}/docker_test_cfg/backup_dfgs.json
            ${CMAKE_CURRENT_SOURCE_DIR}/docker_test_cfg/derecho.cfg
            ${CMAKE_CURRENT_SOURCE_DIR}/docker_test_cfg/backup_derecho.cfg
    COMMENT "prepare cascade chain docker test configuration"
)
