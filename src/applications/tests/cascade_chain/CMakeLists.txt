cmake_minimum_required(VERSION 3.12.4)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

add_custom_target(cascade_chain_config ALL
     COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/chain_pipeline_cfg
     ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/run.sh
     ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n0/run.sh
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/run.sh
     ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n1/run.sh
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/run.sh
     ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n2/run.sh
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/dfgs.json
     ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n0/dfgs.json
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/dfgs.json
     ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n1/dfgs.json
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/dfgs.json
     ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n2/dfgs.json
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/udl_dlls.cfg
     ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n0/udl_dlls.cfg
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/udl_dlls.cfg
     ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n1/udl_dlls.cfg
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/udl_dlls.cfg
     ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n2/udl_dlls.cfg
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/layout.json
     ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n0/layout.json
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/layout.json
     ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n1/layout.json
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/layout.json
     ${CMAKE_CURRENT_BINARY_DIR}/chain_pipeline_cfg/n2/layout.json
     DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/chain_pipeline_cfg/run.sh
             ${CMAKE_CURRENT_SOURCE_DIR}/chain_pipeline_cfg/dfgs.json
             ${CMAKE_CURRENT_SOURCE_DIR}/chain_pipeline_cfg/udl_dlls.cfg
             ${CMAKE_CURRENT_SOURCE_DIR}/chain_pipeline_cfg/layout.json
     COMMENT "prepare cascade_chain configuration"
)
