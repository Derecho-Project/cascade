cmake_minimum_required(VERSION 3.10.0)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

add_executable(pcli pipeline_client.cpp)
target_include_directories(pcli PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)
target_link_libraries(pcli cascade pthread)

add_library(pipeline_udl SHARED pipeline_udl.cpp)
target_include_directories(pipeline_udl PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)
target_link_libraries(pipeline_udl cascade)
add_custom_command(TARGET pipeline_udl POST_BUILD
     COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/trigger_put_pipeline_cfg
     ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/run.sh.tmp
     ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/n0/run.sh 
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/run.sh.tmp
     ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/n1/run.sh 
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/run.sh.tmp
     ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/n2/run.sh 
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/dfgs.json.tmp
     ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/n0/dfgs.json 
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/dfgs.json.tmp
     ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/n1/dfgs.json 
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/dfgs.json.tmp
     ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/n2/dfgs.json 
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/udl_dlls.cfg.tmp
     ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/n0/udl_dlls.cfg 
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/udl_dlls.cfg.tmp
     ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/n1/udl_dlls.cfg 
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/udl_dlls.cfg.tmp
     ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/n2/udl_dlls.cfg 
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/layout.json
     ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/n0/layout.json 
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/layout.json
     ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/n1/layout.json 
     COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/layout.json
     ${CMAKE_CURRENT_BINARY_DIR}/trigger_put_pipeline_cfg/n2/layout.json
     DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/trigger_put_pipeline_cfg/run.sh.tmp
             ${CMAKE_CURRENT_SOURCE_DIR}/trigger_put_pipeline_cfg/dfgs.json.tmp
             ${CMAKE_CURRENT_SOURCE_DIR}/trigger_put_pipeline_cfg/udl_dlls.cfg.tmp
             ${CMAKE_CURRENT_SOURCE_DIR}/trigger_put_pipeline_cfg/layout.json
     COMMENT "prepare console_printer configuration"
)
