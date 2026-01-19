# Test for get_by_time API
add_executable(test_get_by_time test_get_by_time.cpp)
target_include_directories(test_get_by_time PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)
target_link_libraries(test_get_by_time cascade pthread)

# Test for put_by_time API
add_executable(test_put_by_time test_put_by_time.cpp)
target_include_directories(test_put_by_time PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(test_put_by_time cascade)

add_custom_command(TARGET test_put_by_time POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/consistency_test_cfg
        ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/derecho.cfg
        ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/n0/derecho.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/derecho.cfg
        ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/n1/derecho.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/derecho.cfg
        ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/n2/derecho.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/derecho.cfg
        ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/n3/derecho.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/layout.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/n0/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/layout.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/n1/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/layout.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/n2/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/layout.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/n3/layout.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/dfgs.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/n0/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/dfgs.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/n1/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/dfgs.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/n2/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/dfgs.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/n3/dfgs.json
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/udl_dlls.cfg.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/n0/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/udl_dlls.cfg.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/n1/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/udl_dlls.cfg.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/n2/udl_dlls.cfg
    COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/udl_dlls.cfg.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/consistency_test_cfg/n3/udl_dlls.cfg
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/consistency_test_cfg/layout.json.tmp
            ${CMAKE_CURRENT_SOURCE_DIR}/consistency_test_cfg/dfgs.json.tmp
            ${CMAKE_CURRENT_SOURCE_DIR}/consistency_test_cfg/udl_dlls.cfg.tmp
    COMMENT "prepare consistency tests configuration"
)
