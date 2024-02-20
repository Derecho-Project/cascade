add_executable(dfg dfg.cpp)
target_include_directories(dfg PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(dfg cascade)
add_custom_command(TARGET dfg POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/dfgs.json
    ${CMAKE_CURRENT_BINARY_DIR}/dfgs.json
)

add_executable(object_pool_metadata object_pool_metadata.cpp)
target_include_directories(object_pool_metadata PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(object_pool_metadata cascade)

add_executable(test_persistence_observer test_persistence_observer.cpp)
target_include_directories(test_persistence_observer PRIVATE
$<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(test_persistence_observer cascade)

add_executable(hyperscan_perf hyperscan_perf.cpp)
target_include_directories(hyperscan_perf PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(hyperscan_perf ${Hyperscan_LIBRARIES} cascade)

if (MPROC_ENABLED)
    add_executable(mproc_manager_tester mproc_manager_tester.cpp)
    target_include_directories(mproc_manager_tester PRIVATE
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    )
    target_link_libraries(mproc_manager_tester cascade)
endif()
