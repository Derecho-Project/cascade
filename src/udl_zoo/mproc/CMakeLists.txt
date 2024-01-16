if (ENABLE_MPROC)
    add_library(mproc_client_udl SHARED mproc_client_udl.cpp)
    target_include_directories(mproc_client_udl PRIVATE
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
    )
    target_link_libraries(mproc_client_udl cascade libwsong::ipc)
    add_dependencies(mproc_client_udl cascade)
    
    add_executable(mproc_server mproc_server.cpp)
    target_include_directories(mproc_server PRIVATE
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
    )
    target_link_libraries(mproc_server cascade libwsong::ipc)
    add_dependencies(mproc_server cascade)
    
    add_executable(mproc_udl_client_tester mproc_udl_client.cpp)
    target_include_directories(mproc_udl_client_tester PRIVATE
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
    )
    target_link_libraries(mproc_udl_client_tester cascade libwsong::ipc)
    add_dependencies(mproc_udl_client_tester cascade)
endif()
