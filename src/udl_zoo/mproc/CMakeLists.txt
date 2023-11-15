find_package(libwsong REQUIRED)

add_library(mproc_udl_client OBJECT mproc_udl_client.cpp)
target_include_directories(mproc_udl_client PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)
target_link_libraries(mproc_udl_client cascade)
add_dependencies(mproc_udl_client cascade)

add_library(mproc_client_udl SHARED mproc_client_udl.cpp)
target_include_directories(mproc_client_udl PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)
target_link_libraries(mproc_client_udl cascade mproc_udl_client libwsong::ipc)
add_dependencies(mproc_client_udl cascade mproc_udl_client)

add_executable(mproc_server mproc_server.cpp)
target_include_directories(mproc_server PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)
target_link_libraries(mproc_server cascade libwsong::ipc)
add_dependencies(mproc_server cascade)

