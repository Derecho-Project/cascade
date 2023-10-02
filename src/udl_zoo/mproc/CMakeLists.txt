find_package(libwsong REQUIRED)

add_library(mproc_connector OBJECT mproc_connector.cpp)

add_library(mproc_udl SHARED mproc_udl.cpp)
target_include_directories(mproc_udl PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)
target_link_libraries(mproc_udl cascade mproc_connector libwsong::ipc)
add_dependencies(mproc_udl cascade mproc_connector)
