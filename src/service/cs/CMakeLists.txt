cmake_minimum_required(VERSION 3.10.0)

set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

find_package(derecho CONFIG REQUIRED)
find_package(cascade CONFIG REQUIRED)

add_library(cascade_client_cs SHARED cascade_client_cs.cpp)
target_include_directories(cascade_client_cs PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)
target_compile_definitions(cascade_client_cs PUBLIC __EXTERNAL_CLIENT__) 
target_link_libraries(cascade_client_cs derecho::cascade)
add_dependencies(cascade_client_cs cascade)

add_custom_command(TARGET cascade_client_cs POST_BUILD
    COMMAND dotnet build ${CMAKE_CURRENT_SOURCE_DIR}/CascadeClient.csproj
    COMMAND rm -rf ${CMAKE_CURRENT_SOURCE_DIR}/obj/
    COMMENT "building C# client library"
)

install(TARGETS cascade_client_cs RUNTIME DESTINATION ${CMAKE_INSTALL_LIBDIR})
