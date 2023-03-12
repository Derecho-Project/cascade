cmake_minimum_required(VERSION 3.10.0)

set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

add_library(external_client_cs SHARED cascade_client_cs.cpp)
target_include_directories(external_client_cs PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)
target_compile_definitions(external_client_cs PUBLIC __EXTERNAL_CLIENT__)
target_link_libraries(external_client_cs cascade)
add_dependencies(external_client_cs cascade)

add_library(member_client_cs SHARED cascade_client_cs.cpp)
target_include_directories(member_client_cs PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)
target_link_libraries(member_client_cs cascade)
add_dependencies(member_client_cs cascade)

# Build the external client C# DLL.
add_custom_command(TARGET external_client_cs POST_BUILD
    COMMAND dotnet build ${CMAKE_CURRENT_SOURCE_DIR}/CascadeClient.csproj -o ${CMAKE_CURRENT_BINARY_DIR} -p:DefineConstants=IS_EXTERNAL_CLIENT /property:AssemblyName=ExternalClientCS
    COMMAND rm -rf ${CMAKE_CURRENT_SOURCE_DIR}/obj/
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/CascadeClient.cs
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/CascadeClient.csproj
    COMMENT "building external client C# DLL"
)

# Build the member client C# DLL.
add_custom_command(TARGET member_client_cs POST_BUILD
    COMMAND dotnet build ${CMAKE_CURRENT_SOURCE_DIR}/CascadeClient.csproj -o ${CMAKE_CURRENT_BINARY_DIR} /property:AssemblyName=MemberClientCS
    COMMAND rm -rf ${CMAKE_CURRENT_SOURCE_DIR}/obj/
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/CascadeClient.cs
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/CascadeClient.csproj
    COMMENT "building member client C# DLL"
)

install(TARGETS external_client_cs RUNTIME DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(TARGETS member_client_cs RUNTIME DESTINATION ${CMAKE_INSTALL_LIBDIR})
