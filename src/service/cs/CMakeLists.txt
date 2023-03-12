cmake_minimum_required(VERSION 3.10.0)

set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

find_program(DOTNET_CMD dotnet)

if (DOTNET_CMD)
    execute_process(COMMAND ${DOTNET_CMD} --version OUTPUT_VARIABLE DOTNET_VER OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND bash "-c" "echo ${DOTNET_VER} | sed 's/\\./ /g' | awk '{print $1}'" OUTPUT_VARIABLE DOTNET_MAJOR_VER OUTPUT_STRIP_TRAILING_WHITESPACE)
    if (${DOTNET_MAJOR_VER} LESS 6)
        message(WARNING ".NET ${DOTNET_VER} is below version 6. Disable C# support.")
    else ()
        message(NOTICE ".NET ${DOTNET_VER} is found at ${DOTNET_CMD}. Enable C# support.")
        add_library(cascade_client_cs SHARED cascade_client_cs.cpp)
        target_include_directories(cascade_client_cs PRIVATE
            $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
            $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        )
        target_compile_definitions(cascade_client_cs PUBLIC __EXTERNAL_CLIENT__) 
        target_link_libraries(cascade_client_cs PUBLIC cascade)
        add_dependencies(cascade_client_cs cascade)
        
        # Custom target will always cause its dependencies to be evaluated and is
        # run by default
        add_custom_target(cascade_client_cs_library ALL
            DEPENDS custom_output
        )
        
        # custom_output will always be rebuilt because it depends on always_rebuild
        add_custom_command(
            OUTPUT custom_output
            COMMAND echo "Built C# Cascade client library."
            DEPENDS always_rebuild
        )
        
        add_custom_command(POST_BUILD
            OUTPUT always_rebuild
            COMMAND dotnet build ${CMAKE_CURRENT_SOURCE_DIR}/CascadeClient.csproj
            COMMAND rm -rf ${CMAKE_CURRENT_SOURCE_DIR}/obj/
            DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/CascadeClient.cs
            DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/CascadeClient.csproj
        )
        
        install(TARGETS cascade_client_cs RUNTIME DESTINATION ${CMAKE_INSTALL_LIBDIR})
    endif()
else ()
    message(NOTICE ".NET is not found. C# support is disabled")
endif()

