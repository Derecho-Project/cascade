if (DOTNET_CMD)
    execute_process(COMMAND ${DOTNET_CMD} --version OUTPUT_VARIABLE DOTNET_VER OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND bash "-c" "echo ${DOTNET_VER} | sed 's/\\./ /g' | awk '{print $1}'" OUTPUT_VARIABLE DOTNET_MAJOR_VER OUTPUT_STRIP_TRAILING_WHITESPACE)
    if (${DOTNET_MAJOR_VER} LESS 6)
        message(WARNING ".NET ${DOTNET_VER} is below version 6. Disable C# support.")
    else ()
        message(NOTICE ".NET ${DOTNET_VER} is found at ${DOTNET_CMD}. Enable C# support.")
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
        add_dependencies(member_client_cs cascade external_client_cs) # avoid parallelly building member_client and external client.

        install(TARGETS external_client_cs RUNTIME DESTINATION ${CMAKE_INSTALL_LIBDIR})
        install(TARGETS member_client_cs RUNTIME DESTINATION ${CMAKE_INSTALL_LIBDIR})

        # Custom target will always cause its dependencies to be evaluated and is
        # run by default
        add_custom_target(cascade_client_cs_library ALL
            DEPENDS custom_output
        )

        # custom_output will always be rebuilt because it depends on always_rebuild
        add_custom_command(
            OUTPUT custom_output
            COMMAND echo "Built C# Cascade client CLI."
            DEPENDS always_rebuild
        )

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

        # Build the C# CLI.
        add_custom_command(POST_BUILD
            OUTPUT always_rebuild
            COMMAND cp ${CMAKE_CURRENT_BINARY_DIR}/ExternalClientCS.dll ${CMAKE_CURRENT_SOURCE_DIR}/cli/
            COMMAND dotnet build ${CMAKE_CURRENT_SOURCE_DIR}/cli/CascadeClientCLI.csproj -o ${CMAKE_CURRENT_BINARY_DIR}
            DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/cli/CascadeClientCLI.csproj
            DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/cli/CascadeClientCLI.cs
            DEPENDS external_client_cs
        )
    endif()
else ()
    message(NOTICE ".NET is not found. C# support is disabled")
endif()
