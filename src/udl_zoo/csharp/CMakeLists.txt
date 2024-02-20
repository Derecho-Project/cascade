if (DOTNET_CMD)
    add_library(csharp_udl SHARED csharp_udl.cpp gateway_to_managed.cpp)
    
    target_include_directories(csharp_udl PRIVATE
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
    )
    target_link_libraries(csharp_udl cascade)
    add_custom_command(TARGET csharp_udl POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/cfg
            ${CMAKE_CURRENT_BINARY_DIR}/cfg
	# TODO: use dotnet instead of mcs here.
	# COMMAND mcs -unsafe -t:library ${CMAKE_CURRENT_SOURCE_DIR}/api/*.cs -out:GatewayLib.dll
	# COMMAND mv GatewayLib.dll ${CMAKE_CURRENT_BINARY_DIR}/cfg/
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json.tmp
            ${CMAKE_CURRENT_BINARY_DIR}/cfg/n0/layout.json
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json.tmp
            ${CMAKE_CURRENT_BINARY_DIR}/cfg/n1/layout.json
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/layout.json.tmp
            ${CMAKE_CURRENT_BINARY_DIR}/cfg/n2/layout.json
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/dfgs.json.tmp
            ${CMAKE_CURRENT_BINARY_DIR}/cfg/n0/dfgs.json
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/dfgs.json.tmp
            ${CMAKE_CURRENT_BINARY_DIR}/cfg/n1/dfgs.json
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/dfgs.json.tmp
            ${CMAKE_CURRENT_BINARY_DIR}/cfg/n2/dfgs.json
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/udl_dlls.cfg.tmp
            ${CMAKE_CURRENT_BINARY_DIR}/cfg/n0/udl_dlls.cfg
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/udl_dlls.cfg.tmp
            ${CMAKE_CURRENT_BINARY_DIR}/cfg/n1/udl_dlls.cfg
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cfg/udl_dlls.cfg.tmp
            ${CMAKE_CURRENT_BINARY_DIR}/cfg/n2/udl_dlls.cfg
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/cfg/layout.json.tmp
                ${CMAKE_CURRENT_SOURCE_DIR}/cfg/dfgs.json.tmp
                ${CMAKE_CURRENT_SOURCE_DIR}/cfg/udl_dlls.cfg.tmp
                ${CMAKE_CURRENT_SOURCE_DIR}/api/Gateway.cs
                ${CMAKE_CURRENT_SOURCE_DIR}/api/ReflectionLogic.cs
        COMMENT "prepare csharp_udl configuration"
    )
    add_dependencies(csharp_udl cascade)
else()
    message(NOTICE ".NET is not found. C# support is disabled") 
endif()
