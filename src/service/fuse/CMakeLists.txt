cmake_minimum_required(VERSION 3.10.0)

set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

find_package(derecho CONFIG REQUIRED)

if (${HAS_FUSE})
    add_executable(fuse_client fuse_client.cpp)
    target_include_directories(fuse_client PRIVATE
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
    )
    target_link_libraries(fuse_client cascade readline fuse3)
    set_target_properties(fuse_client PROPERTIES OUTPUT_NAME cascade_fuse_client)

    add_custom_command(TARGET fuse_client POST_BUILD
        	COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/testfuse.py ${CMAKE_CURRENT_BINARY_DIR}/.
        	COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/util.py ${CMAKE_CURRENT_BINARY_DIR}/.
        	DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/testfuse.py
	        	${CMAKE_CURRENT_SOURCE_DIR}/util.py
    		)

    install(FILES testfuse.py util.py
            TYPE BIN
            PERMISSIONS OWNER_WRITE OWNER_READ GROUP_READ WORLD_READ OWNER_EXECUTE GROUP_EXECUTE WORLD_EXECUTE
	    )

endif()
