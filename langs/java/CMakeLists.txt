cmake_minimum_required (VERSION 3.0)
project(cascade_java)

find_package(Java)

if (NOT Java_FOUND)
    message("Java not found - skipping building tests")
	return()
endif (NOT Java_FOUND)

find_package(JNI)

if (NOT JNI_FOUND)
	message("JNI not found - skipping building tests")
	return()
endif (NOT JNI_FOUND)


include(UseJava)

set(CMAKE_JAVA_COMPILE_FLAGS "-source" "11" "-target" "11" "-h" "${CMAKE_CURRENT_SOURCE_DIR}/cpp")
set(GOOGLE_JAVA_FORMAT_JAR "${CMAKE_CURRENT_SOURCE_DIR}/external/google-java-format-1.7-all-deps.jar")

set(CMAKE_CXX_FLAGS "-std=c++1z -Wall -g -fPIC -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -ggdb")


add_jar(cascadejar
        java/io/cascade/Client.java
        java/io/cascade/Bundle.java
        java/io/cascade/QueryResults.java
        java/io/cascade/ServiceType.java
        java/io/cascade/ShardMemberSelectionPolicy.java
    OUTPUT_NAME
        cascadejava
)

add_jar(clienttest
        java/io/cascade/test/ClientTest.java
    ENTRY_POINT
		io/cascade/test/ClientTest
    OUTPUT_NAME
        client_test
    MANIFEST
        Manifest.txt
    INCLUDE_JARS
        cascadejar
)

add_jar(perftest
        java/io/cascade/test/PerfTestClient.java
    ENTRY_POINT
		io/cascade/test/PerfTestClient
    OUTPUT_NAME
        perf_test
    MANIFEST
        Manifest.txt
    INCLUDE_JARS
        cascadejar
)

message("cmake source dirs")
message(${CMAKE_SOURCE_DIR})
message(${JAVA_INCLUDE_PATH})
message(${JAVA_INCLUDE_PATH2})
message(${CMAKE_SOURCE_DIR}/include)

include_directories(${JAVA_INCLUDE_PATH}
                    ${JAVA_INCLUDE_PATH2}
		    ${CMAKE_BINARY_DIR}/include
                    ${CMAKE_SOURCE_DIR}/include
                    ${CMAKE_INSTALL_PREFIX}/include)
add_library(cascadejavacpp SHARED ${CMAKE_CURRENT_SOURCE_DIR}/cpp/cascade_java.cpp)
target_link_libraries(cascadejavacpp cascade)

install(TARGETS cascadejavacpp DESTINATION ${CMAKE_INSTALL_PREFIX}/lib/)
