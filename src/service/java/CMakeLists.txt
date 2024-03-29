find_package(Java QUIET)

if (NOT Java_FOUND)
    message(STATUS "Java not found - skipping building tests")
	return()
endif (NOT Java_FOUND)

# set(JAVA_AWT_LIBRARY NotNeeded)
# set(JAVA_JVM_LIBRARY NotNeeded)
# set(JAVA_AWT_INCLUDE_PATH NotNeeded)
find_package(JNI)

include(UseJava)

set(CMAKE_JAVA_COMPILE_FLAGS "-source" "11" "-target" "11" "-h" "${CMAKE_CURRENT_SOURCE_DIR}/jni" "-Xlint")

add_jar(cascade_jar
        io/cascade/Client.java
        io/cascade/Bundle.java
        io/cascade/QueryResults.java
        io/cascade/ServiceType.java
        io/cascade/ShardMemberSelectionPolicy.java
        io/cascade/ShardingPolicy.java
        io/cascade/CascadeObject.java
        io/cascade/CascadeObjectPoolMetadata.java
        io/cascade/stream/ShardSupplier.java
        io/cascade/stream/ShardTimeSupplier.java
        io/cascade/stream/VersionSupplier.java
        io/cascade/stream/SubgroupSupplier.java
    OUTPUT_NAME
        cascade
)

add_jar(client_test
        io/cascade/test/ClientTest.java
    ENTRY_POINT
		io/cascade/test/ClientTest
    OUTPUT_NAME
        client_test
    MANIFEST
        Manifest.txt
    INCLUDE_JARS
        cascade_jar
)

add_jar(perf_test
        io/cascade/test/PerfTestClient.java
    ENTRY_POINT
		io/cascade/test/PerfTestClient
    OUTPUT_NAME
        perf_test
    MANIFEST
        Manifest.txt
    INCLUDE_JARS
        cascade_jar
)

add_jar(stream_test
        io/cascade/test/StreamTest.java
    ENTRY_POINT
		io/cascade/test/StreamTest
    OUTPUT_NAME
        stream_test
    MANIFEST
        Manifest.txt
    INCLUDE_JARS
        cascade_jar
)

add_library(cascade_jni SHARED ${CMAKE_CURRENT_SOURCE_DIR}/jni/cascade_jni.cpp)
target_include_directories(cascade_jni PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${JAVA_INCLUDE_PATH}>
    $<BUILD_INTERFACE:${JAVA_INCLUDE_PATH2}>)
target_link_libraries(cascade_jni cascade)

install(TARGETS cascade_jni RUNTIME DESTINATION ${CMAKE_INSTALL_LIBDIR}/)
