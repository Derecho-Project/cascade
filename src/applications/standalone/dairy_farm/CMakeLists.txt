cmake_minimum_required(VERSION 3.12.4)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(cascade_dairy_farm CXX)

# Version
set(cascade_dairy_farm_VERSION 1.0rc0)
set(cascade_build_VERSION 1.0rc0)

# C++ STANDARD
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
if (${USE_VERBS_API})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_VERBS_API")
endif()
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -Wall --ggdb -gdwarf-3 -ftemplate-backtrace-limit=0 -DEVALUATION")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wall -DEVALUATION -fcompare-debug-second")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -Wall -ggdb -gdwarf-3")

find_package(cascade CONFIG REQUIRED)

include(CheckIncludeFileCXX)
find_package(CUDA QUIET)
if (CUDA_FOUND)
    set (ENABLE_GPU 1)
endif()
# set(CMAKE_REQUIRED_LIBRARIES mxnet)
# CHECK_INCLUDE_FILE_CXX("mxnet-cpp/MxNetCpp.h" HAS_MXNET_CPP)
# unset(CMAKE_REQUIRED_LIBRARIES)
find_package(OpenCV QUIET)
find_library(TENSORFLOW_LIB_FOUND tensorflow)
find_library(ANN_LIB_FOUND ANN)
find_package(Torch QUIET)

set (ENABLE_EVALUATION 1)
if (ENABLE_EVALUATION)
    find_package(rpclib 2.3.0 REQUIRED)
endif()

# configure header
# CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/config.h)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/config.h)

if (OPENCV_CORE_FOUND)
    # dairy farm client
    add_executable(dairy_farm_client client.cpp)
    target_include_directories(dairy_farm_client PRIVATE
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
        $<BUILD_INTERFACE:${OpenCV_INCLUDE_DIRS}>
    )
    target_link_libraries(dairy_farm_client derecho::cascade ${OpenCV_LIBS})
    # dairy farm perf test
    add_executable(dairy_farm_perf perf.cpp)
    target_include_directories(dairy_farm_perf PRIVATE
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
        $<BUILD_INTERFACE:${OpenCV_INCLUDE_DIRS}>
    )
    if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 9)
        if (ENABLE_EVALUATION)
            target_link_libraries(dairy_farm_perf derecho::cascade rpc ${OpenCV_LIBS})
        else()
            target_link_libraries(dairy_farm_perf derecho::cascade ${OpenCV_LIBS})
        endif()
    else ()
        if (ENABLE_EVALUATION)
            target_link_libraries(dairy_farm_perf derecho::cascade rpc stdc++fs ${OpenCV_LIBS})
        else()
            target_link_libraries(dairy_farm_perf derecho::cascade stdc++fs ${OpenCV_LIBS})
        endif()
    endif()
endif()

if (TENSORFLOW_LIB_FOUND AND OPENCV_CORE_FOUND AND ANN_LIB_FOUND AND TORCH_FOUND)
    # demo_common
    add_library(demo_common SHARED demo_common.cpp)
    target_include_directories(demo_common PRIVATE
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
        $<BUILD_INTERFACE:${OpenCV_INCLUDE_DIRS}>
        $<BUILD_INTERFACE:${TENSORFLOW_LIB_INCLUDE_DIRS}>
    )
    target_link_libraries(demo_common ${TENSORFLOW_LIB_FOUND})
    # filter_udl
    add_library(filter_udl SHARED filter_udl.cpp)
    target_include_directories(filter_udl PRIVATE
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
        $<BUILD_INTERFACE:${OpenCV_INCLUDE_DIRS}>
        $<BUILD_INTERFACE:${TENSORFLOW_LIB_INCLUDE_DIRS}>
    )
    target_link_libraries(filter_udl demo_common derecho::cascade ${OpenCV_LIBS} ${TENSORFLOW_LIB_FOUND})
    if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/filter-model.tar.gz")
        add_custom_command(TARGET filter_udl POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/filter-model.tar.gz
            ${CMAKE_CURRENT_BINARY_DIR}/filter-model.tar.gz
            COMMENT "prepare filter_udl"
        )
    else ()
        message(WARNING "${CMAKE_CURRENT_SOURCE_DIR}/filter-model.tar.gz is not found. It is required to run the demo.")
    endif()

    # infer_udl
    add_library(infer_udl SHARED infer_udl.cpp)
    target_include_directories(infer_udl PRIVATE
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
        $<BUILD_INTERFACE:${TORCH_INCLUDE_DIRS}>
        $<BUILD_INTERFACE:${TENSORFLOW_LIB_INCLUDE_DIRS}>
        $<BUILD_INTERFACE:${OpenCV_INCLUDE_DIRS}>
    )
    target_link_libraries(infer_udl demo_common derecho::cascade ${TENSORFLOW_LIB_FOUND} ${TORCH_LIBRARIES} ${OpenCV_LIBS} ANN)
    if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/bcs-model.tar.gz" AND
        EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/cow-id-model.tar.gz")
        add_custom_command(TARGET infer_udl POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/bcs-model.tar.gz
            ${CMAKE_CURRENT_BINARY_DIR}/bcs-model.tar.gz
            COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/cow-id-model.tar.gz
            ${CMAKE_CURRENT_BINARY_DIR}/cow-id-model.tar.gz
            COMMENT "prepare infer_udl"
        )
    else ()
        message(WARNING "${CMAKE_CURRENT_SOURCE_DIR}/filter-model.tar.gz or ${CMAKE_CURRENT_SOURCE_DIR}/cow-id-model.tar.gz is not found. They are required to run the demo.")
    endif()

    add_custom_command(TARGET infer_udl POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/video_receive.py
        ${CMAKE_CURRENT_BINARY_DIR}/video_receive.py
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/simulated_camera.py
        ${CMAKE_CURRENT_BINARY_DIR}/simulated_camera.py
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/extract_model.sh
        ${CMAKE_CURRENT_BINARY_DIR}/extract_model.sh
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/dairy_farm_cfg
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/run.sh.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n0/run.sh 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/run.sh.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n1/run.sh 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/run.sh.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n2/run.sh 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/run.sh.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n3/run.sh 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/run.sh.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n4/run.sh 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/dfgs.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n0/dfgs.json 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/dfgs.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n1/dfgs.json 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/dfgs.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n2/dfgs.json 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/dfgs.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n3/dfgs.json 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/dfgs.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n4/dfgs.json 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/layout.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n0/layout.json 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/layout.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n1/layout.json 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/layout.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n2/layout.json 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/layout.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n3/layout.json 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/layout.json.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n4/layout.json 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/udl_dlls.cfg.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n0/udl_dlls.cfg 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/udl_dlls.cfg.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n1/udl_dlls.cfg 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/udl_dlls.cfg.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n2/udl_dlls.cfg 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/udl_dlls.cfg.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n3/udl_dlls.cfg 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/udl_dlls.cfg.tmp
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n4/udl_dlls.cfg 
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/filter-model
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n0/filter-model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/filter-model
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n1/filter-model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/filter-model
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n2/filter-model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/filter-model
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n3/filter-model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/filter-model
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n4/filter-model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/bcs-model
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n0/bcs-model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/bcs-model
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n1/bcs-model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/bcs-model
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n2/bcs-model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/bcs-model
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n3/bcs-model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/bcs-model
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n4/bcs-model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cow-id-model
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n0/cow-id-model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cow-id-model
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n1/cow-id-model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cow-id-model
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n2/cow-id-model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cow-id-model
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n3/cow-id-model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cow-id-model
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n4/cow-id-model
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/dairy_farm_cfg/run.sh.tmp
                ${CMAKE_CURRENT_SOURCE_DIR}/dairy_farm_cfg/dfgs.json.tmp
                ${CMAKE_CURRENT_SOURCE_DIR}/dairy_farm_cfg/udl_dlls.cfg.tmp
                ${CMAKE_CURRENT_SOURCE_DIR}/simulated_camera.py
                ${CMAKE_CURRENT_SOURCE_DIR}/video_receive.py
        COMMENT "prepare dairy farm demo configuration"
    )
    
else()
    message (STATUS "Skipping diary farm demo... to enable it, please install the following packages")
    if (NOT TENSORFLOW_LIB_FOUND)
        message (STATUS "* tensorflow c library")
    endif()
    if (NOT ANN_LIB_FOUND)
        message (STATUS "* approximate nearest neighbour (ann 1.1.2) library with -fPIC")
    endif()
    if (NOT OPENCV_CORE_FOUND)
        message (STATUS "* opencv core")
    endif()
    if (NOT TORCH_FOUND)
        message (STATUS "* libtorch c binding")
    endif()
endif()

# storage_udl
add_library(storage_udl SHARED storage_udl.cpp)
target_include_directories(storage_udl PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
    $<BUILD_INTERFACE:${OpenCV_INCLUDE_DIRS}>
)
target_link_libraries(storage_udl derecho::cascade)
