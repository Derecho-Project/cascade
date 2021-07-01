cmake_minimum_required(VERSION 3.12.4)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(cascade CXX)

include(CheckIncludeFileCXX)
find_library(TENSORFLOW_LIB_FOUND tensorflow)
CHECK_INCLUDE_FILE_CXX("cppflow/cppflow.h" CPPFLOW_FOUND)
find_library(ANN_LIB_FOUND ANN)
find_package(Torch QUIET)

if (TENSORFLOW_LIB_FOUND AND CPPFLOW_FOUND AND OPENCV_CORE_FOUND AND ANN_LIB_FOUND AND TORCH_FOUND)
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
    target_link_libraries(filter_udl cascade ${OpenCV_LIBS} ${TENSORFLOW_LIB_FOUND})
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
    target_link_libraries(infer_udl cascade ${TENSORFLOW_LIB_FOUND} ${TORCH_LIBRARIES} ${OpenCV_LIBS} ANN)
    if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/bcs-inference-model.tar.gz" AND
        EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/cow-id-model.tar.gz")
        add_custom_command(TARGET infer_udl POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/bcs-inference-model.tar.gz
            ${CMAKE_CURRENT_BINARY_DIR}/bcs-inference-model.tar.gz
            COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/cow-id-model.tar.gz
            ${CMAKE_CURRENT_BINARY_DIR}/cow-id-model.tar.gz
            COMMENT "prepare infer_udl"
        )
    else ()
        message(WARNING "${CMAKE_CURRENT_SOURCE_DIR}/filter-model.tar.gz or ${CMAKE_CURRENT_SOURCE_DIR}/cow-id-model.tar.gz is not found. They are required to run the demo.")
    endif()

    # dairy farm client
    add_executable(dairy_farm_client client.cpp)
    target_include_directories(dairy_farm_client PRIVATE
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
        $<BUILD_INTERFACE:${OpenCV_INCLUDE_DIRS}>
    )
    target_link_libraries(dairy_farm_client cascade ${OpenCV_LIBS})
    add_custom_command(TARGET dairy_farm_client POST_BUILD
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
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/model
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n0/filter_model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/model
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n1/filter_model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/model
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n2/filter_model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/model
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n3/filter_model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/model
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n4/filter_model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/model_orig
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n0/infer_model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/model_orig
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n1/infer_model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/model_orig
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n2/infer_model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/model_orig
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n3/infer_model
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/model_orig
        ${CMAKE_CURRENT_BINARY_DIR}/dairy_farm_cfg/n4/infer_model
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
    message (NOTICE "Skipping diary farm demo... to enable it, please install the following packages")
    if (NOT TENSORFLOW_LIB_FOUND)
        message (NOTICE "* tensorflow c library")
    endif()
    if (NOT CPPFLOW_FOUND)
        message (NOTICE "* cppflow headers")
    endif()
    if (NOT ANN_LIB_FOUND)
        message (NOTICE "* approximate nearest neighbour (ann 1.1.2) library with -fPIC")
    endif()
    if (NOT OPENCV_CORE_FOUND)
        message (NOTICE "* opencv core")
    endif()
    if (NOT TORCH_FOUND)
        message (NOTICE "* libtorch c binding")
    endif()
endif()
