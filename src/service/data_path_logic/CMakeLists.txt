cmake_minimum_required(VERSION 3.10.0)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

CHECK_INCLUDE_FILES("mxnet-cpp/MxNetCpp.h" HAS_MXNET_CPP)

add_library(console_printer_dpl SHARED console_printer_dpl.cpp)
target_include_directories(console_printer_dpl PRIVATE
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)

if (HAS_MXNET_CPP AND OPENCV_CORE_FOUND)
    add_library(cnn_classifier_dpl SHARED cnn_classifier_dpl.cpp)
    target_include_directories(cnn_classifier_dpl PRIVATE
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
        $<BUILD_INTERFACE:${OpenCV_INCLUDE_DIRS}>
    )
    target_link_libraries(cnn_classifier_dpl mxnet cascade ${OpenCV_LIBS})
endif()
