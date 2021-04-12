cmake_minimum_required(VERSION 3.12.4)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
project(cascade CXX)

add_subdirectory(unit_tests)
add_subdirectory(cascade_as_subgroup_classes)
add_subdirectory(data_path_logic)
