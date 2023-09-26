add_library(utils OBJECT utils.cpp)
target_include_directories(utils PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_DIR}>
)
target_link_libraries(utils derecho::derecho)
