file(GLOB SourceFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(StoreCommonBackend ${SourceFiles})
target_include_directories(StoreCommonBackend PUBLIC ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})