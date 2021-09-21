file(GLOB SourceFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(BenchmarkCommon ${SourceFiles})
target_include_directories(BenchmarkCommon PUBLIC ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})