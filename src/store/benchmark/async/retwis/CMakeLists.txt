file(GLOB SourceFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(BenchmarkRetwis ${SourceFiles})
target_include_directories(BenchmarkRetwis PUBLIC ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})
target_link_libraries(BenchmarkRetwis BenchmarkCommon StoreCommonFrontend)