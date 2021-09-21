add_subdirectory(common)
add_subdirectory(retwis)

file(GLOB SourceFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_executable(benchmark ${SourceFiles})
target_include_directories(benchmark PUBLIC ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})
target_link_libraries(benchmark BenchmarkCommon BenchmarkRetwis StoreStrong Common)