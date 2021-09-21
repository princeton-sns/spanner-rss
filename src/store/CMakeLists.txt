add_subdirectory(common)
add_subdirectory(strongstore)
add_subdirectory(benchmark/async)

file(GLOB SourceFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_executable(server ${SourceFiles})
target_include_directories(server PUBLIC ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})
target_link_libraries(server StoreStrong StoreCommon StoreCommonBackend StoreCommonFrontend Common)