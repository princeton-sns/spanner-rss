file(GLOB SourceFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(StoreCommonFrontend ${SourceFiles})
target_include_directories(StoreCommonFrontend PUBLIC ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})
target_link_libraries(StoreCommonFrontend StoreCommon rss)