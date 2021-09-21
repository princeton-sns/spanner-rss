set(HEADER_LIST "${CMAKE_SOURCE_DIR}/include/rss/lib.h")

add_library(rss lib.cc ${HEADER_LIST})
target_include_directories(rss PUBLIC ../include)
