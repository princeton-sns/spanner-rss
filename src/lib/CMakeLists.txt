protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS latency-format.proto)

file(GLOB SourceFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(Common ${SourceFiles} ${PROTO_SRCS} ${PROTO_HDRS})

target_include_directories(Common PUBLIC ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})