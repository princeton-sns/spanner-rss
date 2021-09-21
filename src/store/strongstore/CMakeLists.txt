protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${CMAKE_SOURCE_DIR}/store/common/common-proto.proto strong-proto.proto)

file(GLOB SourceFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(StoreStrong ${SourceFiles} ${PROTO_SRCS} ${PROTO_HDRS})
target_include_directories(StoreStrong PUBLIC ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})
target_link_libraries(StoreStrong ReplicationVR rss Common StoreCommon StoreCommonFrontend)