protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${CMAKE_SOURCE_DIR}/replication/common/request.proto ir-proto.proto)

file(GLOB SourceFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(ReplicationIR ${SourceFiles} ${PROTO_SRCS} ${PROTO_HDRS})
target_include_directories(ReplicationIR PUBLIC ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})
target_link_libraries(ReplicationIR ReplicationCommon Common)