protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${CMAKE_SOURCE_DIR}/replication/common/request.proto vr-proto.proto)

file(GLOB SourceFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(ReplicationVR ${SourceFiles} ${PROTO_SRCS} ${PROTO_HDRS})
target_include_directories(ReplicationVR PUBLIC ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})
target_link_libraries(ReplicationVR ReplicationCommon)