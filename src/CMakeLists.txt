cmake_minimum_required(VERSION 3.10)

# set the project name and version
project(TAPIR VERSION 1.0)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake")

# add_compile_options(-Wall -Wextra -pedantic)

# require protobuf
# set(Protobuf_DEBUG 1)
find_package(Protobuf REQUIRED)
link_libraries(${PROTOBUF_LIBRARIES})

# require openssl
find_package(OpenSSL REQUIRED)
link_libraries(${OPENSSL_LIBRARIES})

# require libevent
find_package(Libevent REQUIRED)
include_directories(${LIBEVENT_INCLUDE_DIR})
link_libraries(${LIBEVENT_LIB})

# require gflags
add_subdirectory(${CMAKE_SOURCE_DIR}/third_party/gflags)
link_libraries(gflags::gflags)

# add third-party JSON library
include_directories(${CMAKE_SOURCE_DIR}/third_party/json/single_include)

# add the common library
add_subdirectory(lib)

# add the rss library
add_subdirectory(rss)

# add the replication library
add_subdirectory(replication)

# add store executables
add_subdirectory(store)
