# CMakeLists.txt
cmake_minimum_required(VERSION 3.12)
project(SDCS)

set(CMAKE_CXX_STANDARD 17)

# 添加编译选项
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -pthread")

# 查找依赖库
find_package(OpenSSL REQUIRED)
find_package(Boost REQUIRED COMPONENTS system thread)
find_package(CURL REQUIRED)

add_executable(sdcs
    main.cpp
    cache.h
    rpc_protocol.h
    rpc_client.h
    rpc_server.h
    node_manager.h
    cache_server.h
    gossip_protocol.h
)

target_include_directories(sdcs PRIVATE ${CMAKE_SOURCE_DIR} ${OPENSSL_INCLUDE_DIR} ${Boost_INCLUDE_DIRS} ${CURL_INCLUDE_DIRS})

# 链接库
target_link_libraries(sdcs ${OPENSSL_LIBRARIES} ${Boost_LIBRARIES} ${CURL_LIBRARIES} -lssl -lcrypto -lpthread -lmicrohttpd)
