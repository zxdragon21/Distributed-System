# CMakeLists.txt
cmake_minimum_required(VERSION 3.12)
project(SDCS)

set(CMAKE_CXX_STANDARD 17)

find_package(Boost REQUIRED COMPONENTS system thread)

add_executable(sdcs
    main.cpp
    cache.h
    rpc_protocol.h
    rpc_client.h
    rpc_server.h
    node_manager.h
    cache_server.h
)

target_include_directories(sdcs PRIVATE ${CMAKE_SOURCE_DIR})

target_link_libraries(sdcs
    Boost::system
    Boost::thread
    pthread
)
