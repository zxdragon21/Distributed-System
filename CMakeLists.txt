# CMakeLists.txt
cmake_minimum_required(VERSION 3.12)
project(SDCS)

set(CMAKE_CXX_STANDARD 17)

# 尝试查找Workflow库
find_package(workflow REQUIRED)

# 如果没有找到，尝试使用pkg-config
if(NOT workflow_FOUND)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(WORKFLOW REQUIRED libworkflow)
endif()

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

target_include_directories(sdcs PRIVATE ${CMAKE_SOURCE_DIR})

# 链接库
if(workflow_FOUND)
    target_link_libraries(sdcs workflow pthread)
else()
    target_include_directories(sdcs PRIVATE ${WORKFLOW_INCLUDE_DIRS})
    target_link_libraries(sdcs ${WORKFLOW_LIBRARIES} pthread)
endif()
