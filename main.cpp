/*************************************************************************
	> File Name: main.cpp
	> Author: 
	> Mail: 
	> Created Time: Fri Oct  3 17:04:58 2025
 ************************************************************************/
#include <iostream>
#include <string>
#include "cache_server.h"

void signalHandler(int signal) {
    std::cout << "Received signal: " << signal << std::endl;
    exit(signal);
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <node_index>" << std::endl;
        std::cerr << "Example: " << argv[0] << " 1" << std::endl;
        std::cerr << "  node_index: 节点索引 (1, 2, 3...)" << std::endl;
        return 1;
    }
    
    int node_index = std::stoi(argv[1]);
    
    // 验证节点索引
    if (node_index < 1) {
        std::cerr << "Error: node_index must be greater than 0" << std::endl;
        return 1;
    }
    
    std::string port = std::to_string(9526 + node_index);  // 1->9527, 2->9528, 3->9529
    std::string node_id = "node" + std::to_string(node_index);
    
    std::cout << "Starting SDCS server:" << std::endl;
    std::cout << "  Node ID: " << node_id << std::endl;
    std::cout << "  HTTP Port: " << port << std::endl;
    std::cout << "  Press Ctrl+C to stop the server" << std::endl;
    
    try {
        CacheServer server(node_id, port);
        std::cout << "CacheServer created successfully, starting run..." << std::endl;
        server.run();
    } catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}