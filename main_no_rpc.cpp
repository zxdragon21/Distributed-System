/*************************************************************************
	> File Name: main_no_rpc.cpp
	> Author: 
	> Mail: 
	> Created Time: Fri Oct  3 18:12:39 2025
 ************************************************************************/
#include <iostream>
#include "cache_server_no_rpc.h"

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <node_id> <port>" << std::endl;
        return 1;
    }
    
    std::string node_id = argv[1];
    std::string port = argv[2];
    
    try {
        CacheServerNoRpc server(node_id, port);
        server.run();
    } catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
