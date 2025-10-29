/*************************************************************************
	> File Name: cache_server.h
	> Author: 
	> Mail: 
	> Created Time: Fri Oct  3 17:04:30 2025
 ************************************************************************/
#ifndef CACHE_SERVER_H
#define CACHE_SERVER_H
#include <boost/asio.hpp>
#include <crow.h>          // 主要的 Crow 头文件
#include <iostream>        // 用于调试输出
#include <string>          // 用于字符串处理
#include <memory>          // 如果需要智能指针
#include <thread>          // 用于多线程
#include <atomic>          // 用于原子操作
#include <csignal>         // 用于信号处理
#include <chrono>
#include "cache.h"
#include "node_manager.h"
#include "rpc_client.h"
#include "rpc_server.h"

class CacheServer {
public:
    CacheServer(const std::string& node_id, const std::string& port, int total_nodes = 3) 
        : node_id_(node_id), 
          port_(port), 
          // 修复RPC端口计算：9527->27080, 9528->27081, 9529->27082
          rpc_port_(std::to_string(27080 + (std::stoi(port) - 9527))),
          cache_(),
          node_manager_(total_nodes) {
        
        std::cout << "=== CacheServer Constructor ===" << std::endl;
        std::cout << "Node ID: " << node_id_ << std::endl;
        std::cout << "HTTP port: " << port_ << std::endl;
        std::cout << "RPC port: " << rpc_port_ << std::endl;
        
        // 设置日志级别
        app_.loglevel(crow::LogLevel::Warning);
        
        // 设置路由
        setupHttpRoutes();
        
        std::cout << "HTTP routes setup complete" << std::endl;
        std::cout << "=== CacheServer Constructor Complete ===" << std::endl;
    }
    
   void run() {
    std::cout << "=== Starting CacheServer Run ===" << std::endl;
    std::cout << "Starting cache server " << node_id_ 
              << " on HTTP port " << port_ 
              << " and RPC port " << rpc_port_ << std::endl;
    
    // 初始化 RPC 服务器 - 传入 cache_ 参数
    std::cout << "Initializing RPC server on port " << rpc_port_ << "..." << std::endl;
    boost::asio::io_context io_context;
    RpcServer rpc_server(io_context, std::stoi(rpc_port_), cache_);  // 添加 cache_ 参数
    
    // 启动 RPC 线程
    std::thread rpc_thread([&io_context, this]() {
        std::cout << "RPC server thread starting for node " << node_id_ << std::endl;
        try {
            io_context.run();
            std::cout << "RPC server thread finished for node " << node_id_ << std::endl;
        } catch (std::exception& e) {
            std::cerr << "RPC server exception in node " << node_id_ << ": " << e.what() << std::endl;
        }
    });
    
    // 给 RPC 服务器一点时间启动
    std::cout << "Waiting for RPC server to start..." << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    
    try {
        std::cout << "Starting HTTP server on port " << port_ << "..." << std::endl;
        app_.port(std::stoi(port_)).run();
    } catch (std::exception& e) {
        std::cerr << "HTTP server exception in node " << node_id_ << ": " << e.what() << std::endl;
    }
    
    // 停止 RPC 服务器
    std::cout << "Stopping RPC server..." << std::endl;
    io_context.stop();
    if (rpc_thread.joinable()) {
        rpc_thread.join();
    }
    
    std::cout << "CacheServer " << node_id_ << " stopped" << std::endl;
}
    
private:
    // 辅助方法获取 Cache 单例引用
      
    void setupHttpRoutes() {
        std::cout << "Setting up HTTP routes for node " << node_id_ << "..." << std::endl;
        
        // 测试路由
        CROW_ROUTE(app_, "/test")
        ([this]() {
            std::cout << "[" << node_id_ << "] GET /test" << std::endl;
            return "Test OK from " + node_id_;
        });
        
        // 健康检查路由
        CROW_ROUTE(app_, "/health")
        ([this]() {
            std::cout << "[" << node_id_ << "] GET /health" << std::endl;
            nlohmann::json response = {
                {"node", node_id_},
                {"status", "healthy"},
                {"http_port", port_},
                {"rpc_port", rpc_port_}
            };
            return crow::response(200, response.dump());
        });
        
        // 写入/更新缓存
        CROW_ROUTE(app_, "/").methods("POST"_method)
        ([this](const crow::request& req) {
            std::cout << "[" << node_id_ << "] POST / - Body: " << req.body << std::endl;
            try {
                auto json = nlohmann::json::parse(req.body);
                
                if (json.size() != 1) {
                    std::cout << "[" << node_id_ << "] Error: Only one key-value pair allowed" << std::endl;
                    return crow::response(400, "Only one key-value pair allowed");
                }
                
                for (auto it = json.begin(); it != json.end(); ++it) {
                    std::string key = it.key();
                    std::string value = it.value().dump();
                    
                    // 获取目标节点ID
                    std::string target_node_id = node_manager_.getTargetNodeId(key);
                    std::cout << "[" << node_id_ << "] Key '" << key << "' should be on node: " << target_node_id << std::endl;
                    
                    if (target_node_id == node_id_) {
                        // 本地存储
                        cache_.set(key, value);
                        std::cout << "[" << node_id_ << "] Stored locally: " << key << " = " << value << std::endl;
                    } else {
                        // 远程RPC调用
                        Node target_node = node_manager_.getNodeById(target_node_id);
                        std::cout << "[" << node_id_ << "] Making RPC SET call to " << target_node.id 
                                  << " (" << target_node.address << ":" << target_node.rpc_port << ")" << std::endl;
                        
                        try {
                            boost::asio::io_context io_context;
                            RpcClient client(io_context, target_node.address, target_node.rpc_port);
                            
                            nlohmann::json rpc_data;
                            rpc_data[key] = nlohmann::json::parse(value);
                            RpcMessage request = RpcMessage::createRequest(
                                RpcType::SET, 
                                next_request_id_++, 
                                rpc_data.dump()
                            );
                            
                            std::string response_body = client.call(request);
                            std::cout << "[" << node_id_ << "] RPC SET response: " << response_body << std::endl;
                            
                        } catch (std::exception& e) {
                            std::cout << "[" << node_id_ << "] RPC SET call failed: " << e.what() << std::endl;
                            // 降级处理：在本地存储
                            cache_.set(key, value);
                            std::cout << "[" << node_id_ << "] Fallback: stored locally" << std::endl;
                        }
                    }
                }
                
                nlohmann::json response = {{"status", "success"}};
                return crow::response(200, response.dump());
                
            } catch (std::exception& e) {
                std::cout << "[" << node_id_ << "] JSON parse error: " << e.what() << std::endl;
                return crow::response(400, "Invalid JSON");
            }
        });
        
        // 读取缓存
        CROW_ROUTE(app_, "/<string>").methods("GET"_method)
        ([this](const std::string& key) {
            std::cout << "[" << node_id_ << "] GET /" << key << std::endl;
            
            // 获取目标节点ID
            std::string target_node_id = node_manager_.getTargetNodeId(key);
            std::cout << "[" << node_id_ << "] Key '" << key << "' should be on node: " << target_node_id << std::endl;
            
            try {
                if (target_node_id == node_id_) {
                    // 本地查询
                    std::cout << "[" << node_id_ << "] Local query for key: " << key << std::endl;
                    std::string value;
                    if (cache_.get(key, value)) {
                        auto parsed_value = nlohmann::json::parse(value);
                        nlohmann::json response;
                        response[key] = parsed_value;
                        std::cout << "[" << node_id_ << "] Local query success: " << response.dump() << std::endl;
                        return crow::response(200, response.dump());
                    } else {
                        std::cout << "[" << node_id_ << "] Local query: key not found" << std::endl;
                        return crow::response(404);
                    }
                } else {
                    // 远程RPC调用
                    Node target_node = node_manager_.getNodeById(target_node_id);
                    std::cout << "[" << node_id_ << "] Making RPC GET call to " << target_node.id 
                              << " (" << target_node.address << ":" << target_node.rpc_port << ")" << std::endl;
                    
                    try {
                        boost::asio::io_context io_context;
                        RpcClient client(io_context, target_node.address, target_node.rpc_port);
                        
                        nlohmann::json rpc_data;
                        rpc_data["key"] = key;
                        RpcMessage request = RpcMessage::createRequest(
                            RpcType::GET, 
                            next_request_id_++, 
                            rpc_data.dump()
                        );
                        
                        std::string response_body = client.call(request);
                        std::cout << "[" << node_id_ << "] RPC GET response: " << response_body << std::endl;
                        
                        auto response_json = nlohmann::json::parse(response_body);
                        
                        if (response_json.contains("error")) {
                            std::cout << "[" << node_id_ << "] RPC GET: key not found on remote node" << std::endl;
                            return crow::response(404);
                        } else {
                            std::cout << "[" << node_id_ << "] RPC GET success" << std::endl;
                            return crow::response(200, response_body);
                        }
                        
                    } catch (std::exception& e) {
                        std::cout << "[" << node_id_ << "] RPC GET call failed: " << e.what() << std::endl;
                        return crow::response(500, "RPC call failed");
                    }
                }
            } catch (std::exception& e) {
                std::cout << "[" << node_id_ << "] Exception in GET: " << e.what() << std::endl;
                return crow::response(500, "Internal server error");
            }
        });
        
        // 删除缓存
        CROW_ROUTE(app_, "/<string>").methods("DELETE"_method)
        ([this](const std::string& key) {
            std::cout << "[" << node_id_ << "] DELETE /" << key << std::endl;
            
            // 获取目标节点ID
            std::string target_node_id = node_manager_.getTargetNodeId(key);
            std::cout << "[" << node_id_ << "] Key '" << key << "' should be on node: " << target_node_id << std::endl;
            
            int count = 0;
            
            try {
                if (target_node_id == node_id_) {
                    // 本地删除
                    count = cache_.remove(key);
                    std::cout << "[" << node_id_ << "] Local delete count: " << count << std::endl;
                } else {
                    // 远程RPC调用
                    Node target_node = node_manager_.getNodeById(target_node_id);
                    std::cout << "[" << node_id_ << "] Making RPC DELETE call to " << target_node.id 
                              << " (" << target_node.address << ":" << target_node.rpc_port << ")" << std::endl;
                    
                    try {
                        boost::asio::io_context io_context;
                        RpcClient client(io_context, target_node.address, target_node.rpc_port);
                        
                        nlohmann::json rpc_data;
                        rpc_data["key"] = key;
                        RpcMessage request = RpcMessage::createRequest(
                            RpcType::DELETE, 
                            next_request_id_++, 
                            rpc_data.dump()
                        );
                        
                        std::string response_body = client.call(request);
                        std::cout << "[" << node_id_ << "] RPC DELETE response: " << response_body << std::endl;
                        
                        auto response_json = nlohmann::json::parse(response_body);
                        count = response_json["count"];
                        
                    } catch (std::exception& e) {
                        std::cout << "[" << node_id_ << "] RPC DELETE call failed: " << e.what() << std::endl;
                        // 降级处理：尝试本地删除
                        count = cache_.remove(key);
                        std::cout << "[" << node_id_ << "] Fallback local delete count: " << count << std::endl;
                    }
                }
                
                return crow::response(200, std::to_string(count));
                
            } catch (std::exception& e) {
                std::cout << "[" << node_id_ << "] Exception in DELETE: " << e.what() << std::endl;
                return crow::response(500, "Internal server error");
            }
        });
        
        std::cout << "HTTP routes setup complete for node " << node_id_ << std::endl;
    }
    
    std::string node_id_;
    std::string port_;
    std::string rpc_port_;
    crow::SimpleApp app_;
    NodeManager node_manager_;
    Cache cache_; 
    std::atomic<uint32_t> next_request_id_{0};
};

#endif