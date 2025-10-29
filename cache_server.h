/*************************************************************************
	> File Name: cache_server.h
	> Author: 
	> Mail: 
	> Created Time: Fri Oct  3 17:04:30 2025
 ************************************************************************/
#ifndef CACHE_SERVER_H
#define CACHE_SERVER_H
#include <string>
#include <memory>
#include <thread>
#include <atomic>
#include <csignal>
#include <nlohmann/json.hpp>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>

// 替换为libmicrohttpd
#include "http_server_libmicrohttpd.h"

#include "cache.h"
#include "node_manager.h"
#include "rpc_protocol.h"


class CacheServer {
private:
    bool running_ = false;
    
public:
    // 构造函数
    CacheServer(const std::string& node_id, const std::string& port, const std::string& rpc_port, int worker_threads = 16)
        : node_id_(node_id), 
          port_(port), 
          rpc_port_(rpc_port),
          cache_(),
          node_manager_(3),
          http_server_(nullptr),
          rpc_server_(nullptr) {
        
        // 节点管理器初始化完成
    }
    
    ~CacheServer() {
        stop();
    }
    
    // 线程池已移除，不再需要初始化线程池
    void initializeThreadPool(int num_threads = 16) {
        // 不再需要创建线程池，所有请求直接处理
    }
    
    void run() {
        running_ = true;
        
        try {
            // 启动HTTP服务器
            startHttpServer();
            
            // 启动RPC服务器
            startRpcServer();
            
        } catch (const std::exception& e) {
            // 静默处理异常
        }
    }
    
    void stop() {
        running_ = false;
        
        if (http_server_) {
            http_server_->stop();
            http_server_ = nullptr;
        }
        
        if (rpc_server_) {
            rpc_server_->stop();
            rpc_server_ = nullptr;
        }
    }
    
    bool isRunning() const {
        return running_;
    }
    
private:
    void startHttpServer() {
        // 创建libmicrohttpd服务器实例，所有请求都直接处理以提高性能
        http_server_ = new HttpServerLibmicrohttpd(
            std::stoi(port_), 
            [this](const std::string& url, const std::string& method, const std::string& body, const std::string& version, std::string& response, int& status_code) {
                // 直接处理所有请求，移除不必要的异步处理
                this->handleHttpRequest(url, method, body, response, status_code);
            }
        );
        
        // 启动服务器
        if (!http_server_->start()) {
            throw std::runtime_error("Failed to start HTTP server");
        }
    }
    
    void startRpcServer() {
        // 创建libmicrohttpd RPC服务器实例，直接处理RPC请求以提高性能
        rpc_server_ = new HttpServerLibmicrohttpd(
            std::stoi(rpc_port_),
            [this](const std::string& url, const std::string& method, const std::string& body, const std::string& version, std::string& response, int& status_code) {
                // 直接处理RPC请求，移除不必要的异步处理
                this->handleRpcRequestLibmicrohttpd(body, response, status_code);
            }
        );
        
        // 启动服务器
        if (!rpc_server_->start()) {
            throw std::runtime_error("Failed to start RPC server");
        }
    }
    
    void handleHttpRequest(const std::string& url, const std::string& method, const std::string& body, std::string& response, int& status_code) {
        // 测试路由
        if (method == "GET" && url == "/test") {
            response = "Test OK from " + node_id_;
            status_code = 200;
            return;
        }
        
        // 健康检查路由
        if (method == "GET" && url == "/health") {
            nlohmann::json response_json = {
                {"node", node_id_},
                {"status", "healthy"},
                {"http_port", port_},
                {"rpc_port", rpc_port_}
            };
            response = response_json.dump();
            status_code = 200;
            return;
        }
        
        // 写入/更新缓存
        if (method == "POST" && url == "/") {
            try {
                auto json = nlohmann::json::parse(body);
                
                if (json.size() != 1) {
                    response = "Only one key-value pair allowed";
                    status_code = 400;
                    return;
                }
                
                for (auto it = json.begin(); it != json.end(); ++it) {
                    std::string key = it.key();
                    std::string value = it.value().dump();
                    
                    // 获取目标节点ID
                    std::string target_node_id = node_manager_.getTargetNodeId(key);
                    
                    if (target_node_id == node_id_) {
                        // 本地存储，忽略版本号返回值
                        cache_.set(key, value);
                    } else {
                        // 使用NodeManager的RPC客户端发送请求
                        std::string rpc_response = node_manager_.sendSetRequest(target_node_id, key, value);
                        
                        try {
                            auto response_json = nlohmann::json::parse(rpc_response);
                            if (response_json.contains("error")) {
                                // 降级处理：本地存储，忽略版本号返回值
                                cache_.set(key, value);
                            }
                        } catch (...) {
                            // 降级处理：本地存储，忽略版本号返回值
                            cache_.set(key, value);
                        }
                    }
                }
                
                // 快速响应
                response = "{\"status\":\"success\"}";
                status_code = 200;
                
            } catch (...) {
                response = "Invalid JSON";
                status_code = 400;
            }
            return;
        }
        
        // 读取缓存
        if (method == "GET" && url.size() > 1) {
            
            // 从URL路径获取key
            std::string key = url.substr(1);  // 去掉开头的'/'
            
            // 获取目标节点ID
            std::string target_node_id = node_manager_.getTargetNodeId(key);
            
            try {
                if (target_node_id == node_id_) {
                    // 本地查询
                    std::string value;
                    if (cache_.get(key, value)) {
                        try {
                            // 使用nlohmann::json来正确构造响应
                            nlohmann::json response_json;
                            // 解析value为JSON对象
                            response_json[key] = nlohmann::json::parse(value);
                            response = response_json.dump();
                            status_code = 200;
                        } catch (...) {
                            response = "Internal server error";
                            status_code = 500;
                        }
                    } else {
                        response = "Not found";
                        status_code = 404;
                    }
                } else {
                    // 使用NodeManager的RPC客户端发送GET请求
                    std::string rpc_response = node_manager_.sendGetRequest(target_node_id, key);
                    
                    try {
                        auto response_json = nlohmann::json::parse(rpc_response);
                        if (response_json.contains("error")) {
                            if (response_json["error"] == "not_found") {
                                response = "Not found";
                                status_code = 404;
                            } else {
                                response = "Internal server error";
                                status_code = 500;
                            }
                        } else if (response_json.contains(key)) {
                            response = response_json.dump();
                            status_code = 200;
                        }
                    } catch (...) {
                        response = "Internal server error";
                        status_code = 500;
                    }
                }
            } catch (...) {
                response = "Internal server error";
                status_code = 500;
            }
            
            return;
        }
        
        // 删除缓存
        if (method == "DELETE" && url.size() > 1) {
            
            // 从URL路径获取key
            std::string key = url.substr(1);  // 去掉开头的'/'
            
            // 获取目标节点ID
            std::string target_node_id = node_manager_.getTargetNodeId(key);
            
            int count = 0;
            
            try {
                if (target_node_id == node_id_) {
                    // 本地删除，不使用乐观锁
                    // 确保先检查key是否存在，再执行删除
                    std::string dummy;
                    if (cache_.get(key, dummy)) {
                        count = cache_.remove(key);
                    } else {
                        count = 0; // 键不存在，删除失败
                    }
                } else {
                    // 使用NodeManager的RPC客户端发送DELETE请求
                    std::string rpc_response = node_manager_.sendDeleteRequest(target_node_id, key);
                    
                    try {
                        auto response_json = nlohmann::json::parse(rpc_response);
                        if (response_json.contains("count")) {
                            count = response_json["count"];
                        } else {
                            // 降级处理：尝试本地删除，不使用乐观锁
                            count = cache_.remove(key);
                        }
                    } catch (...) {
                        // 降级处理：尝试本地删除，不使用乐观锁
                        count = cache_.remove(key);
                    }
                }
                
                response = std::to_string(count);
                status_code = 200;
                return; // 确保这里有return，避免继续执行到未匹配路由
                
            } catch (...) {
                response = "Internal server error";
                status_code = 500;
                return;
            }
        }
        
        // 未匹配的路由
        response = "Not Found";
        status_code = 404;
    }
    
    void handleRpcRequestLibmicrohttpd(const std::string& body, std::string& response, int& status_code) {
        try {
            auto json = nlohmann::json::parse(body);
            std::string rpc_type = json.value("__rpc_type", "");
            
            nlohmann::json response_json;
            
            if (rpc_type == "GET") {
                std::string key = json["key"];
                std::string value;
                if (cache_.get(key, value)) {
                    try {
                        response_json[key] = nlohmann::json::parse(value);
                    } catch (...) {
                        response_json["error"] = "invalid_value";
                    }
                } else {
                    response_json["error"] = "not_found";
                }
                response = response_json.dump();
                status_code = 200;
            } else if (rpc_type == "SET") {
                for (auto it = json.begin(); it != json.end(); ++it) {
                    if (it.key() != "__rpc_type") {
                        std::string key = it.key();
                        std::string value = it.value().dump();
                        // 忽略版本号返回值
                        cache_.set(key, value);
                    }
                }
                // 快速响应
                response = "{\"status\":\"success\"}";
                status_code = 200;
            } else if (rpc_type == "DELETE") {
                std::string key = json["key"];
                // 不使用乐观锁删除
                int count = cache_.remove(key);
                response_json["count"] = count;
                response = response_json.dump();
                status_code = 200;
            } else {
                response_json["error"] = "unknown_operation";
                response = response_json.dump();
                status_code = 200;
            }
            
        } catch (...) {
            response = "Invalid request";
            status_code = 400;
        }
    }
    
    std::string node_id_;
    std::string port_;
    std::string rpc_port_;
    HttpServerLibmicrohttpd *http_server_;
    HttpServerLibmicrohttpd *rpc_server_;
    NodeManager node_manager_;
    Cache cache_; 
    std::atomic<uint32_t> next_request_id_{0};
    
    // 响应缓冲区和锁（备用）
    std::mutex response_mutex_;
};

#endif