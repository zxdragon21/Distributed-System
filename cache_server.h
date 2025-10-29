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
          rpc_server_(nullptr),
          stop_threads_(false) {
        
        // 节点管理器初始化完成
    }
    
    ~CacheServer() {
        stop();
    }
    
    // 初始化线程池
    void initializeThreadPool(int num_threads = 16) {
        // 创建指定数量的工作线程
        for (int i = 0; i < num_threads; ++i) {
            worker_threads_.emplace_back([this] {
                while (!stop_threads_) {
                    std::function<void()> task;
                    
                    {   // 加锁作用域
                        std::unique_lock<std::mutex> lock(queue_mutex_);
                        condition_.wait(lock, [this] { 
                            return stop_threads_ || !task_queue_.empty(); 
                        });
                        
                        if (stop_threads_ && task_queue_.empty())
                            return;
                        
                        task = std::move(task_queue_.front());
                        task_queue_.pop();
                    }
                    
                    // 执行任务
                    try {
                        task();
                    } catch (...) {
                        // 静默处理异常
                    }
                }
            });
        }
    }
    
    // 提交任务到线程池
    template<class F, class... Args>
    void submitTask(F&& f, Args&&... args) {
        auto task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
        
        {   // 加锁作用域
            std::unique_lock<std::mutex> lock(queue_mutex_);
            task_queue_.emplace(task);
        }
        
        condition_.notify_one();
    }
    
    void run() {
        running_ = true;
        
        try {
            // 初始化线程池
            initializeThreadPool();
            
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
        
        // 停止线程池
        stop_threads_ = true;
        condition_.notify_all();
        for (auto& thread : worker_threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
        worker_threads_.clear();
        
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
        // 创建libmicrohttpd服务器实例
        http_server_ = new HttpServerLibmicrohttpd(
            std::stoi(port_), 
            [this](const std::string& url, const std::string& method, const std::string& body, const std::string& version, std::string& response, int& status_code) {
                // 对于简单请求直接处理
                if ((method == "GET" && (url == "/test" || url == "/health"))) {
                    this->handleHttpRequest(url, method, body, response, status_code);
                } else {
                    // 对于写操作和复杂查询使用线程池异步处理
                    std::shared_ptr<std::string> response_ptr = std::make_shared<std::string>();
                    std::shared_ptr<int> status_ptr = std::make_shared<int>(200);
                    
                    // 创建临时缓冲区
                    *response_ptr = "{\"status\":\"processing\"}";
                    *status_ptr = 200;
                    
                    // 保存临时结果到输出参数
                    response = *response_ptr;
                    status_code = *status_ptr;
                    
                    // 使用线程池异步处理实际请求（无需返回结果）
                    this->submitTask([this, url, method, body]() {
                        std::string dummy_response;
                        int dummy_status;
                        this->handleHttpRequest(url, method, body, dummy_response, dummy_status);
                    });
                }
            }
        );
        
        // 启动服务器
        if (!http_server_->start()) {
            throw std::runtime_error("Failed to start HTTP server");
        }
    }
    
    void startRpcServer() {
        // 创建libmicrohttpd RPC服务器实例
        rpc_server_ = new HttpServerLibmicrohttpd(
            std::stoi(rpc_port_),
            [this](const std::string& url, const std::string& method, const std::string& body, const std::string& version, std::string& response, int& status_code) {
                // 使用线程池异步处理RPC请求
                std::shared_ptr<std::string> response_ptr = std::make_shared<std::string>();
                std::shared_ptr<int> status_ptr = std::make_shared<int>(200);
                
                // 创建临时缓冲区
                *response_ptr = "{\"status\":\"processing\"}";
                *status_ptr = 200;
                
                // 保存临时结果到输出参数
                response = *response_ptr;
                status_code = *status_ptr;
                
                // 异步处理实际的RPC请求
                this->submitTask([this, body, response_ptr, status_ptr]() {
                    this->handleRpcRequestLibmicrohttpd(body, *response_ptr, *status_ptr);
                });
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
    
    // 线程池用于异步请求处理
    std::vector<std::thread> worker_threads_;
    std::queue<std::function<void()>> task_queue_;
    std::mutex queue_mutex_;
    std::condition_variable condition_;
    std::atomic<bool> stop_threads_;
    
    // 响应缓冲区和锁
    std::mutex response_mutex_;
};

#endif