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


// 异步任务类型
typedef std::function<void()> AsyncTask;

class CacheServer {
private:
    bool running_ = false;
    
    // 线程池实现，用于异步处理请求
    class ThreadPool {
    public:
        ThreadPool(size_t num_threads) : stop_(false) {
            for (size_t i = 0; i < num_threads; ++i) {
                workers_.emplace_back([this] {
                    while (true) {
                        AsyncTask task;
                        {
                            std::unique_lock<std::mutex> lock(queue_mutex_);
                            this->condition_.wait(lock, [this] {
                                return this->stop_ || !this->tasks_.empty();
                            });
                            
                            if (this->stop_ && this->tasks_.empty()) {
                                return;
                            }
                            
                            task = std::move(this->tasks_.front());
                            this->tasks_.pop();
                        }
                        task();
                    }
                });
            }
        }
        
        ~ThreadPool() {
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                stop_ = true;
            }
            
            condition_.notify_all();
            
            for (std::thread &worker : workers_) {
                worker.join();
            }
        }
        
        // 添加任务到队列
        template<class F>
        void enqueue(F&& f) {
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                if (stop_) {
                    throw std::runtime_error("enqueue on stopped ThreadPool");
                }
                tasks_.emplace(std::forward<F>(f));
            }
            condition_.notify_one();
        }
        
    private:
        std::vector<std::thread> workers_;
        std::queue<AsyncTask> tasks_;
        std::mutex queue_mutex_;
        std::condition_variable condition_;
        bool stop_;
    };
    
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
          thread_pool_(std::make_unique<ThreadPool>(worker_threads)) {
        
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
                    std::string value;
                    
                    // 保存原始JSON表示，以便正确处理各种类型
                    value = it.value().dump();
                    
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
        
        // 读取缓存 - 高度优化版本，支持单个和批量请求
        if (method == "GET" && url.size() > 1) {
            // 批量GET请求处理
            if (url == "/batch") {
                try {
                    auto json = nlohmann::json::parse(body);
                    if (json.contains("keys") && json["keys"].is_array()) {
                        std::vector<std::string> keys;
                        for (const auto& key : json["keys"]) {
                            keys.push_back(key.get<std::string>());
                        }
                        
                        // 按节点ID分组，优化网络请求
                        std::unordered_map<std::string, std::vector<std::string>> keys_by_node;
                        std::unordered_map<std::string, std::string> local_results;
                        
                        // 快速处理本地键
                        for (const auto& key : keys) {
                            std::string target_node_id = node_manager_.getTargetNodeId(key);
                            if (target_node_id == node_id_) {
                                // 本地键，稍后批量获取
                            } else {
                                // 远程键，按节点分组
                                keys_by_node[target_node_id].push_back(key);
                            }
                        }
                        
                        // 批量获取本地缓存
                        cache_.batchGet(keys, local_results);
                        
                        // 异步处理远程节点的批量请求
                        std::unordered_map<std::string, std::unordered_map<std::string, std::string>> all_results;
                        std::mutex results_mutex;
                        std::condition_variable results_cv;
                        std::atomic<int> pending_requests(keys_by_node.size());
                        
                        for (const auto& [target_node, node_keys] : keys_by_node) {
                            thread_pool_->enqueue([this, target_node, node_keys, &all_results, &results_mutex, &pending_requests, &results_cv]() {
                                try {
                                    // 构造批量请求
                                    nlohmann::json request_json;
                                    request_json["__rpc_type"] = "BATCH_GET";
                                    request_json["keys"] = node_keys;
                                    
                                    // 发送RPC请求
                                    std::string rpc_response = node_manager_.sendRpcRequest(target_node, request_json.dump());
                                    
                                    // 解析响应
                                    auto response_json = nlohmann::json::parse(rpc_response);
                                    std::unordered_map<std::string, std::string> node_results;
                                    
                                    for (auto& [key, value] : response_json.items()) {
                                        if (key != "status" && key != "error") {
                                            node_results[key] = value.dump();
                                        }
                                    }
                                    
                                    // 保存结果
                                    {   
                                        std::lock_guard<std::mutex> lock(results_mutex);
                                        all_results[target_node] = node_results;
                                    }
                                } catch (...) {
                                    // 忽略异常
                                }
                                
                                // 减少待处理请求计数并通知主线程
                                if (--pending_requests == 0) {
                                    results_cv.notify_one();
                                }
                            });
                        }
                        
                        // 等待所有异步请求完成或超时
                        std::unique_lock<std::mutex> lock(results_mutex);
                        results_cv.wait_for(lock, std::chrono::milliseconds(500), [&pending_requests] { 
                            return pending_requests == 0; 
                        });
                        
                        // 合并所有结果
                        nlohmann::json response_json;
                        
                        // 添加本地结果
                        for (const auto& [key, value] : local_results) {
                            try {
                                response_json[key] = nlohmann::json::parse(value);
                            } catch (...) {
                                response_json[key] = value;
                            }
                        }
                        
                        // 添加远程结果
                        for (const auto& [node, node_results] : all_results) {
                            for (const auto& [key, value] : node_results) {
                                try {
                                    response_json[key] = nlohmann::json::parse(value);
                                } catch (...) {
                                    response_json[key] = value;
                                }
                            }
                        }
                        
                        response = response_json.dump();
                        status_code = 200;
                    } else {
                        response = "Invalid batch request format";
                        status_code = 400;
                    }
                } catch (...) {
                    response = "Invalid JSON";
                    status_code = 400;
                }
                return;
            } else {
                // 单个GET请求处理（保持原有逻辑）
                std::string key = url.substr(1);  // 去掉开头的'/'
                
                try {
                    // 优化：直接尝试本地查询，减少RPC调用
                    std::string value;
                    if (cache_.get(key, value)) {
                        // 本地存在，直接返回
                        response = value;
                        status_code = 200;
                        return;
                    }
                    
                    // 本地不存在，获取目标节点ID
                    std::string target_node_id = node_manager_.getTargetNodeId(key);
                    
                    // 只有当目标节点不是当前节点时才进行RPC调用
                    if (target_node_id != node_id_) {
                        // 从目标节点获取数据
                        std::string rpc_response = node_manager_.sendGetRequest(target_node_id, key);
                        
                        // 快速路径：直接返回响应内容
                        if (!rpc_response.empty()) {
                            // 优化：不再进行JSON解析，直接转发响应
                            response = rpc_response;
                            status_code = 200;
                        } else {
                            // 响应为空表示键不存在
                            response = "";
                            status_code = 404;
                        }
                    } else {
                        // 目标节点就是当前节点但缓存中不存在，直接返回404
                        response = "";
                        status_code = 404;
                    }
                } catch (...) {
                    // 发生异常，直接返回404
                    response = "";
                    status_code = 404;
                }
                
                return;
            }
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
                    // 优化：直接调用remove方法，与新的返回类型(bool)匹配
                    bool success = cache_.remove(key);
                    count = success ? 1 : 0;
                } else {
                    // 使用NodeManager的RPC客户端发送DELETE请求
                    std::string rpc_response = node_manager_.sendDeleteRequest(target_node_id, key);
                    
                    // 简化解析逻辑，快速提取count值
                    if (!rpc_response.empty() && rpc_response[0] == '{') {
                        try {
                            auto response_json = nlohmann::json::parse(rpc_response);
                            count = response_json.value("count", 0);
                        } catch (...) {
                            // 解析失败不再尝试本地删除，减少不必要操作
                            count = 0;
                        }
                    } else {
                        // 非JSON响应，默认为0
                        count = 0;
                    }
                }
                
                response = std::to_string(count);
                status_code = 200;
                return;
                
            } catch (...) {
                response = "Internal server error";
                status_code = 500;
                return;
            }
        }
        
        // 未匹配的路由
        response = "";
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
                    // 优化：直接返回value，不再进行JSON包装和解析
                    response = value;
                    status_code = 200;
                } else {
                    // 键不存在，返回空body的404
                    response = "";
                    status_code = 404;
                }
            } else if (rpc_type == "BATCH_GET") {
                // 批量GET请求处理
                if (json.contains("keys") && json["keys"].is_array()) {
                    std::vector<std::string> keys;
                    for (const auto& key : json["keys"]) {
                        keys.push_back(key.get<std::string>());
                    }
                    
                    // 使用批量获取方法提高性能
                    std::unordered_map<std::string, std::string> results;
                    cache_.batchGet(keys, results);
                    
                    // 构建响应
                    nlohmann::json batch_response;
                    for (const auto& [key, value] : results) {
                        try {
                            // 尝试解析原始JSON值以保持格式一致性
                            batch_response[key] = nlohmann::json::parse(value);
                        } catch (...) {
                            // 如果解析失败，直接使用字符串值
                            batch_response[key] = value;
                        }
                    }
                    
                    response = batch_response.dump();
                    status_code = 200;
                } else {
                    response_json["error"] = "invalid_batch_request";
                    response = response_json.dump();
                    status_code = 400;
                }
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
                // 不使用乐观锁删除，与新的返回类型(bool)匹配
                bool success = cache_.remove(key);
                response_json["count"] = success ? 1 : 0;
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
    
    // 线程池用于异步处理
    std::unique_ptr<ThreadPool> thread_pool_;
    
    // 响应缓冲区和锁（备用）
    std::mutex response_mutex_;
};

#endif