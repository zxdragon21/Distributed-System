/*************************************************************************
	> File Name: cache_server.h
	> Author: 
	> Mail: 
	> Created Time: Fri Oct  3 17:04:30 2025
 ************************************************************************/
#ifndef CACHE_SERVER_H
#define CACHE_SERVER_H
#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <atomic>
#include <csignal>
#include <nlohmann/json.hpp>

// Workflow 头文件
#include <workflow/WFFacilities.h>
#include <workflow/WFHttpServer.h>
#include <workflow/HttpUtil.h>
#include <workflow/WFServer.h>
#include <workflow/WFGlobal.h>

#include "cache.h"
#include "node_manager.h"
#include "rpc_protocol.h"
#include "gossip_protocol.h"

// 全局信号处理器
static WFFacilities::WaitGroup wait_group(1);

void sighandler(int signum) {
    wait_group.done();
}

class CacheServer {
public:
    CacheServer(const std::string& node_id, const std::string& port, int total_nodes = 3) 
        : node_id_(node_id), 
          port_(port), 
          // RPC端口计算：9527->27080, 9528->27081, 9529->27082
          rpc_port_(std::to_string(27080 + (std::stoi(port) - 9527))),
          gossip_protocol_(std::make_shared<GossipProtocol>(node_id, total_nodes)),
          cache_(gossip_protocol_),
          node_manager_(total_nodes),
          http_server_(nullptr),
          rpc_server_(nullptr) {
        
        // 设置信号处理
        signal(SIGINT, sighandler);
        signal(SIGTERM, sighandler);
        
        // 设置gossip协议到节点管理器
        node_manager_.setGossipProtocol(gossip_protocol_);
    }
    
    ~CacheServer() {
        stop();
    }
    
    void run() {
        try {
            // 启动HTTP服务器
            startHttpServer();
            
            // 启动RPC服务器
            startRpcServer();
            
            // 等待信号退出
            wait_group.wait();
            
        } catch (const std::exception& e) {
            // 静默处理异常
        }
        
        // 停止服务器
        stop();
    }
    
    void stop() {
        if (http_server_) {
            http_server_->stop();
            http_server_ = nullptr;
        }
        
        if (rpc_server_) {
            rpc_server_->stop();
            rpc_server_ = nullptr;
        }
        
        // 停止gossip协议
        if (gossip_protocol_) {
            gossip_protocol_->stop();
        }
    }
    
private:
    void startHttpServer() {
        // 创建HTTP服务器实例
        http_server_ = new WFHttpServer([this](WFHttpTask *task) {
            this->registerHttpRoutes(task);
        });
        
        // 启动服务器
        if (http_server_->start(std::stoi(port_)) != 0) {
            throw std::runtime_error("Failed to start HTTP server");
        }
    }
    
    void startRpcServer() {
        // 创建RPC服务器实例
        rpc_server_ = new WFHttpServer([this](WFHttpTask *task) {
            this->handleRpcRequest(task);
        });
        
        // 启动服务器
        if (rpc_server_->start(std::stoi(rpc_port_)) != 0) {
            throw std::runtime_error("Failed to start RPC server");
        }
    }
    
    void registerHttpRoutes(WFHttpTask *task) {
        protocol::HttpRequest *req = task->get_req();
        protocol::HttpResponse *resp = task->get_resp();
        std::string method = req->get_method();
        std::string path = req->get_request_uri();
        
        // 测试路由 - 保留但移除日志
        if (method == "GET" && path == "/test") {
            resp->set_status_code("200");
            resp->append_output_body("Test OK from " + node_id_);
            return;
        }
        
        // 健康检查路由 - 保留但移除日志
        if (method == "GET" && path == "/health") {
            nlohmann::json response_json = {
                {"node", node_id_},
                {"status", "healthy"},
                {"http_port", port_},
                {"rpc_port", rpc_port_}
            };
            resp->set_status_code("200");
            resp->add_header_pair("Content-Type", "application/json");
            resp->append_output_body(response_json.dump());
            return;
        }
        
        // 写入/更新缓存
        if (method == "POST" && path == "/") {
            // 获取请求体
            const void *body;
            size_t body_len;
            req->get_parsed_body(&body, &body_len);
            std::string request_body(static_cast<const char*>(body), body_len);
            
            try {
                auto json = nlohmann::json::parse(request_body);
                
                if (json.size() != 1) {
                    resp->set_status_code("400");
                    resp->append_output_body("Only one key-value pair allowed");
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
                
                // 快速响应，使用预定义的成功响应字符串
                const char* success_response = "{\"status\":\"success\"}";
                resp->set_status_code("200");
                resp->add_header_pair("Content-Type", "application/json");
                resp->append_output_body(success_response);
                
            } catch (...) {
                resp->set_status_code("400");
                resp->append_output_body("Invalid JSON");
            }
            return;
        }
        
        // 读取缓存
        if (method == "GET" && path.size() > 1) {
            
            // 从URL路径获取key
            std::string key = path.substr(1);  // 去掉开头的'/'
            
            // 获取目标节点ID
            std::string target_node_id = node_manager_.getTargetNodeId(key);
            
            try {
                if (target_node_id == node_id_) {
                    // 本地查询
                    std::string value;
                    if (cache_.get(key, value)) {
                        try {
                            // 使用nlohmann::json来正确构造响应，避免字符串拼接错误
                            nlohmann::json response_json;
                            // 解析value为JSON对象
                            response_json[key] = nlohmann::json::parse(value);
                            resp->set_status_code("200");
                            resp->add_header_pair("Content-Type", "application/json");
                            resp->append_output_body(response_json.dump());
                        } catch (...) {
                            resp->set_status_code("500");
                            resp->append_output_body("Internal server error");
                        }
                    } else {
                        resp->set_status_code("404");
                    }
                } else {
                    // 使用NodeManager的RPC客户端发送GET请求
                    std::string rpc_response = node_manager_.sendGetRequest(target_node_id, key);
                    
                    try {
                        auto response_json = nlohmann::json::parse(rpc_response);
                        if (response_json.contains("error")) {
                            if (response_json["error"] == "not_found") {
                                resp->set_status_code("404");
                            } else {
                                resp->set_status_code("500");
                                resp->append_output_body("Internal server error");
                            }
                        } else if (response_json.contains(key)) {
                            resp->set_status_code("200");
                            resp->add_header_pair("Content-Type", "application/json");
                            resp->append_output_body(response_json.dump());
                        }
                    } catch (...) {
                        resp->set_status_code("500");
                        resp->append_output_body("Internal server error");
                    }
                }
            } catch (...) {
                resp->set_status_code("500");
                resp->append_output_body("Internal server error");
            }
            
            return;
        }
        
        // 删除缓存
        if (method == "DELETE" && path.size() > 1) {
            
            // 从URL路径获取key
            std::string key = path.substr(1);  // 去掉开头的'/'
            
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
                
                resp->set_status_code("200");
                resp->append_output_body(std::to_string(count));
                return; // 确保这里有return，避免继续执行到未匹配路由
                
            } catch (...) {
                resp->set_status_code("500");
                resp->append_output_body("Internal server error");
                return;
            }
        }
        
        // 未匹配的路由
        resp->set_status_code("404");
        resp->append_output_body("Not Found");
    }
    
    void handleRpcRequest(WFHttpTask *task) {
        protocol::HttpRequest *req = task->get_req();
        protocol::HttpResponse *resp = task->get_resp();
        
        // 获取请求体
        const void *body;
        size_t body_len;
        req->get_parsed_body(&body, &body_len);
        std::string request_body(static_cast<const char*>(body), body_len);
        
        try {
            auto json = nlohmann::json::parse(request_body);
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
                resp->set_status_code("200");
                resp->add_header_pair("Content-Type", "application/json");
                resp->append_output_body("{\"status\":\"success\"}");
                return;
            } else if (rpc_type == "DELETE") {
                std::string key = json["key"];
                // 不使用乐观锁删除
                int count = cache_.remove(key);
                response_json["count"] = count;
            } else {
                response_json["error"] = "unknown_operation";
            }
            
            resp->set_status_code("200");
            resp->add_header_pair("Content-Type", "application/json");
            resp->append_output_body(response_json.dump());
            
        } catch (...) {
            resp->set_status_code("400");
            resp->append_output_body("Invalid request");
        }
    }
    
    std::string node_id_;
    std::string port_;
    std::string rpc_port_;
    WFHttpServer *http_server_;
    WFHttpServer *rpc_server_;
    NodeManager node_manager_;
    std::shared_ptr<GossipProtocol> gossip_protocol_;
    Cache cache_; 
    std::atomic<uint32_t> next_request_id_{0};
};

#endif