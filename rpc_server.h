/*************************************************************************
	> File Name: rpc_server.h
	> Author: 
	> Mail: 
	> Created Time: Fri Oct  3 17:04:14 2025
 ************************************************************************/
// rpc_server.h
#ifndef RPC_SERVER_H
#define RPC_SERVER_H

#include <iostream>
#include <string>
#include <nlohmann/json.hpp>

// Workflow 头文件
#include <workflow/WFServer.h>
#include <workflow/WFHttpServer.h>
#include <workflow/HttpUtil.h>

#include "cache.h"
#include "rpc_protocol.h"

class RpcServer {
public:
    RpcServer(int port, Cache& cache) : port_(port), cache_(cache) {
        std::cout << "RpcServer constructor called for port: " << port << std::endl;
        
        // 创建Workflow HTTP服务器作为RPC服务器
        server_ = new WFServer<workflow::protocol::HttpReq, workflow::protocol::HttpResp>(
            port,
            [this](WFHttpTask *task) {
                this->handleRequest(task);
            }
        );
    }
    
    ~RpcServer() {
        if (server_) {
            server_->stop();
            delete server_;
        }
    }
    
    void start() {
        if (server_->start() == 0) {
            std::cout << "RpcServer successfully started on port: " << port_ << std::endl;
        } else {
            std::cerr << "RpcServer failed to start on port " << port_ << std::endl;
            throw std::runtime_error("Failed to start RPC server");
        }
    }
    
    void stop() {
        if (server_) {
            server_->stop();
        }
    }
    
private:
    void handleRequest(WFHttpTask *task) {
        workflow::protocol::HttpRequest *req = task->get_req();
        workflow::protocol::HttpResponse *resp = task->get_resp();
        
        // 获取请求体
        const void *body;
        size_t body_len;
        req->get_parsed_body(&body, &body_len);
        std::string request_body(static_cast<const char*>(body), body_len);
        
        std::cout << "RpcServer received request body: " << request_body << std::endl;
        
        try {
            auto json = nlohmann::json::parse(request_body);
            std::string rpc_type = json.value("__rpc_type", "");
            
            nlohmann::json response_json;
            
            // 根据RPC类型处理请求
            if (rpc_type == "GET") {
                handleGetRequest(json, response_json);
            } else if (rpc_type == "SET") {
                handleSetRequest(json, response_json);
            } else if (rpc_type == "DELETE") {
                handleDeleteRequest(json, response_json);
            } else {
                response_json["error"] = "unknown_operation";
            }
            
            // 设置响应
            resp->set_status_code(200);
            resp->add_header_pair("Content-Type", "application/json");
            resp->append_output_body(response_json.dump());
            
        } catch (std::exception& e) {
            std::cerr << "RPC handler exception: " << e.what() << std::endl;
            resp->set_status_code(400);
            resp->append_output_body("Invalid request");
        }
    }
    
    void handleGetRequest(const nlohmann::json& request, nlohmann::json& response) {
        std::string key = request["key"];
        std::string value;
        
        if (cache_.get(key, value)) {
            response[key] = nlohmann::json::parse(value);
        } else {
            response["error"] = "not_found";
        }
    }
    
    void handleSetRequest(const nlohmann::json& request, nlohmann::json& response) {
        for (auto it = request.begin(); it != request.end(); ++it) {
            if (it.key() != "__rpc_type") {
                std::string key = it.key();
                std::string value = it.value().dump();
                cache_.set(key, value);
            }
        }
        response["status"] = "success";
    }
    
    void handleDeleteRequest(const nlohmann::json& request, nlohmann::json& response) {
        std::string key = request["key"];
        int count = cache_.remove(key);
        response["count"] = count;
    }
    
    int port_;
    Cache& cache_;
    WFServer<workflow::protocol::HttpReq, workflow::protocol::HttpResp> *server_;
};

#endif