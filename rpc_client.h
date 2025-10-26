/*************************************************************************
	> File Name: rpc_client.h
	> Author: 
	> Mail: 
	> Created Time: Fri Oct  3 17:03:39 2025
 ************************************************************************/
// rpc_client.h
#ifndef RPC_CLIENT_H
#define RPC_CLIENT_H

#include <iostream>
#include <string>
#include <nlohmann/json.hpp>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <atomic>

// Workflow 头文件
#include <workflow/WFTaskFactory.h>
#include <workflow/WFHttpServer.h>
#include <workflow/HttpUtil.h>
#include <workflow/WFFacilities.h>

#include "rpc_protocol.h"

class RpcClient {
public:
    RpcClient(const std::string& host, int port) : host_(host), port_(port), request_count_(0) {
        // 移除不必要的日志输出
    }
    
    std::string call(const RpcMessage& request) {
        // 从RpcMessage中提取类型和内容
        std::string type = std::to_string(static_cast<int>(request.header.type));
        return sendRequest(type, request.body);
    }
    
    std::string sendRequest(const std::string& type, const std::string& body) {
        // 为避免死锁，简化RPC调用，使用较短的超时时间
        std::string url = "http://" + host_ + ":" + std::to_string(port_);
        
        // 创建一个新的WaitGroup，避免使用成员变量导致的潜在问题
        WFFacilities::WaitGroup wait_group(1);
        std::string response_body = "{\"error\":\"timeout\"}";
        
        try {
            WFHttpTask *task = WFTaskFactory::create_http_task(
                url, 
                500,   // 降低连接超时时间
                1000,  // 降低回复超时时间
                [&](WFHttpTask *task) {
                    if (task->get_state() == WFT_STATE_SUCCESS) {
                        protocol::HttpResponse *resp = task->get_resp();
                        const void *body;
                        size_t body_len;
                        if (resp->get_parsed_body(&body, &body_len) && body_len > 0) {
                            response_body = std::string(static_cast<const char*>(body), body_len);
                        }
                    } else {
                        response_body = "{\"error\":\"rpc_error\"}";
                    }
                    wait_group.done();
                }
            );
            
            protocol::HttpRequest *req = task->get_req();
            req->set_method("POST");
            req->add_header_pair("Content-Type", "application/json");
            
            // 简化请求体构造
            nlohmann::json json_body;
            if (!body.empty()) {
                try {
                    json_body = nlohmann::json::parse(body);
                } catch (...) {
                    // 如果解析失败，直接使用空对象
                }
            }
            json_body["__rpc_type"] = type;
            
            req->append_output_body(json_body.dump());
            task->start();
            
            // 使用非阻塞模式，设置非常短的超时时间避免死锁
            wait_group.wait(1500);  // 1.5秒超时
            
        } catch (std::exception& e) {
            response_body = "{\"error\":\"rpc_error\"}";
        }
        
        return response_body;
    }
    
    // 便捷方法
    std::string get(const std::string& key) {
        nlohmann::json request;
        request["key"] = key;
        return sendRequest("GET", request.dump());
    }
    
    std::string set(const std::string& key, const std::string& value) {
        nlohmann::json request;
        try {
            request[key] = nlohmann::json::parse(value);
        } catch (...) {
            // 如果value不是有效的JSON，直接作为字符串处理
            request[key] = value;
        }
        return sendRequest("SET", request.dump());
    }
    
    std::string remove(const std::string& key) {
        nlohmann::json request;
        request["key"] = key;
        return sendRequest("DELETE", request.dump());
    }
    
private:
    std::string host_;
    int port_;
    std::atomic<int> request_count_;  // 用于性能监控
};

#endif