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

// 使用libcurl代替Workflow进行HTTP客户端请求
#include <curl/curl.h>

#include "rpc_protocol.h"

// curl回调函数
static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* response) {
    size_t total_size = size * nmemb;
    response->append(static_cast<char*>(contents), total_size);
    return total_size;
}

class RpcClient {
public:
    RpcClient(const std::string& host, int port) : host_(host), port_(port), request_count_(0) {
        // 初始化libcurl
        curl_global_init(CURL_GLOBAL_DEFAULT);
    }
    
    ~RpcClient() {
        // 清理libcurl
        curl_global_cleanup();
    }
    
    std::string call(const RpcMessage& request) {
        // 从RpcMessage中提取类型和内容
        std::string type = std::to_string(static_cast<int>(request.header.type));
        return sendRequest(type, request.body);
    }
    
    std::string sendRequest(const std::string& type, const std::string& body) {
        // 为避免死锁，简化RPC调用，使用较短的超时时间
        std::string url = "http://" + host_ + ":" + std::to_string(port_);
        std::string response_body = "{\"error\":\"timeout\"}";
        
        try {
            CURL* curl = curl_easy_init();
            if (!curl) {
                return "{\"error\":\"curl_init_failed\"}";
            }
            
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
            std::string request_body = json_body.dump();
            
            // 设置curl选项，优化连接参数以提高性能
            curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
            curl_easy_setopt(curl, CURLOPT_POST, 1L);
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request_body.c_str());
            curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, request_body.length());
            
            // 进一步减少超时时间以提高响应速度
            curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 50L);  // 减少到50毫秒超时
            curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 20L);  // 减少到20毫秒连接超时
            
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_body);
            
            // 启用持久连接以提高性能
            curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);  // 启用TCP保活
            curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);  // 禁用Nagle算法
            curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0L);  // 允许连接重用
            curl_easy_setopt(curl, CURLOPT_MAXCONNECTS, 100L);  // 增加最大连接数
            curl_easy_setopt(curl, CURLOPT_DNS_CACHE_TIMEOUT, 3600L);  // 增加DNS缓存时间
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);  // 禁用SSL验证（如果不需要HTTPS）
            curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);  // 禁用信号处理以提高性能
            
            // 设置HTTP头
            struct curl_slist *headers = NULL;
            headers = curl_slist_append(headers, "Content-Type: application/json");
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
            
            // 执行请求
            CURLcode res = curl_easy_perform(curl);
            
            // 清理资源
            curl_slist_free_all(headers);
            curl_easy_cleanup(curl);
            
            if (res != CURLE_OK) {
                response_body = "{\"error\":\"rpc_error\"}";
            } else if (response_body.empty()) {
                response_body = "{\"error\":\"empty_response\"}";
            }
            
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
    
    // 直接发送原始RPC请求体，用于支持批量操作等复杂请求
    std::string sendRpcRequest(const std::string& request_body) {
        // 直接发送原始请求体，不进行额外的包装
        std::string url = "http://" + host_ + ":" + std::to_string(port_);
        std::string response_body = "{\"error\":\"timeout\"}";
        
        try {
            CURL* curl = curl_easy_init();
            if (!curl) {
                return "{\"error\":\"curl_init_failed\"}";
            }
            
            // 设置curl选项，使用与sendRequest相同的优化参数
            curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
            curl_easy_setopt(curl, CURLOPT_POST, 1L);
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request_body.c_str());
            curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, request_body.length());
            
            // 使用相同的优化参数
            curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 50L);
            curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 20L);
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_body);
            curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
            curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);
            curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0L);
            curl_easy_setopt(curl, CURLOPT_MAXCONNECTS, 100L);
            curl_easy_setopt(curl, CURLOPT_DNS_CACHE_TIMEOUT, 3600L);
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
            curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
            
            // 设置HTTP头
            struct curl_slist *headers = NULL;
            headers = curl_slist_append(headers, "Content-Type: application/json");
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
            
            // 执行请求
            CURLcode res = curl_easy_perform(curl);
            
            // 清理资源
            curl_slist_free_all(headers);
            curl_easy_cleanup(curl);
            
            if (res != CURLE_OK) {
                response_body = "{\"error\":\"rpc_error\"}";
            } else if (response_body.empty()) {
                response_body = "{\"error\":\"empty_response\"}";
            }
            
        } catch (std::exception& e) {
            response_body = "{\"error\":\"rpc_error\"}";
        }
        
        return response_body;
    }
    
private:
    std::string host_;
    int port_;
    std::atomic<int> request_count_;  // 用于性能监控
};

#endif