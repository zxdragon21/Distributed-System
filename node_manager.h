#ifndef NODE_MANAGER_H
#define NODE_MANAGER_H

#include <vector>
#include <string>
#include <memory>
#include <shared_mutex>
#include <functional>
#include <iostream>
#include <unordered_map>

#include "rpc_client.h"

struct Node {
    std::string id;
    std::string address;
    std::string http_port;
    std::string rpc_port;
};

class NodeManager {
public:
    NodeManager(int total_nodes = 3) : total_nodes_(total_nodes) {
        // 修复端口配置：节点1->9527, 节点2->9528, 节点3->9529
        for (int i = 1; i <= total_nodes; i++) {
            nodes_.push_back({
                "node" + std::to_string(i),
                "node" + std::to_string(i), 
                std::to_string(9526 + i),      // HTTP端口：9527, 9528, 9529
                std::to_string(27080 + i - 1)  // RPC端口：27080, 27081, 27082
            });
            
            // 预初始化所有RPC客户端，避免懒加载带来的延迟
            rpc_clients_[nodes_.back().id] = std::make_shared<RpcClient>(
                nodes_.back().address, 
                std::stoi(nodes_.back().rpc_port)
            );
        }
    }
    
    std::string getTargetNodeId(const std::string& key) {
        // 由于节点列表在初始化后不会改变，使用无锁访问以提高性能
        size_t hash = std::hash<std::string>{}(key);
        size_t index = hash % nodes_.size();
        return nodes_[index].id;
    }
    
    Node getNodeById(const std::string& node_id) {
        // 直接获取共享锁，简化锁管理
        std::shared_lock<std::shared_mutex> lock(mutex_);
        for (const auto& node : nodes_) {
            if (node.id == node_id) {
                return node;
            }
        }
        throw std::runtime_error("Node not found: " + node_id);
    }
    
    // 获取指定节点的RPC客户端（预初始化方式）
    std::shared_ptr<RpcClient> getRpcClientForNode(const std::string& node_id) {
        // 由于RPC客户端在初始化时已预创建，使用共享锁简化访问
        std::shared_lock<std::shared_mutex> lock(mutex_);
        
        auto it = rpc_clients_.find(node_id);
        if (it != rpc_clients_.end()) {
            return it->second;
        }
        
        // 应该不会到达这里，因为客户端已预初始化
        return nullptr;
    }
    
    // 向指定节点发送GET请求
    std::string sendGetRequest(const std::string& node_id, const std::string& key) {
        try {
            auto client = getRpcClientForNode(node_id);
            auto result = client->get(key);
            
            return result;
        } catch (const std::exception& e) {
            // std::cerr << "Error sending GET request to node " << node_id << ": " << e.what() << std::endl;
            return "{\"error\":\"rpc_error\"}";
        }
    }
    
    // 向指定节点发送SET请求
    std::string sendSetRequest(const std::string& node_id, const std::string& key, const std::string& value) {
        try {
            auto client = getRpcClientForNode(node_id);
            auto result = client->set(key, value);
            
            return result;
        } catch (const std::exception& e) {
            // std::cerr << "Error sending SET request to node " << node_id << ": " << e.what() << std::endl;
            return "{\"error\":\"rpc_error\"}";
        }
    }
    
    // 向指定节点发送DELETE请求
    std::string sendDeleteRequest(const std::string& node_id, const std::string& key) {
        try {
            auto client = getRpcClientForNode(node_id);
            auto result = client->remove(key);
            
            return result;
        } catch (const std::exception& e) {
            // std::cerr << "Error sending DELETE request to node " << node_id << ": " << e.what() << std::endl;
            return "{\"error\":\"rpc_error\"}";
        }
    }
    
private:
    std::vector<Node> nodes_;
    int total_nodes_;
    mutable std::shared_mutex mutex_;
    
    // 缓存RPC客户端实例
    std::unordered_map<std::string, std::shared_ptr<RpcClient>> rpc_clients_;
    

};

#endif