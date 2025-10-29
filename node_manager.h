#ifndef NODE_MANAGER_H
#define NODE_MANAGER_H

#include <vector>
#include <string>
#include <memory>
#include <shared_mutex>
#include <functional>

struct Node {
    std::string id;
    std::string address;
    std::string http_port;
    std::string rpc_port;
};

class NodeManager {
public:
    NodeManager(int total_nodes = 3) {
        // 修复端口配置：节点1->9527, 节点2->9528, 节点3->9529
        for (int i = 1; i <= total_nodes; i++) {
            nodes_.push_back({
                "node" + std::to_string(i),
                "node" + std::to_string(i), 
                std::to_string(9526 + i),      // HTTP端口：9527, 9528, 9529
                std::to_string(27080 + i - 1)  // RPC端口：27080, 27081, 27082
            });
        }
        
        std::cout << "NodeManager initialized with " << total_nodes << " nodes:" << std::endl;
        for (const auto& node : nodes_) {
            std::cout << "  " << node.id << " -> " << node.address << ":" << node.http_port 
                      << " (RPC:" << node.rpc_port << ")" << std::endl;
        }
    }
    
    std::string getTargetNodeId(const std::string& key) {
        std::shared_lock lock(mutex_);
        size_t hash = std::hash<std::string>{}(key);
        size_t index = hash % nodes_.size();
        return nodes_[index].id;
    }
    
    Node getNodeById(const std::string& node_id) {
        std::shared_lock lock(mutex_);
        for (const auto& node : nodes_) {
            if (node.id == node_id) {
                return node;
            }
        }
        throw std::runtime_error("Node not found: " + node_id);
    }
    
private:
    std::vector<Node> nodes_;
    mutable std::shared_mutex mutex_;
};

#endif