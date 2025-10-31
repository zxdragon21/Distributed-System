/*************************************************************************
	> File Name: gossip_protocol.h
	> Author: 
	> Mail: 
	> Created Time: Sat Oct  4 10:00:00 2025
 ************************************************************************/
// gossip_protocol.h - Gossip协议实现，用于分布式缓存的并发控制
#ifndef GOSSIP_PROTOCOL_H
#define GOSSIP_PROTOCOL_H

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
#include <memory>
#include <functional>
#include <iostream>
#include <sstream>
#include <condition_variable>

// Gossip消息类型
enum class GossipMessageType {
    HEARTBEAT,
    READ_LOCK_REQUEST,
    READ_LOCK_RELEASE,
    WRITE_LOCK_REQUEST,
    WRITE_LOCK_RELEASE,
    UPDATE_VERSION,
    NODE_JOIN,
    NODE_LEAVE
};

// Gossip消息结构
struct GossipMessage {
    GossipMessageType type;
    std::string sender_id;
    std::string target_key;  // 缓存键
    uint64_t version;        // 版本信息
    uint64_t timestamp;      // 时间戳
    bool approved;           // 对于锁请求的响应
    
    // 序列化和反序列化方法
    std::string serialize() const {
        std::stringstream ss;
        ss << "{";
        ss << "\"type\": " << static_cast<int>(type) << ",";
        ss << "\"sender_id\": \"" << sender_id << "\",";
        ss << "\"timestamp\": " << timestamp;
        
        if (!target_key.empty()) {
            ss << ",\"target_key\": \"" << target_key << "\"";
        }
        
        ss << ",\"version\": " << version;
        ss << ",\"approved\": " << (approved ? "true" : "false");
        ss << "}";
        return ss.str();
    }
    
    static GossipMessage deserialize(const std::string& data) {
        GossipMessage message;
        // 简单的JSON解析
        
        // 提取type
        size_t type_pos = data.find("\"type\": ");
        if (type_pos != std::string::npos) {
            type_pos += 8; // 跳过 "type": 
            size_t comma_pos = data.find(",", type_pos);
            if (comma_pos != std::string::npos) {
                message.type = static_cast<GossipMessageType>(std::stoi(data.substr(type_pos, comma_pos - type_pos)));
            }
        }
        
        // 提取sender_id
        size_t sender_pos = data.find("\"sender_id\": \"");
        if (sender_pos != std::string::npos) {
            sender_pos += 14; // 跳过 "sender_id": "
            size_t quote_pos = data.find("\"", sender_pos);
            if (quote_pos != std::string::npos) {
                message.sender_id = data.substr(sender_pos, quote_pos - sender_pos);
            }
        }
        
        // 提取timestamp
        size_t ts_pos = data.find("\"timestamp\": ");
        if (ts_pos != std::string::npos) {
            ts_pos += 13; // 跳过 "timestamp": 
            size_t comma_pos = data.find(",", ts_pos);
            size_t end_pos = (comma_pos != std::string::npos) ? comma_pos : data.find("}", ts_pos);
            if (end_pos != std::string::npos) {
                message.timestamp = std::stoll(data.substr(ts_pos, end_pos - ts_pos));
            }
        }
        
        // 提取target_key
        size_t key_pos = data.find("\"target_key\": \"");
        if (key_pos != std::string::npos) {
            key_pos += 14; // 跳过 "target_key": "
            size_t quote_pos = data.find("\"", key_pos);
            if (quote_pos != std::string::npos) {
                message.target_key = data.substr(key_pos, quote_pos - key_pos);
            }
        }
        
        // 提取version
        size_t version_pos = data.find("\"version\": ");
        if (version_pos != std::string::npos) {
            version_pos += 12; // 跳过 "version": 
            size_t comma_pos = data.find(",", version_pos);
            size_t end_pos = (comma_pos != std::string::npos) ? comma_pos : data.find("}", version_pos);
            if (end_pos != std::string::npos) {
                message.version = std::stoull(data.substr(version_pos, end_pos - version_pos));
            }
        }
        
        // 提取approved
        size_t approved_pos = data.find("\"approved\": ");
        if (approved_pos != std::string::npos) {
            approved_pos += 13; // 跳过 "approved": 
            size_t end_pos = data.find("}", approved_pos);
            if (end_pos != std::string::npos) {
                std::string approved_str = data.substr(approved_pos, end_pos - approved_pos);
                message.approved = (approved_str == "true");
            }
        }
        
        return message;
    }
};

// 节点状态
enum class NodeStatus {
    ONLINE,
    SUSPECTED,
    OFFLINE
};

// 节点信息
struct NodeInfo {
    std::string node_id;
    std::string address;  // IP:port
    NodeStatus status;
    uint64_t last_heartbeat;
    
    NodeInfo(const std::string& id, const std::string& addr) 
        : node_id(id), address(addr), status(NodeStatus::ONLINE), last_heartbeat(0) {}
};

// 版本信息 - 用于乐观并发控制
struct VersionInfo {
    uint64_t latest_version; // 最新版本号
    std::string last_writer; // 最后写入节点ID
    uint64_t last_update_time; // 最后更新时间
};

// Gossip协议管理器
class GossipProtocol {
public:
    // 构造函数 - 用于集成到CacheServer中
    GossipProtocol(const std::string& node_id, int total_nodes = 3)
        : node_id_(node_id), address_(), send_function_(), running_(false),
          rng_(std::random_device{}()), dist_(0.0, 1.0),
          GOSSIP_FANOUT(3), GOSSIP_INTERVAL_MS(5000), HEARTBEAT_INTERVAL_MS(2000),
          STATUS_CHECK_INTERVAL_MS(10000), NODE_TIMEOUT_MS(15000),
          VERSION_INFO_TTL_MS(60000), CLEANUP_INTERVAL_MS(20000) {
        // 初始化协议，移除调试输出
    }
    
    // 构造函数 - 完整版本
    GossipProtocol(const std::string& node_id, const std::string& address,
                  std::function<bool(const std::string&, const std::string&)> send_fn)
        : node_id_(node_id), address_(address), send_function_(send_fn), running_(false),
          rng_(std::random_device{}()), dist_(0.0, 1.0),
          GOSSIP_FANOUT(3), GOSSIP_INTERVAL_MS(1000), HEARTBEAT_INTERVAL_MS(500),
          STATUS_CHECK_INTERVAL_MS(2000), NODE_TIMEOUT_MS(5000),
          VERSION_INFO_TTL_MS(60000), CLEANUP_INTERVAL_MS(5000) {
        // 初始化协议，移除调试输出
    }
    
    ~GossipProtocol() {
        stop();
    }
    
    // 删除拷贝构造和赋值运算符
    GossipProtocol(const GossipProtocol&) = delete;
    GossipProtocol& operator=(const GossipProtocol&) = delete;
    
    // 启动gossip协议
    void start() {
        if (!running_) {
            running_ = true;
            gossip_thread_ = std::thread(&GossipProtocol::gossipTask, this);
            heartbeat_thread_ = std::thread(&GossipProtocol::heartbeatTask, this);
            status_check_thread_ = std::thread(&GossipProtocol::checkNodeStatus, this);
            cleanup_thread_ = std::thread(&GossipProtocol::cleanupExpiredVersions, this);
            // 启动协议，移除调试输出
        }
    }
    
    // 停止gossip协议
    void stop() {
        if (running_) {
            running_ = false;
            
            if (gossip_thread_.joinable()) {
                gossip_thread_.join();
            }
            if (heartbeat_thread_.joinable()) {
                heartbeat_thread_.join();
            }
            if (status_check_thread_.joinable()) {
                status_check_thread_.join();
            }
            if (cleanup_thread_.joinable()) {
                cleanup_thread_.join();
            }
            
            // 停止协议，移除调试输出
        }
    }
    
    // 发送消息到指定节点
    bool sendMessage(const std::string& target_node, const GossipMessage& message) {
        if (send_function_) {
            std::string serialized = serializeMessage(message);
            return send_function_(target_node, serialized);
        }
        return false;
    }
    
    // 广播消息给所有节点（优化：直接获取锁）
    void broadcastMessage(const GossipMessage& message) {
        std::vector<std::string> targets;
        
        // 优化：直接获取锁，减少复杂性和等待时间
        std::unique_lock<std::mutex> lock(nodes_mutex_);
        
        for (const auto& [id, node] : nodes_) {
            if (id != node_id_ && node && node->status == NodeStatus::ONLINE) {
                targets.push_back(node->address);
            }
        }
        lock.unlock(); // 显式解锁
          
          // 同步发送消息，避免频繁创建线程
          for (const auto& target : targets) {
              sendMessage(target, message);
          }
    }
    
    // 处理接收到的消息
    void processMessage(const GossipMessage& message) {
        // 更新发送者的心跳
        updatePeerHeartbeat(message.sender_id);
        
        // 根据消息类型处理
        switch (message.type) {
            case GossipMessageType::HEARTBEAT:
                // 心跳消息只更新节点状态
                break;
            case GossipMessageType::UPDATE_VERSION:
                handleVersionUpdate(message);
                break;
            case GossipMessageType::NODE_JOIN:
            case GossipMessageType::NODE_LEAVE:
                handleStatusUpdate(message);
                break;
            default:
                // 移除调试输出
                break;
        }
    }
    
    // 处理版本更新消息（优化：直接获取锁）
    void handleVersionUpdate(const GossipMessage& message) {
        if (!message.target_key.empty()) {
            VersionInfo remote_info;
            remote_info.latest_version = message.version;
            remote_info.last_writer = message.sender_id;
            remote_info.last_update_time = message.timestamp;
            
            // 优化：直接获取锁，减少复杂性和等待时间
            {   // 作用域限制锁的持有时间
                std::unique_lock<std::shared_mutex> lock(version_mutex_);
                
                // 直接进行版本合并操作，避免调用其他可能获取锁的函数
                auto it = version_map_.find(message.target_key);
                bool updated = false;
                
                // 如果本地没有版本信息或者远程版本更新，则更新本地信息
                if (it == version_map_.end() || remote_info.latest_version > it->second.latest_version) {
                    version_map_[message.target_key] = remote_info;
                    updated = true;
                    // 移除调试输出
                }
                
                // 如果更新了版本，异步广播给其他节点
                if (updated) {
                    std::thread([this, key = message.target_key, version = message.version]() {
                        // 移除不必要的延迟
                        broadcastVersionUpdate(key, version);
                    }).detach();
                }
            }
            
            // 移除调试输出
        }
    }
    
    // 处理状态更新消息
    void handleStatusUpdate(const GossipMessage& message) {
        // 这里可以实现状态同步逻辑
        // 例如更新本地的节点状态等
        std::cout << "[GOSSIP] Handling status update message from: " << message.sender_id << std::endl;
    }
    
    // 获取键的最新版本信息
    VersionInfo getLatestVersionInfo(const std::string& key) {
        // 优化：直接获取共享锁，减少复杂性和等待时间
        std::shared_lock<std::shared_mutex> lock(version_mutex_);
        
        auto it = version_map_.find(key);
        
        if (it != version_map_.end()) {
            return it->second;
        }
        
        // 返回默认版本信息
        return {0, "", getCurrentTimestamp()};
    }
    
    // 检查版本是否可写（无冲突）- 实现真正的乐观并发控制
    bool checkWriteConflict(const std::string& key, uint64_t expected_version) {
        // 实现真正的版本冲突检测
        std::shared_lock<std::shared_mutex> lock(version_mutex_);
        
        auto it = version_map_.find(key);
        
        // 如果本地有该键的版本信息
        if (it != version_map_.end()) {
            const VersionInfo& local_info = it->second;
            
            // 如果提供了期望版本，检查是否匹配
            if (expected_version > 0) {
                // 有冲突的情况：本地最新版本大于期望版本
                // 说明其他节点已经修改了该键的值
                if (local_info.latest_version > expected_version) {
                    return true; // 存在冲突
                }
            }
            
            // 移除不必要的延迟检查，减少误判冲突
        }
        
        // 无冲突，允许写入
        return false;
    }
    

    
    // 广播版本更新消息（使用批量异步发送）
    void broadcastVersionUpdate(const std::string& key, uint64_t new_version) {
        // 优化：使用消息池避免频繁创建消息对象
        GossipMessage message;
        message.type = GossipMessageType::UPDATE_VERSION;
        message.sender_id = node_id_;
        message.target_key = key;
        message.version = new_version;
        message.timestamp = getCurrentTimestamp();
        
        // 获取目标节点列表
        std::vector<std::string> targets = selectRandomNodes(GOSSIP_FANOUT);
        
        // 优化：批量异步发送，减少线程创建开销
        if (!targets.empty()) {
            // 将所有目标节点的消息发送任务合并到一个线程中
            std::thread([this, message, targets]() {
                for (const auto& target : targets) {
                    try {
                        sendMessage(target, message);
                    } catch (...) {
                        // 忽略单个发送失败
                    }
                }
            }).detach();
        }
    }
    
    // 合并远程版本信息
    void mergeRemoteVersionInfo(const std::string& key, const VersionInfo& remote_info) {
        // 优化：直接获取锁，减少复杂性和等待时间
        std::unique_lock<std::shared_mutex> lock(version_mutex_);
        
        auto it = version_map_.find(key);
        
        // 如果本地没有版本信息或者远程版本更新，则更新本地信息
        if (it == version_map_.end() || remote_info.latest_version > it->second.latest_version) {
            version_map_[key] = remote_info;
            // 移除调试输出
        }
    }
    
    // 添加节点
    void addNode(const std::string& node_id, const std::string& address) {
        std::unique_lock<std::mutex> lock(nodes_mutex_);
        auto it = nodes_.find(node_id);
        
        if (it == nodes_.end()) {
            auto node_info = std::make_shared<NodeInfo>(node_id, address);
            node_info->last_heartbeat = getCurrentTimestamp();
            nodes_[node_id] = node_info;
            // 移除调试输出
        }
    }
    
    // 移除节点
    void removeNode(const std::string& node_id) {
        std::unique_lock<std::mutex> lock(nodes_mutex_);
        auto it = nodes_.find(node_id);
        
        if (it != nodes_.end()) {
            nodes_.erase(it);
            // 移除调试输出
        }
    }
    
    // 获取在线节点列表
    std::vector<std::string> getOnlineNodes() {
        std::vector<std::string> online_nodes;
        std::unique_lock<std::mutex> lock(nodes_mutex_);
        
        for (const auto& [id, node] : nodes_) {
            if (node && node->status == NodeStatus::ONLINE) {
                online_nodes.push_back(id);
            }
        }
        
        return online_nodes;
    }
    
    // 通知版本更新（优化：减少线程创建开销）
    void notifyVersionUpdate(const std::string& key, uint64_t new_version) {
        std::unique_lock<std::shared_mutex> lock(version_mutex_);
        VersionInfo& info = version_map_[key];
        
        if (info.latest_version < new_version) {
            info.latest_version = new_version;
            info.last_writer = node_id_;
            info.last_update_time = getCurrentTimestamp();
            
            // 锁释放后再广播版本更新
            lock.unlock();
            
            // 同步处理广播，避免线程创建开销
            GossipMessage message;
            message.type = GossipMessageType::UPDATE_VERSION;
            message.sender_id = node_id_;
            message.target_key = key;
            message.version = new_version;
            message.timestamp = getCurrentTimestamp();
            
            // 直接广播，不创建新线程
            std::vector<std::string> targets = selectRandomNodes(GOSSIP_FANOUT);
            for (const auto& target : targets) {
                try {
                    sendMessage(target, message);
                } catch (...) {
                    // 忽略单个发送失败
                }
            }
        }
    }
    
    // 获取当前时间戳（毫秒）
    uint64_t getCurrentTimestamp() const {
        auto now = std::chrono::system_clock::now();
        auto duration = now.time_since_epoch();
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(duration).count());
    }
    
    // 消息序列化
    std::string serializeMessage(const GossipMessage& message) {
        return message.serialize();
    }
    
    // 消息反序列化
    GossipMessage deserializeMessage(const std::string& data) {
        return GossipMessage::deserialize(data);
    }
    
    // 处理接收到的原始消息
    void handleMessage(const std::string& sender, const std::string& message) {
        try {
            // 反序列化消息
            GossipMessage msg = deserializeMessage(message);
            
            // 更新发送者的心跳时间
            updatePeerHeartbeat(sender);
            
            // 处理消息
            processMessage(msg);
        } catch (const std::exception& e) {
            // 移除调试输出
        }
    }
    
    // 更新节点心跳
    void updatePeerHeartbeat(const std::string& peer_id) {
        std::unique_lock<std::mutex> lock(nodes_mutex_);
        auto it = nodes_.find(peer_id);
        if (it != nodes_.end() && it->second) {
            it->second->status = NodeStatus::ONLINE;
            it->second->last_heartbeat = getCurrentTimestamp();
        }
    }
    
private:
    // 定期gossip任务
    void gossipTask() {
        while (running_) {
            // 随机选择一些节点进行gossip
            std::vector<std::string> targets = selectRandomNodes(GOSSIP_FANOUT);
            
            // 这里可以添加更多的gossip逻辑，比如传播版本信息等
            
            // 等待下一次gossip
            std::this_thread::sleep_for(std::chrono::milliseconds(GOSSIP_INTERVAL_MS));
        }
    }
    
    // 心跳任务
    void heartbeatTask() {
        while (running_) {
            GossipMessage heartbeat_msg;
            heartbeat_msg.type = GossipMessageType::HEARTBEAT;
            heartbeat_msg.sender_id = node_id_;
            heartbeat_msg.timestamp = getCurrentTimestamp();
            
            // 随机选择一些节点发送心跳
            std::vector<std::string> targets = selectRandomNodes(GOSSIP_FANOUT);
            for (const auto& target : targets) {
                sendMessage(target, heartbeat_msg);
            }
            
            // 等待下一次心跳
            std::this_thread::sleep_for(std::chrono::milliseconds(HEARTBEAT_INTERVAL_MS));
        }
    }
    
    // 节点状态检查
    void checkNodeStatus() {
        while (running_) {
            std::unique_lock<std::mutex> lock(nodes_mutex_);
            uint64_t current_time = getCurrentTimestamp();
            
            for (auto& [id, node] : nodes_) {
                if (node && current_time - node->last_heartbeat > NODE_TIMEOUT_MS) {
                    if (node->status == NodeStatus::ONLINE) {
                        node->status = NodeStatus::SUSPECTED;
                        // 移除调试输出
                    } else if (node->status == NodeStatus::SUSPECTED && 
                              current_time - node->last_heartbeat > NODE_TIMEOUT_MS * 2) {
                        node->status = NodeStatus::OFFLINE;
                        // 移除调试输出
                    }
                }
            }
            
            lock.unlock();
            // 等待下一次检查
            std::this_thread::sleep_for(std::chrono::milliseconds(STATUS_CHECK_INTERVAL_MS));
        }
    }
    
    // 随机选择k个节点进行gossip
    std::vector<std::string> selectRandomNodes(size_t k) {
        std::vector<std::string> result;
        std::vector<std::string> peers_list;
        
        // 获取所有在线节点的地址
        {
            std::unique_lock<std::mutex> lock(nodes_mutex_);
            for (const auto& [id, node] : nodes_) {
                if (node && node->status == NodeStatus::ONLINE && id != node_id_) {
                    peers_list.push_back(node->address);
                }
            }
        }
        
        // 如果没有足够的节点，返回空结果
        if (peers_list.empty()) {
            return result;
        }
        
        // 随机选择k个节点
        std::shuffle(peers_list.begin(), peers_list.end(), rng_);
        size_t count = std::min(k, peers_list.size());
        result.assign(peers_list.begin(), peers_list.begin() + count);
        
        return result;
    }
    
    // 清理过期的版本信息
    void cleanupExpiredVersions() {
        while (running_) {
            // 简化锁获取逻辑，使用try_lock避免长时间阻塞
            std::unique_lock<std::shared_mutex> lock(version_mutex_, std::defer_lock);
            if (lock.try_lock()) {
                uint64_t now = getCurrentTimestamp();
                
                for (auto it = version_map_.begin(); it != version_map_.end();) {
                    // 检查版本信息是否过期
                    if (now - it->second.last_update_time > VERSION_INFO_TTL_MS) {
                        // 移除调试输出
                        it = version_map_.erase(it);
                    } else {
                        ++it;
                    }
                }
                
                lock.unlock();
            }
            
            // 等待下一次清理
            std::this_thread::sleep_for(std::chrono::milliseconds(CLEANUP_INTERVAL_MS));
        }
    }
    
    // 当前节点信息
    std::string node_id_;
    std::string address_;
    
    // 节点列表
    std::unordered_map<std::string, std::shared_ptr<NodeInfo>> nodes_;
    std::mutex nodes_mutex_;
    
    // 版本状态 - 用于乐观并发控制
    std::unordered_map<std::string, VersionInfo> version_map_;
    std::shared_mutex version_mutex_;  // 读写锁保护版本状态
    
    // 消息发送函数
    std::function<bool(const std::string&, const std::string&)> send_function_;
    
    // 线程控制
    std::atomic<bool> running_;
    std::thread gossip_thread_;
    std::thread heartbeat_thread_;
    std::thread status_check_thread_;
    std::thread cleanup_thread_;
    
    // 随机数生成器
    std::mt19937_64 rng_;
    std::uniform_real_distribution<double> dist_;
    
    // 配置参数
    const size_t GOSSIP_FANOUT;        // 每次gossip选择的节点数
    const int GOSSIP_INTERVAL_MS;      // gossip间隔
    const int HEARTBEAT_INTERVAL_MS;   // 心跳间隔
    const int STATUS_CHECK_INTERVAL_MS;// 状态检查间隔
    const int NODE_TIMEOUT_MS;         // 节点超时时间
    const int VERSION_INFO_TTL_MS;     // 版本信息过期时间
    const int CLEANUP_INTERVAL_MS;     // 清理间隔
};

#endif // GOSSIP_PROTOCOL_H