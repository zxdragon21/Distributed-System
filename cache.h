/*************************************************************************
	> File Name: cache.h
	> Author: 
	> Mail: 
	> Created Time: Fri Oct  3 17:02:51 2025
 ************************************************************************/
// cache.h - 基于MVCC和乐观并发控制的缓存实现
#ifndef CACHE_H
#define CACHE_H

#include <unordered_map>
#include <string>
#include <mutex>
#include <shared_mutex>
#include <memory>
#include <chrono>
#include <atomic>
#include <condition_variable>
#include <vector>

#include "gossip_protocol.h"

// 单个版本的数据条目
struct VersionedEntry {
    std::string value;        // 数据值
    uint64_t version;         // 版本号
    std::chrono::steady_clock::time_point created_at; // 创建时间
    bool is_deleted;          // 是否被删除（用于软删除）
    
    VersionedEntry(const std::string& v, uint64_t ver) 
        : value(v), version(ver), created_at(std::chrono::steady_clock::now()), is_deleted(false) {}
};

class Cache {
public:
    // 公共构造函数
    Cache() : global_version_(0), stop_cleanup_(false) {
        // 启动定期清理线程
        cleanup_thread_ = std::thread(&Cache::periodicCleanup, this);
    }
    
    // 带gossip协议的构造函数
    Cache(std::shared_ptr<GossipProtocol> gossip) : global_version_(0), gossip_protocol_(gossip), stop_cleanup_(false) {
        // 启动定期清理线程
        cleanup_thread_ = std::thread(&Cache::periodicCleanup, this);
    }
    
    ~Cache() {
        // 停止清理线程
        stop_cleanup_ = true;
        cleanup_cv_.notify_one();
        if (cleanup_thread_.joinable()) {
            cleanup_thread_.join();
        }
    }
    
    // 删除拷贝构造函数和赋值运算符
    Cache(const Cache&) = delete;
    Cache& operator=(const Cache&) = delete;
    
    // 设置缓存
    uint64_t set(const std::string& key, const std::string& value, uint64_t expected_version = 0) {
        if (key.empty()) return 0;
        
        // 获取键级锁
        auto& key_mutex = getOrCreateKeyMutex(key);
        uint64_t actual_version = 0;
        bool is_deleted = false;
        
        // 减少锁嵌套：先获取data_mutex_读取，再获取key_mutex写入
        // 读取当前版本
        {   
            std::shared_lock<std::shared_mutex> data_lock(data_mutex_);
            auto it = data_.find(key);
            if (it != data_.end() && !it->second.empty()) {
                actual_version = it->second.back().version;
                is_deleted = it->second.back().is_deleted;
            }
        }
        
        // 已删除则直接返回
        if (is_deleted) return 0;
        
        // 分布式冲突检查
        if (gossip_protocol_) {
            uint64_t version_to_check = (expected_version > 0) ? expected_version : actual_version;
            if (version_to_check > 0 && gossip_protocol_->checkWriteConflict(key, version_to_check)) {
                return 0;
            }
        }
        
        // 原子操作获取新版本号
        uint64_t new_version = ++global_version_;
        
        try {
            // 按照固定顺序获取锁：先key_mutex，再data_mutex_
            std::unique_lock<std::shared_mutex> key_lock(key_mutex);
            std::unique_lock<std::shared_mutex> data_lock(data_mutex_);
            
            auto& versions = data_[key];
            
            // 版本匹配检查
            if (!versions.empty()) {
                uint64_t current_version = versions.back().version;
                if ((expected_version > 0 && current_version != expected_version) || 
                    (expected_version == 0 && current_version != actual_version)) {
                    return 0;
                }
            } else if (expected_version > 0) {
                return 0;
            }
            
            // 添加新版本
            versions.emplace_back(value, new_version);
            
            // 限制版本数量
            if (versions.size() > 10) {
                versions.erase(versions.begin(), versions.end() - 10);
            }
            
            data_lock.unlock(); // 尽早释放data_mutex_
            key_lock.unlock();  // 尽早释放key_mutex
            
            // 锁外通知
            if (gossip_protocol_) {
                try {
                    gossip_protocol_->notifyVersionUpdate(key, new_version);
                } catch (...) {}
            }
        } catch (...) {
            throw;
        }
        
        return new_version;
    }
    
    // 获取缓存（使用一致的锁获取顺序）
    bool get(const std::string& key, std::string& value, uint64_t* version = nullptr) {
        if (key.empty()) return false;
        
        bool found = false;
        try {
            // 优化锁顺序：先获取data_mutex_再获取key_mutex
            // 大部分时间我们只需要检查数据是否存在
            std::shared_lock<std::shared_mutex> data_lock(data_mutex_);
            
            auto it = data_.find(key);
            if (it != data_.end() && !it->second.empty()) {
                const auto& latest = it->second.back();
                if (!latest.is_deleted) {
                    value = latest.value;
                    if (version) *version = latest.version;
                    found = true;
                }
            }
        } catch (...) {
            throw;
        }
        
        return found;
    }
    
    // 获取指定版本缓存（优化：减少锁嵌套）
    bool getVersion(const std::string& key, std::string& value, uint64_t version) {
        if (key.empty() || version == 0) return false;
        
        bool found = false;
        try {
            // 优化：只获取data_mutex_
            std::shared_lock<std::shared_mutex> data_lock(data_mutex_);
            
            auto it = data_.find(key);
            if (it != data_.end()) {
                // 查找指定版本
                for (auto rit = it->second.rbegin(); rit != it->second.rend(); ++rit) {
                    if (rit->version <= version && !rit->is_deleted) {
                        value = rit->value;
                        found = true;
                        break;
                    }
                }
            }
        } catch (...) {
            throw;
        }
        
        return found;
    }
    
    // 软删除缓存（优化：减少锁嵌套，尽早释放锁）
    int remove(const std::string& key, uint64_t expected_version = 0) {
        if (key.empty()) return 0;
        
        // 获取键对应的互斥锁
        auto& key_mutex = getOrCreateKeyMutex(key);
        uint64_t actual_version = 0;
        bool is_present = false;
        bool is_already_deleted = false;
        
        // 优化：先只获取data_mutex_读取当前状态，减少锁嵌套
        {   
            std::shared_lock<std::shared_mutex> data_lock(data_mutex_);
            
            auto it = data_.find(key);
            if (it != data_.end() && !it->second.empty()) {
                actual_version = it->second.back().version;
                is_present = true;
                is_already_deleted = it->second.back().is_deleted;
            }
        }
        
        if (!is_present || is_already_deleted) return 0;
        
        uint64_t version_to_check = (expected_version > 0) ? expected_version : actual_version;
        
        if (gossip_protocol_ && gossip_protocol_->checkWriteConflict(key, version_to_check)) {
            return 0;
        }
        
        uint64_t new_version = ++global_version_;
        int result = 0;
        
        try {
            // 按照固定顺序获取锁：先key_mutex，再data_mutex_
            std::unique_lock<std::shared_mutex> key_lock(key_mutex);
            std::unique_lock<std::shared_mutex> data_lock(data_mutex_);
            
            auto it = data_.find(key);
            
            if (it != data_.end() && !it->second.empty() && !it->second.back().is_deleted) {
                uint64_t current_head_version = it->second.back().version;
                
                if ((expected_version > 0 && current_head_version != expected_version) ||
                    (expected_version == 0 && current_head_version != actual_version)) {
                    return 0;
                }
                
                // 执行软删除
                it->second.emplace_back(it->second.back().value, new_version);
                it->second.back().is_deleted = true;
                result = 1;
                
                // 限制版本数量
                if (it->second.size() > 10) {
                    it->second.erase(it->second.begin(), it->second.end() - 10);
                }
            }
            
            // 尽早释放锁
            data_lock.unlock();
            key_lock.unlock();
            
            // 锁外通知
            if (gossip_protocol_ && result > 0) {
                try {
                    gossip_protocol_->notifyVersionUpdate(key, new_version);
                } catch (...) {}
            }
        } catch (...) {
            throw;
        }
        
        return result;
    }
    
    // 强制移除缓存项
    bool forceRemove(const std::string& key) {
        if (key.empty()) return false;
        
        // 阶段1：检查key_mutex是否存在
        std::shared_mutex* key_mutex_ptr = nullptr;
        {
            std::unique_lock<std::mutex> map_lock(key_mutex_map_mutex_);
            auto it = key_mutexes_.find(key);
            if (it != key_mutexes_.end()) {
                key_mutex_ptr = &it->second;
            } else {
                return false; // 键不存在
            }
        }
        
        // 按照固定顺序获取锁：先key_mutex，再data_mutex_
        auto& key_mutex = *key_mutex_ptr;
        std::unique_lock<std::shared_mutex> key_lock(key_mutex);
        
        bool removed = false;
        {
            std::unique_lock<std::shared_mutex> data_lock(data_mutex_);
            auto it = data_.find(key);
            if (it != data_.end()) {
                if (!it->second.empty()) {
                    it->second.back().is_deleted = true;
                }
                data_.erase(it);
                removed = true;
            }
        }
        
        // 清理key_mutexes_
        if (removed) {
            std::unique_lock<std::mutex> map_lock(key_mutex_map_mutex_);
            key_mutexes_.erase(key);
        }
        
        return removed;
    }
    
    // 设置gossip协议实例
    void setGossipProtocol(std::shared_ptr<GossipProtocol> gossip) {
        gossip_protocol_ = gossip;
    }
    
private:
    // 定期清理过期的版本和完全删除的条目
    void periodicCleanup() {
        const auto cleanup_interval = std::chrono::seconds(30);
        const auto version_lifetime = std::chrono::minutes(5);
        
        while (!stop_cleanup_) {
            std::unique_lock<std::mutex> lock(cleanup_mutex_);
            
            // 等待清理间隔或停止信号
            if (cleanup_cv_.wait_for(lock, cleanup_interval) == std::cv_status::timeout) {
                cleanupExpiredVersions(version_lifetime);
            }
        }
    }
    
    // 清理过期的版本数据
    void cleanupExpiredVersions(const std::chrono::steady_clock::duration& lifetime) {
        std::unique_lock<std::shared_mutex> data_lock(data_mutex_);
        auto now = std::chrono::steady_clock::now();
        
        // 优化：批量处理，减少迭代器失效问题
        std::vector<decltype(data_)::iterator> to_remove;
        
        for (auto it = data_.begin(); it != data_.end(); ++it) {
            auto& versions = it->second;
            bool has_active_version = false;
            size_t keep_index = 0;
            
            // 从前往后扫描，找出第一个不过期的版本
            for (; keep_index < versions.size(); ++keep_index) {
                if (now - versions[keep_index].created_at <= lifetime) {
                    if (!versions[keep_index].is_deleted) {
                        has_active_version = true;
                    }
                    break;
                }
            }
            
            // 如果找到了不过期的起始位置，删除之前的所有版本
            if (keep_index > 0) {
                versions.erase(versions.begin(), versions.begin() + keep_index);
            }
            
            // 检查是否需要完全删除该条目
            bool all_deleted = true;
            for (const auto& v : versions) {
                if (!v.is_deleted) {
                    all_deleted = false;
                    break;
                }
            }
            
            if (versions.empty() || (all_deleted && versions.size() > 5)) {
                to_remove.push_back(it);
            }
        }
        
        // 批量删除标记的条目
        for (auto& it : to_remove) {
            data_.erase(it);
        }
    }
    
    // 数据结构
    std::unordered_map<std::string, std::vector<VersionedEntry>> data_;
    std::shared_mutex data_mutex_;  // 使用shared_mutex支持读写分离
    
    // 键级别的细粒度锁管理
    std::unordered_map<std::string, std::shared_mutex> key_mutexes_;
    std::mutex key_mutex_map_mutex_; // 保护key_mutexes_的锁
    
    // 版本控制 - 作为时间戳使用
    std::atomic<uint64_t> global_version_;  // 全局版本计数器（时间戳）
    
    // 定期清理机制
    std::thread cleanup_thread_;
    std::atomic<bool> stop_cleanup_{false};
    std::mutex cleanup_mutex_;
    std::condition_variable cleanup_cv_;
    
    // Gossip协议实例
    std::shared_ptr<GossipProtocol> gossip_protocol_;
    
    // 获取或创建键对应的互斥锁
    std::shared_mutex& getOrCreateKeyMutex(const std::string& key) {
        // 简化实现，直接使用unique_lock
        std::unique_lock<std::mutex> lock(key_mutex_map_mutex_);
        return key_mutexes_[key];
    }
};

#endif