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

// 简化的数据条目，移除版本控制
struct Entry {
    std::string value;        // 数据值
    bool is_deleted;          // 是否被删除（用于软删除）
    
    // 默认构造函数
    Entry() = default;
    
    Entry(const std::string& v) 
        : value(v), is_deleted(false) {}
};

class Cache {
public:
    // 公共构造函数
    Cache() {
        // 简化构造函数，移除版本控制相关初始化
    }
    
    // 构造函数（兼容原有接口，但不再使用gossip协议）
    Cache(std::shared_ptr<GossipProtocol> /*gossip*/) {
        // 简化构造函数，不再使用gossip协议
    }
    
    ~Cache() {
        // 简化析构函数
    }
    
    // 删除拷贝构造函数和赋值运算符
    Cache(const Cache&) = delete;
    Cache& operator=(const Cache&) = delete;
    
    // 设置缓存 - 简化版本，移除版本控制
    bool set(const std::string& key, const std::string& value) {
        if (key.empty()) return false;
        
        // 获取键对应的分段索引和锁
        size_t segment_index = getSegmentIndex(key);
        
        // 直接更新缓存（独占锁）
        {
            std::unique_lock<std::shared_mutex> lock(segment_locks_[segment_index]);
            // 简化为直接存储单个条目，不再维护版本历史
            data_[key] = Entry(value);
        }
        
        return true;
    }
    
    // 获取缓存 - 简化版本
    bool get(const std::string& key, std::string& value) {
        if (key.empty()) return false;
        
        size_t segment_index = getSegmentIndex(key);
        
        // 只读操作使用共享锁
        std::shared_lock<std::shared_mutex> lock(segment_locks_[segment_index]);
        
        auto it = data_.find(key);
        if (it != data_.end() && !it->second.is_deleted) {
            value = it->second.value;
            return true;
        }
        
        return false;
    }
    
    // 删除缓存 - 简化版本
    bool remove(const std::string& key) {
        if (key.empty()) return false;
        
        size_t segment_index = getSegmentIndex(key);
        
        // 最小锁作用域
        {
            std::unique_lock<std::shared_mutex> lock(segment_locks_[segment_index]);
            
            auto it = data_.find(key);
            if (it == data_.end() || it->second.is_deleted) {
                return false; // 键不存在或已删除
            }
            
            // 直接删除
            data_.erase(it);
        }
        
        return true;
    }
    
    // 强制移除缓存项 - 简化版本
    bool forceRemove(const std::string& key) {
        if (key.empty()) return false;
        
        size_t segment_index = getSegmentIndex(key);
        
        // 使用独占锁执行删除操作
        std::unique_lock<std::shared_mutex> lock(segment_locks_[segment_index]);
        
        auto it = data_.find(key);
        if (it != data_.end()) {
            data_.erase(it);
            return true;
        }
        
        return false;
    }
    
    // 兼容方法，不再使用gossip协议
    void setGossipProtocol(std::shared_ptr<GossipProtocol> /*gossip*/) {
        // 不再使用gossip协议
    }
    
private:
    // 数据结构 - 简化为直接存储单个条目
    std::unordered_map<std::string, Entry> data_;
    
    // 使用分段锁提高并发性能
    static constexpr size_t NUM_SEGMENTS = 16;
    std::array<std::shared_mutex, NUM_SEGMENTS> segment_locks_;
    
    // 获取键对应的分段索引
    size_t getSegmentIndex(const std::string& key) const {
        std::hash<std::string> hasher;
        return hasher(key) % NUM_SEGMENTS;
    }
};

#endif