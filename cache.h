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
        // 预分配内存以减少动态内存分配开销
        data_.reserve(10000);
    }
    
    // 构造函数（兼容原有接口，但不再使用gossip协议）
    Cache(std::shared_ptr<GossipProtocol> /*gossip*/) {
        // 预分配内存以减少动态内存分配开销
        data_.reserve(10000);
    }
    
    ~Cache() {
        // 简化析构函数
    }
    
    // 删除拷贝构造函数和赋值运算符
    Cache(const Cache&) = delete;
    Cache& operator=(const Cache&) = delete;
    
    // 设置缓存 - 简化版本
    bool set(const std::string& key, const std::string& value) {
        if (key.empty()) return false;
        
        // 获取键对应的分段索引和锁
        size_t segment_index = getSegmentIndex(key);
        
        // 直接更新缓存（独占锁）
        {
            std::unique_lock<std::shared_mutex> lock(segment_locks_[segment_index]);
            // 简化为直接存储单个条目
            data_[key] = Entry(value);
        }
        
        return true;
    }
    
    // 获取缓存 - 优化版本，进一步提高性能
    bool get(const std::string& key, std::string& value) {
        if (key.empty()) return false;
        
        size_t segment_index = getSegmentIndex(key);
        
        // 只读操作使用共享锁，确保锁的作用域尽可能小
        {   
            std::shared_lock<std::shared_mutex> lock(segment_locks_[segment_index]);
            
            // 直接查找键，不再需要检查is_deleted标志（因为remove已经完全擦除）
            auto it = data_.find(key);
            if (it != data_.end()) {
                // 直接获取值并返回
                value = it->second.value;
                return true;
            }
        }
        
        return false;
    }
    
    // 批量获取缓存 - 高性能版本，支持一次查询多个键
    void batchGet(const std::vector<std::string>& keys, std::unordered_map<std::string, std::string>& results) {
        // 按分段索引分组，减少锁的争用
        std::unordered_map<size_t, std::vector<std::string>> keys_by_segment;
        
        for (const auto& key : keys) {
            if (!key.empty()) {
                size_t segment_index = getSegmentIndex(key);
                keys_by_segment[segment_index].push_back(key);
            }
        }
        
        // 对每个分段单独加锁查询，最大化并发性能
        for (const auto& [segment_index, segment_keys] : keys_by_segment) {
            std::shared_lock<std::shared_mutex> lock(segment_locks_[segment_index]);
            
            for (const auto& key : segment_keys) {
                auto it = data_.find(key);
                if (it != data_.end()) {
                    results[key] = it->second.value;
                }
            }
        }
    }
    
    // 删除缓存 - 高性能版本
    bool remove(const std::string& key) {
        if (key.empty()) return false;
        
        size_t segment_index = getSegmentIndex(key);
        
        // 最小锁作用域
        {
            std::unique_lock<std::shared_mutex> lock(segment_locks_[segment_index]);
            
            // 直接调用erase方法，返回删除的元素数量
            auto erased_count = data_.erase(key);
            return erased_count > 0; // 如果删除成功，返回true
        }
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
    std::unordered_map<std::string, Entry> data_; // 缓存数据存储
    
    // 分段锁，增加锁数量以提高并发性
    static constexpr size_t kSegmentCount = 128; // 增加到128个分段锁
    std::vector<std::shared_mutex> segment_locks_ = std::vector<std::shared_mutex>(kSegmentCount);
    
    // 获取键对应的分段索引 - 高效版本
    size_t getSegmentIndex(const std::string& key) {
        // 使用静态常量，避免运行时计算
        
        // 使用更高效的哈希计算方式
        std::hash<std::string> hasher;
        size_t hash_value = hasher(key);
        
        // 因为kSegmentCount是2的幂，所以直接使用位运算
        return hash_value & (kSegmentCount - 1);
    }
};

#endif