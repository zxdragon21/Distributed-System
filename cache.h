/*************************************************************************
	> File Name: cache.h
	> Author: 
	> Mail: 
	> Created Time: Fri Oct  3 17:02:51 2025
 ************************************************************************/
// cache.h
#ifndef CACHE_H
#define CACHE_H

#include <unordered_map>
#include <string>
#include <mutex>
#include <memory>

class Cache {
public:
    // 公共构造函数
    Cache() = default;
    
    // 删除拷贝构造函数和赋值运算符
    Cache(const Cache&) = delete;
    Cache& operator=(const Cache&) = delete;
    
    void set(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(mutex_);
        data_[key] = value;
    }
    
    bool get(const std::string& key, std::string& value) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = data_.find(key);
        if (it != data_.end()) {
            value = it->second;
            return true;
        }
        return false;
    }
    
    int remove(const std::string& key) {
        std::lock_guard<std::mutex> lock(mutex_);
        return data_.erase(key);
    }
    
private:
    std::unordered_map<std::string, std::string> data_;
    std::mutex mutex_;
};

#endif