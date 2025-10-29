#ifndef CACHE_SERVER_NO_RPC_H
#define CACHE_SERVER_NO_RPC_H

#include "cache.h"
#include "node_manager.h"
#include <nlohmann/json.hpp>
#include <crow.h>
#include <atomic>
#include <iostream>

class CacheServerNoRpc {
public:
    CacheServerNoRpc(const std::string& node_id, const std::string& port) 
        : node_id_(node_id), port_(port) {
        
        std::cout << "CacheServerNoRpc constructor started" << std::endl;
        setupHttpRoutes();
    }
    
    void run() {
        std::cout << "Starting cache server " << node_id_ 
                  << " on HTTP port " << port_ << std::endl;
        
        try {
            app.port(std::stoi(port_)).run();
        } catch (std::exception& e) {
            std::cerr << "Server exception: " << e.what() << std::endl;
        }
    }
    
private:
    void setupHttpRoutes() {
        std::cout << "Setting up HTTP routes..." << std::endl;
        
        CROW_ROUTE(app, "/test")
        ([]() {
            std::cout << "Test route called" << std::endl;
            return "Test OK!";
        });
        
        CROW_ROUTE(app, "/")
        ([]() {
            std::cout << "Root route called" << std::endl;
            return "Root OK!";
        });
        
        CROW_ROUTE(app, "/").methods("POST"_method)
        ([this](const crow::request& req) {
            std::cout << "POST / received" << std::endl;
            try {
                auto json = nlohmann::json::parse(req.body);
                std::cout << "JSON: " << json.dump() << std::endl;
                
                for (auto& [key, value] : json.items()) {
                    cache_.set(key, value.dump());
                    std::cout << "Stored: " << key << std::endl;
                }
                
                nlohmann::json response = {{"status", "success"}};
                return crow::response(200, response.dump());
                
            } catch (std::exception& e) {
                return crow::response(400, "Invalid JSON");
            }
        });
        
        CROW_ROUTE(app, "/<string>").methods("GET"_method)
        ([this](const std::string& key) {
            std::cout << "GET /" << key << " received" << std::endl;
            std::string value;
            if (cache_.get(key, value)) {
                nlohmann::json response;
                response[key] = nlohmann::json::parse(value);
                return crow::response(200, response.dump());
            } else {
                return crow::response(404);
            }
        });
        
        std::cout << "HTTP routes setup complete" << std::endl;
    }
    
    std::string node_id_;
    std::string port_;
    crow::SimpleApp app;
    Cache cache_;
    NodeManager node_manager_;
};

#endif
