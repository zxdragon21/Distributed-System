/*************************************************************************
	> File Name: rpc_server.h
	> Author: 
	> Mail: 
	> Created Time: Fri Oct  3 17:04:14 2025
 ************************************************************************/
// rpc_server.h
#ifndef RPC_SERVER_H
#define RPC_SERVER_H

#include <boost/asio.hpp>
#include <thread>
#include <memory>
#include <unordered_map>
#include <iostream>
#include "cache.h"
#include "rpc_protocol.h"

using boost::asio::ip::tcp;

class RpcServer {
public:
    RpcServer(boost::asio::io_context& io_context, short port, Cache& cache)
        : acceptor_(io_context, tcp::endpoint(tcp::v4(), port)), cache_(cache) {
        std::cout << "RpcServer constructor called for port: " << port << std::endl;
        try {
            startAccept();
            std::cout << "RpcServer successfully started on port: " << port << std::endl;
        } catch (std::exception& e) {
            std::cerr << "RpcServer failed to start on port " << port << ": " << e.what() << std::endl;
            throw;
        }
    }
    
private:
    void startAccept() {
        std::cout << "RpcServer startAccept called" << std::endl;
        auto socket = std::make_shared<tcp::socket>(acceptor_.get_executor());
        acceptor_.async_accept(*socket, [this, socket](boost::system::error_code ec) {
            if (!ec) {
                std::cout << "RpcServer accepted new connection" << std::endl;
                std::thread(&RpcServer::handleClient, this, socket).detach();
            } else {
                std::cerr << "RpcServer accept error: " << ec.message() << std::endl;
            }
            startAccept();
        });
        std::cout << "RpcServer async_accept setup complete" << std::endl;
    }
    
    void handleClient(std::shared_ptr<tcp::socket> socket) {
        std::cout << "RpcServer handling client connection" << std::endl;
        try {
            while (true) {
                // 接收header
                RpcHeader header;
                boost::system::error_code error;
                size_t length = boost::asio::read(*socket, 
                    boost::asio::buffer(&header, sizeof(RpcHeader)), error);
                
                if (error == boost::asio::error::eof) {
                    std::cout << "RpcServer client disconnected" << std::endl;
                    break; // 连接关闭
                } else if (error) {
                    std::cerr << "RpcServer read error: " << error.message() << std::endl;
                    throw boost::system::system_error(error);
                }
                
                std::cout << "RpcServer received request type: " << static_cast<int>(header.type) 
                          << ", length: " << header.length << std::endl;
                
                // 接收body
                std::string body(header.length, 0);
                if (header.length > 0) {
                    boost::asio::read(*socket, boost::asio::buffer(body));
                }
                
                // 处理请求
                std::string response = processRequest(header, body);
                
                // 发送响应
                RpcMessage response_msg = RpcMessage::createResponse(header.request_id, response);
                boost::asio::write(*socket, boost::asio::buffer(&response_msg.header, sizeof(RpcHeader)));
                if (!response.empty()) {
                    boost::asio::write(*socket, boost::asio::buffer(response));
                }
                
                std::cout << "RpcServer sent response: " << response << std::endl;
            }
        } catch (std::exception& e) {
            std::cerr << "RPC client handling exception: " << e.what() << std::endl;
        }
    }
    
    std::string processRequest(const RpcHeader& header, const std::string& body) {
        std::cout << "RpcServer processing request, body: " << body << std::endl;
        nlohmann::json response;
        
        try {
            auto data = nlohmann::json::parse(body);
            
            // 使用传入的cache实例，而不是单例
            switch (header.type) {
                case RpcType::GET: {
                    std::string key = data["key"];
                    std::string value;
                    if (cache_.get(key, value)) {
                        response[key] = nlohmann::json::parse(value);
                    } else {
                        response["error"] = "not_found";
                    }
                    break;
                }
                case RpcType::SET: {
                    for (auto it = data.begin(); it != data.end(); ++it) {
                        std::string key = it.key();
                        std::string value = it.value().dump();
                        cache_.set(key, value);
                    }
                    response["status"] = "success";
                    break;
                }
                case RpcType::DELETE: {
                    std::string key = data["key"];
                    int count = cache_.remove(key);
                    response["count"] = count;
                    break;
                }
                default:
                    response["error"] = "unknown_operation";
            }
        } catch (std::exception& e) {
            std::cerr << "RpcServer processRequest exception: " << e.what() << std::endl;
            response["error"] = "invalid_request";
        }
        
        return response.dump();
    }
    
    tcp::acceptor acceptor_;
    Cache& cache_;
};

#endif