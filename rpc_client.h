/*************************************************************************
	> File Name: rpc_client.h
	> Author: 
	> Mail: 
	> Created Time: Fri Oct  3 17:03:39 2025
 ************************************************************************/
// rpc_client.h
#ifndef RPC_CLIENT_H
#define RPC_CLIENT_H

#include <boost/asio.hpp>
#include <string>
#include <memory>
#include "rpc_protocol.h"
#include "json.hpp"

using boost::asio::ip::tcp;

class RpcClient {
public:
    RpcClient(boost::asio::io_context& io_context, const std::string& host, const std::string& port)
        : socket_(io_context){
        
        std::cout << "RpcClient connecting to " << host << ":" << port << std::endl;
        
        tcp::resolver resolver(io_context);  // 使用参数 io_context，而不是成员变量
        auto endpoints = resolver.resolve(host, port);
        boost::asio::connect(socket_, endpoints);
        
        std::cout << "RpcClient connected successfully" << std::endl;
    }
    
    std::string call(const RpcMessage& request) {
        sendRequest(request);
        return receiveResponse();
    }
    
private:
    void sendRequest(const RpcMessage& request) {
        // 发送header
        boost::asio::write(socket_, boost::asio::buffer(&request.header, sizeof(RpcHeader)));
        
        // 发送body
        if (request.header.length > 0) {
            boost::asio::write(socket_, boost::asio::buffer(request.body));
        }
    }
    
    std::string receiveResponse() {
        // 接收header
        RpcHeader header;
        boost::asio::read(socket_, boost::asio::buffer(&header, sizeof(RpcHeader)));
        
        // 接收body
        std::string body(header.length, 0);
        if (header.length > 0) {
            boost::asio::read(socket_, boost::asio::buffer(body));
        }
        
        return body;
    }
    
    tcp::socket socket_;  // 移除了 io_context_ 成员变量
};

#endif