/*************************************************************************
	> File Name: rpc_protocol.h
	> Author: 
	> Mail: 
	> Created Time: Fri Oct  3 17:03:17 2025
 ************************************************************************/
// rpc_protocol.h
#ifndef RPC_PROTOCOL_H
#define RPC_PROTOCOL_H

#include <string>
#include <cstdint>

enum class RpcType : uint8_t {
    GET = 0,
    SET = 1,
    DELETE = 2,
    RESPONSE = 3
};

struct RpcHeader {
    uint32_t length;    // 消息体长度
    RpcType type;       // 消息类型
    uint32_t request_id;// 请求ID
};

class RpcMessage {
public:
    RpcHeader header;
    std::string body;
    
    static RpcMessage createRequest(RpcType type, uint32_t request_id, const std::string& body = "") {
        RpcMessage msg;
        msg.header.type = type;
        msg.header.request_id = request_id;
        msg.header.length = body.length();
        msg.body = body;
        return msg;
    }
    
    static RpcMessage createResponse(uint32_t request_id, const std::string& body = "") {
        return createRequest(RpcType::RESPONSE, request_id, body);
    }
};

#endif
