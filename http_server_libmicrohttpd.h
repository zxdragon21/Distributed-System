#ifndef HTTP_SERVER_LIBMICROHTTPD_H
#define HTTP_SERVER_LIBMICROHTTPD_H

// #include <iostream>  // 不再需要的头文件
#include <string>
#include <functional>
#include <cstring>
#include <microhttpd.h>
#include <nlohmann/json.hpp>

// HTTP请求处理函数类型
typedef std::function<void(const std::string&, const std::string&, const std::string&, const std::string&, std::string&, int&)> RequestHandler;

class HttpServerLibmicrohttpd {
private:
    uint16_t port_;
    struct MHD_Daemon* daemon_;
    RequestHandler request_handler_;

    // 静态回调函数用于MHD框架
    static int staticHandlerCallback(void* cls, struct MHD_Connection* connection,
                                   const char* url, const char* method, const char* version,
                                   const char* upload_data, size_t* upload_data_size, void** con_cls) {
        HttpServerLibmicrohttpd* server = static_cast<HttpServerLibmicrohttpd*>(cls);
        return server->handleConnection(connection, url, method, version, upload_data, upload_data_size, con_cls);
    }

    // 处理HTTP连接的非静态方法
    int handleConnection(struct MHD_Connection* connection, const char* url, const char* method,
                        const char* version, const char* upload_data, size_t* upload_data_size, void** con_cls) {
        // 首次调用时初始化上下文
        if (*con_cls == nullptr) {
            std::string* request_body = new std::string();
            *con_cls = request_body;
            return MHD_YES;
        }

        // 收集请求体数据
        std::string* request_body = static_cast<std::string*>(*con_cls);
        if (*upload_data_size > 0) {
            request_body->append(upload_data, *upload_data_size);
            *upload_data_size = 0;
            return MHD_YES;
        }

        // 请求处理完成，准备响应
        std::string response_body;
        int status_code = 200;

        // 调用用户定义的请求处理器
        request_handler_(url, method, *request_body, version, response_body, status_code);
        
        // 特殊处理404响应，确保返回空响应体且不设置Content-Type
        if (status_code == 404) {
            response_body = "";
            
            // 创建响应
            struct MHD_Response* response = MHD_create_response_from_buffer(
                0, nullptr, MHD_RESPMEM_PERSISTENT);
                
            // 发送响应
            int ret = MHD_queue_response(connection, status_code, response);
            MHD_destroy_response(response);

            // 清理
            delete request_body;
            *con_cls = nullptr;

            return ret;
        }
        
        // 创建响应
        struct MHD_Response* response = MHD_create_response_from_buffer(
            response_body.size(), (void*)response_body.c_str(), MHD_RESPMEM_MUST_COPY);

        // 设置Content-Type为JSON
        MHD_add_response_header(response, "Content-Type", "application/json");

        // 发送响应
        int ret = MHD_queue_response(connection, status_code, response);
        MHD_destroy_response(response);

        // 清理
        delete request_body;
        *con_cls = nullptr;

        return ret;
    }

public:
    HttpServerLibmicrohttpd(uint16_t port, RequestHandler handler)
        : port_(port), daemon_(nullptr), request_handler_(std::move(handler)) {
    }

    ~HttpServerLibmicrohttpd() {
        stop();
    }

    bool start() {
        // 创建MHD守护进程
        // 优化线程池配置以提高并发性能
        daemon_ = MHD_start_daemon(
            MHD_USE_INTERNAL_POLLING_THREAD | MHD_USE_EPOLL | MHD_USE_TCP_FASTOPEN,
            port_,
            nullptr, nullptr,
            &HttpServerLibmicrohttpd::staticHandlerCallback,
            this,
            MHD_OPTION_THREAD_POOL_SIZE, 16,  // 增加线程池大小到16以提高并发
            MHD_OPTION_CONNECTION_TIMEOUT, 5,  // 减少连接超时到5秒
            MHD_OPTION_LISTENING_ADDRESS_REUSE, 1,  // 允许地址重用
            MHD_OPTION_SOCK_ADDR, nullptr,  // 监听所有接口
            MHD_OPTION_END);

        return (daemon_ != nullptr);
    }

    void stop() {
        if (daemon_ != nullptr) {
            MHD_stop_daemon(daemon_);
            daemon_ = nullptr;
        }
    }
};

#endif // HTTP_SERVER_LIBMICROHTTPD_H