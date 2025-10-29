#include <iostream>
#include <crow.h>
#include <csignal>

std::atomic<bool> running{true};

void signalHandler(int signal) {
    std::cout << "Received signal: " << signal << std::endl;
    running = false;
}

int main() {
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);
    
    crow::SimpleApp app;
    
    // 最简单的路由
    CROW_ROUTE(app, "/")
    ([]() {
        std::cout << "Root route called" << std::endl;
        return "Hello Root!";
    });
    
    CROW_ROUTE(app, "/test")
    ([]() {
        std::cout << "Test route called" << std::endl;
        return "Hello Test!";
    });
    
    CROW_ROUTE(app, "/").methods("POST"_method)
    ([](const crow::request& req) {
        std::cout << "POST received, body size: " << req.body.size() << std::endl;
        return crow::response(200, "POST OK");
    });
    
    CROW_ROUTE(app, "/<string>")
    ([](const std::string& key) {
        std::cout << "Key route called: " << key << std::endl;
        return "Key: " + key;
    });
    
    std::cout << "Starting server on port 8080..." << std::endl;
    
    try {
        // 修正：移除 multithreaded() 调用
        app.port(8080).run();
    } catch (std::exception& e) {
        std::cerr << "Server exception: " << e.what() << std::endl;
    }
    
    std::cout << "Server stopped" << std::endl;
    return 0;
}