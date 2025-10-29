# Dockerfile
FROM ubuntu:20.04

# 设置时区以避免tzdata交互式安装
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 使用阿里云镜像源替换默认源
RUN sed -i 's/archive.ubuntu.com/mirrors.ustc.edu.cn/g' /etc/apt/sources.list && \
    sed -i 's/security.ubuntu.com/mirrors.ustc.edu.cn/g' /etc/apt/sources.list

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    wget \
    curl \
    jq \
    libmicrohttpd-dev \
    nlohmann-json3-dev \
    libssl-dev \
    libzmq3-dev \
    libboost-system-dev \
    libboost-thread-dev \
    libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 依赖库已在安装步骤中处理

# 拷贝源代码
COPY . .

# 编译项目
RUN g++ -std=c++17 -pthread -O3 -I. -I/usr/local/include/workflow main.cpp -o sdcs -lmicrohttpd -lpthread -lcurl

# 使用现有的启动脚本并传入启动节点数

