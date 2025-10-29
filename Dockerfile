# Dockerfile
FROM ubuntu:20.04

# 设置时区以避免tzdata交互式安装
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 使用阿里云镜像源替换默认源
RUN sed -i 's/archive.ubuntu.com/mirrors.aliyun.com/g' /etc/apt/sources.list && \
    sed -i 's/security.ubuntu.com/mirrors.aliyun.com/g' /etc/apt/sources.list

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libboost-system-dev \
    libboost-thread-dev \
    libasio-dev \
    wget \
    curl \
    jq \
    nlohmann-json3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 下载并编译crow
RUN git clone https://github.com/CrowCpp/Crow.git && \
    cd Crow && \
    mkdir build && cd build && \
    cmake .. -DCROW_BUILD_EXAMPLES=OFF -DCROW_BUILD_TESTS=OFF && \
    make && make install

# 拷贝源代码
COPY . .

# 编译项目
RUN g++ -std=c++17 -pthread -I. -I Crow/include main.cpp -o sdcs -lboost_system -lboost_thread -lpthread

# 使用现有的启动脚本并传入启动节点数
# CMD ["./sdcs","1"]

