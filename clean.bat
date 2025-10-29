# 1. 停止所有容器
docker stop node1 node2 node3
docker rm node1 node2 node3

# 2. 重新构建镜像
docker build --no-cache -t dcs .

# 3. 重新运行容器
docker run -d --name node1 -p 127.0.0.1:9527:9527 dcs ./sdcs 1
docker run -d --name node2 -p 127.0.0.1:9528:9528 dcs ./sdcs 2
docker run -d --name node3 -p 127.0.0.1:9529:9529 dcs ./sdcs 3

# 4. 检查启动日志
docker logs node1

# 5. 测试 RPC 调用
C:\Windows\System32\curl.exe http://localhost:9527/age
C:\Windows\System32\curl.exe http://localhost:9527/tasks