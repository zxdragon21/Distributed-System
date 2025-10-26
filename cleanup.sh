#!/bin/bash
#!/bin/bash

echo "=== 清理进程和端口 ==="

# 杀死相关进程
echo "杀死 sdcs 进程..."
sudo pkill -f sdcs 2>/dev/null
sudo pkill -f simple_server 2>/dev/null

# 使用 fuser 清理端口
echo "清理端口..."
for port in 8080 8081 8082 8088 9080 9081 9082; do
    if sudo fuser $port/tcp 2>/dev/null; then
        echo "杀死占用端口 $port 的进程"
        sudo fuser -k $port/tcp 2>/dev/null
    fi
done

# 等待清理完成
sleep 2

# 检查是否清理干净
echo "检查剩余进程..."
ps aux | grep -E '(sdcs|simple_server)' | grep -v grep

echo "检查端口占用..."
ss -tlnp | grep -E ':(8080|8081|8082|8088)'

echo "=== 清理完成 ==="
