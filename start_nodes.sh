#!/bin/bash
#!/bin/bash

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <total_nodes>"
    echo "Example: $0 3"
    exit 1
fi

total_nodes=$1

echo "=== 启动 $total_nodes 个 SDCS 节点 ==="

# 杀死现有进程（在 Docker 中可能不需要）
pkill -f sdcs 2>/dev/null || true
sleep 2

# 启动节点
echo "启动节点..."
for i in $(seq 1 $total_nodes); do
    echo "启动节点$i"
    ./sdcs $i $total_nodes &
done

echo "等待节点启动..."
sleep 5

# 检查节点状态
echo "检查节点状态:"
for i in $(seq 1 $total_nodes); do
    port=$((9526 + i))
    echo -n "节点$i (端口$port): "
    if curl -s http://127.0.0.1:$port/health > /dev/null 2>&1; then
        echo "✓ 运行中"
    else
        echo "✗ 启动失败"
    fi
done

echo "=== 所有节点启动完成 ==="

# 保持容器运行
echo "节点正在运行，按 Ctrl+C 停止..."
wait
