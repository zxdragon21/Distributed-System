#!/bin/bash
#!/bin/bash

echo "=== 简单调试测试 ==="

# 测试基本写入
echo "1. 测试写入..."
curl -s -XPOST -H "Content-type: application/json" http://127.0.0.1:9526/ -d '{"test1": "value1"}' | jq .
curl -s -XPOST -H "Content-type: application/json" http://127.0.0.1:9527/ -d '{"test2": "value2"}' | jq .
curl -s -XPOST -H "Content-type: application/json" http://127.0.0.1:9528/ -d '{"test3": "value3"}' | jq .

echo ""
echo "2. 测试读取..."
curl -s http://127.0.0.1:9526/test1 | jq .
curl -s http://127.0.0.1:9527/test2 | jq .
curl -s http://127.0.0.1:9528/test3 | jq .

echo ""
echo "3. 测试跨节点读取..."
curl -s http://127.0.0.1:9527/test1 | jq .
curl -s http://127.0.0.1:9528/test2 | jq .
curl -s http://127.0.0.1:9526/test3 | jq .

echo ""
echo "=== 测试完成 ==="
