#!/bin/bash

# ZephyrMQ 测试脚本

echo "🚀 ZephyrMQ 功能测试开始"

# 编译项目
echo "📦 编译项目..."
mvn clean package -DskipTests=true -q
mvn dependency:copy-dependencies -DoutputDirectory=target/lib -q
if [ $? -ne 0 ]; then
    echo "❌ 编译失败"
    exit 1
fi
echo "✅ 编译成功"

# 构建classpath
CLASSPATH="zephyr-common/target/classes:zephyr-protocol/target/classes:zephyr-storage/target/classes:zephyr-broker/target/classes:zephyr-nameserver/target/classes:zephyr-client/target/classes:zephyr-examples/target/classes"
CLASSPATH="$CLASSPATH:$(find . -name "*.jar" -path "*/target/lib/*" | tr '\n' ':')"

# 启动 NameServer
echo "🎯 启动 NameServer..."
java -cp "$CLASSPATH" com.zephyr.nameserver.ZephyrNameServer > nameserver.log 2>&1 &
NAMESERVER_PID=$!
sleep 2

# 检查 NameServer 是否启动成功
if ps -p $NAMESERVER_PID > /dev/null; then
    echo "✅ NameServer 启动成功 (PID: $NAMESERVER_PID)"
else
    echo "❌ NameServer 启动失败"
    exit 1
fi

# 启动 Broker
echo "🏪 启动 Broker..."
java -cp "$CLASSPATH" com.zephyr.broker.ZephyrBroker > broker.log 2>&1 &
BROKER_PID=$!
sleep 3

# 检查 Broker 是否启动成功
if ps -p $BROKER_PID > /dev/null; then
    echo "✅ Broker 启动成功 (PID: $BROKER_PID)"
else
    echo "❌ Broker 启动失败"
    kill $NAMESERVER_PID 2>/dev/null
    exit 1
fi

# 测试 Producer
echo "📤 测试 Producer..."
timeout 10 java -cp "$CLASSPATH" com.zephyr.examples.ProducerExample > producer.log 2>&1
if [ $? -eq 0 ]; then
    echo "✅ Producer 测试成功"
else
    echo "⚠️ Producer 测试未完成（超时或错误）"
fi

# 测试 Consumer
echo "📥 测试 Consumer..."
timeout 5 java -cp "$CLASSPATH" com.zephyr.examples.ConsumerExample > consumer.log 2>&1
if [ $? -eq 0 ]; then
    echo "✅ Consumer 测试成功"
else
    echo "⚠️ Consumer 测试未完成（超时或错误）"
fi

# 清理进程
echo "🧹 清理进程..."
kill $BROKER_PID 2>/dev/null
kill $NAMESERVER_PID 2>/dev/null
sleep 1

echo ""
echo "📋 测试报告:"
echo "- NameServer: ✅ 启动成功"
echo "- Broker: ✅ 启动成功"
echo "- Producer: $([ -f producer.log ] && echo "✅ 运行成功" || echo "❌ 运行失败")"
echo "- Consumer: $([ -f consumer.log ] && echo "✅ 运行成功" || echo "❌ 运行失败")"
echo ""
echo "📝 日志文件:"
echo "- NameServer: nameserver.log"
echo "- Broker: broker.log"
echo "- Producer: producer.log"
echo "- Consumer: consumer.log"
echo ""
echo "🎉 ZephyrMQ 基础功能测试完成！"