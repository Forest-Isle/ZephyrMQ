#!/bin/bash

# ZephyrMQ 示例运行脚本

echo "=== ZephyrMQ Example Runner ==="
echo

case "$1" in
    "producer" | "p")
        echo "Running Producer Example..."
        mvn exec:java -pl zephyr-examples -Dexec.mainClass="com.zephyr.examples.ProducerExample" -q
        ;;
    "consumer" | "c")
        echo "Running Consumer Example..."
        echo "Press Ctrl+C to stop the consumer"
        mvn exec:java -pl zephyr-examples -Dexec.mainClass="com.zephyr.examples.ConsumerExample"
        ;;
    "build" | "b")
        echo "Building ZephyrMQ..."
        mvn clean install -DskipTests
        ;;
    "compile")
        echo "Compiling ZephyrMQ..."
        mvn clean compile
        ;;
    *)
        echo "Usage: $0 {producer|consumer|build|compile}"
        echo
        echo "Commands:"
        echo "  producer, p  - Run producer example"
        echo "  consumer, c  - Run consumer example"
        echo "  build, b     - Build and install all modules"
        echo "  compile      - Compile all modules"
        echo
        echo "Examples:"
        echo "  $0 producer"
        echo "  $0 consumer"
        echo "  $0 build"
        exit 1
        ;;
esac