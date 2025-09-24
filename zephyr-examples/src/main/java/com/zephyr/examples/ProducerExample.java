package com.zephyr.examples;

import com.zephyr.client.producer.DefaultZephyrProducer;
import com.zephyr.client.producer.SendCallback;
import com.zephyr.protocol.message.Message;
import com.zephyr.protocol.message.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class ProducerExample {

    private static final Logger logger = LoggerFactory.getLogger(ProducerExample.class);

    public static void main(String[] args) throws Exception {
        // Create producer
        DefaultZephyrProducer producer = new DefaultZephyrProducer("example_producer_group");
        producer.setNameserverAddresses("127.0.0.1:9877");

        // Start producer
        producer.start();

        try {
            // Send sync message
            sendSyncMessage(producer);

            // Send async message
            sendAsyncMessage(producer);

            // Send oneway message
            sendOnewayMessage(producer);

        } finally {
            // Shutdown producer
            producer.shutdown();
        }
    }

    private static void sendSyncMessage(DefaultZephyrProducer producer) throws Exception {
        logger.info("=== Sending sync message ===");

        Message message = new Message("TestTopic", "TagA", "Hello ZephyrMQ - Sync Message".getBytes(StandardCharsets.UTF_8));
        message.setKeys("KEY_SYNC_001");

        SendResult sendResult = producer.send(message);
        logger.info("Sync send result: {}", sendResult);
    }

    private static void sendAsyncMessage(DefaultZephyrProducer producer) throws Exception {
        logger.info("=== Sending async message ===");

        Message message = new Message("TestTopic", "TagB", "Hello ZephyrMQ - Async Message".getBytes(StandardCharsets.UTF_8));
        message.setKeys("KEY_ASYNC_001");

        producer.sendAsync(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                logger.info("Async send success: {}", sendResult);
            }

            @Override
            public void onException(Throwable e) {
                logger.error("Async send failed", e);
            }
        });

        // Wait a moment for async callback
        Thread.sleep(1000);
    }

    private static void sendOnewayMessage(DefaultZephyrProducer producer) throws Exception {
        logger.info("=== Sending oneway message ===");

        Message message = new Message("TestTopic", "TagC", "Hello ZephyrMQ - Oneway Message".getBytes(StandardCharsets.UTF_8));
        message.setKeys("KEY_ONEWAY_001");

        producer.sendOneway(message);
        logger.info("Oneway message sent");
    }
}