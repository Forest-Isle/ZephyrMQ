package com.zephyr.examples;

import com.zephyr.client.consumer.ConsumeOrderlyContext;
import com.zephyr.client.consumer.ConsumeOrderlyStatus;
import com.zephyr.client.consumer.DefaultZephyrPushConsumer;
import com.zephyr.client.consumer.MessageListener;
import com.zephyr.protocol.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ConsumerExample {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerExample.class);

    public static void main(String[] args) throws Exception {
        // Create consumer
        DefaultZephyrPushConsumer consumer = new DefaultZephyrPushConsumer("example_consumer_group");
        consumer.setNameserverAddresses("127.0.0.1:9877");

        // Register message listener
        consumer.registerMessageListener(new MessageListener() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                logger.info("Received {} messages from queue: {}", msgs.size(), context.getMessageQueue());

                for (MessageExt msg : msgs) {
                    try {
                        String body = new String(msg.getBody(), StandardCharsets.UTF_8);
                        logger.info("Consuming message: topic={}, tags={}, keys={}, body={}",
                                msg.getTopic(), msg.getTags(), msg.getKeys(), body);

                        // Simulate message processing
                        Thread.sleep(100);

                    } catch (Exception e) {
                        logger.error("Error processing message: " + msg.getMsgId(), e);
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    }
                }

                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        // Subscribe to topic
        consumer.subscribe("TestTopic", "*");

        // Start consumer
        consumer.start();

        logger.info("Consumer started, press any key to stop...");
        System.in.read();

        // Shutdown consumer
        consumer.shutdown();
        logger.info("Consumer stopped");
    }
}