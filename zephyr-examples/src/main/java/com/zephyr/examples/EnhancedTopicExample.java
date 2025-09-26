package com.zephyr.examples;

import com.zephyr.broker.partition.PartitionStrategyManager;
import com.zephyr.broker.partition.HashPartitionStrategy;
import com.zephyr.broker.route.MessageRouter;
import com.zephyr.broker.topic.TopicConfigManager;
import com.zephyr.broker.topic.TopicService;
import com.zephyr.broker.loadbalance.LoadBalanceManager;
import com.zephyr.client.producer.DefaultZephyrProducer;
import com.zephyr.client.partition.SelectMessageQueueByHash;
import com.zephyr.common.config.BrokerConfig;
import com.zephyr.protocol.message.Message;
import com.zephyr.protocol.message.MessageQueue;
import com.zephyr.protocol.message.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Enhanced example demonstrating new Topic and Partition features
 */
public class EnhancedTopicExample {

    private static final Logger logger = LoggerFactory.getLogger(EnhancedTopicExample.class);

    public static void main(String[] args) {
        logger.info("Starting Enhanced Topic and Partition Example...");

        try {
            // Initialize broker components
            BrokerConfig brokerConfig = new BrokerConfig();
            brokerConfig.setBrokerName("example-broker");
            brokerConfig.setStorePathRootDir("./data");

            TopicConfigManager topicConfigManager = new TopicConfigManager(brokerConfig);
            topicConfigManager.start();

            TopicService topicService = new TopicService(topicConfigManager, brokerConfig);

            // Create topics
            createTopics(topicService);

            // Demonstrate partition strategies
            demonstratePartitionStrategies(topicConfigManager);

            // Demonstrate load balancing
            demonstrateLoadBalancing();

            // Demonstrate client integration
            demonstrateClientIntegration();

            // Cleanup
            topicConfigManager.shutdown();

            logger.info("Enhanced Topic and Partition Example completed successfully!");

        } catch (Exception e) {
            logger.error("Example execution failed", e);
        }
    }

    private static void createTopics(TopicService topicService) {
        logger.info("=== Creating Topics ===");

        // Create order topic with 8 queues
        TopicService.TopicCreateRequest orderRequest = new TopicService.TopicCreateRequest();
        orderRequest.setTopicName("OrderTopic");
        orderRequest.setReadQueueNums(8);
        orderRequest.setWriteQueueNums(8);
        orderRequest.setOrder(true);

        TopicService.TopicCreateResult result = topicService.createTopic(orderRequest);
        logger.info("Create OrderTopic: {}", result.isSuccess() ? "SUCCESS" : result.getMessage());

        // Create payment topic with 4 queues
        TopicService.TopicCreateRequest paymentRequest = new TopicService.TopicCreateRequest();
        paymentRequest.setTopicName("PaymentTopic");
        paymentRequest.setReadQueueNums(4);
        paymentRequest.setWriteQueueNums(4);

        result = topicService.createTopic(paymentRequest);
        logger.info("Create PaymentTopic: {}", result.isSuccess() ? "SUCCESS" : result.getMessage());

        // List all topics
        List<String> topics = topicService.listTopics();
        logger.info("Available topics: {}", topics);

        // Query topic information
        TopicService.TopicInfo orderInfo = topicService.queryTopic("OrderTopic");
        if (orderInfo != null) {
            logger.info("OrderTopic info - ReadQueues: {}, WriteQueues: {}, Order: {}",
                       orderInfo.getReadQueueNums(), orderInfo.getWriteQueueNums(), orderInfo.isOrder());
        }
    }

    private static void demonstratePartitionStrategies(TopicConfigManager topicConfigManager) {
        logger.info("=== Demonstrating Partition Strategies ===");

        PartitionStrategyManager strategyManager = new PartitionStrategyManager();
        MessageRouter router = new MessageRouter(topicConfigManager, strategyManager);

        // Test different messages with different strategies
        Message[] testMessages = {
            createTestMessage("OrderTopic", "user123", "order-001"),
            createTestMessage("OrderTopic", "user456", "order-002"),
            createTestMessage("OrderTopic", "user123", "order-003"), // Same key as first
            createTestMessage("PaymentTopic", "payment-1", "payment-data")
        };

        // Test Round Robin
        logger.info("--- Round Robin Strategy ---");
        for (int i = 0; i < testMessages.length; i++) {
            MessageQueue mq = router.route(testMessages[i], "RoundRobin", null);
            logger.info("Message {} -> Queue: {}", i, mq != null ? mq.getQueueId() : "null");
        }

        // Test Hash Strategy
        logger.info("--- Hash Strategy ---");
        for (int i = 0; i < testMessages.length; i++) {
            MessageQueue mq = router.route(testMessages[i], "Hash", null);
            logger.info("Message {} (key: {}) -> Queue: {}",
                       i, testMessages[i].getKeys(), mq != null ? mq.getQueueId() : "null");
        }

        // Show that same key goes to same queue
        logger.info("--- Hash Consistency Test ---");
        for (int i = 0; i < 3; i++) {
            Message msg = createTestMessage("OrderTopic", "consistent-key", "data-" + i);
            MessageQueue mq = router.route(msg, "Hash", null);
            logger.info("Consistent key message {} -> Queue: {}", i, mq != null ? mq.getQueueId() : "null");
        }
    }

    private static void demonstrateLoadBalancing() {
        logger.info("=== Demonstrating Load Balancing ===");

        LoadBalanceManager loadBalanceManager = new LoadBalanceManager();

        // Create message queues for topics
        List<MessageQueue> orderQueues = List.of(
            new MessageQueue("OrderTopic", "broker-a", 0),
            new MessageQueue("OrderTopic", "broker-a", 1),
            new MessageQueue("OrderTopic", "broker-a", 2),
            new MessageQueue("OrderTopic", "broker-a", 3)
        );

        loadBalanceManager.updateTopicSubscribeInfo("OrderTopic", orderQueues);

        // Register consumers in a group
        String consumerGroup = "order-consumer-group";
        loadBalanceManager.registerConsumer(consumerGroup, "consumer-1");
        loadBalanceManager.registerConsumer(consumerGroup, "consumer-2");
        loadBalanceManager.registerConsumer(consumerGroup, "consumer-3");

        // Show different allocation strategies
        logger.info("--- Average Allocation ---");
        for (String consumerId : List.of("consumer-1", "consumer-2", "consumer-3")) {
            List<MessageQueue> allocated = loadBalanceManager.allocateMessageQueues(
                consumerGroup, consumerId, "OrderTopic", "AVG");
            logger.info("Consumer {} allocated queues: {}", consumerId,
                       allocated.stream().map(mq -> String.valueOf(mq.getQueueId())).toList());
        }

        logger.info("--- Hash Allocation ---");
        for (String consumerId : List.of("consumer-1", "consumer-2", "consumer-3")) {
            List<MessageQueue> allocated = loadBalanceManager.allocateMessageQueues(
                consumerGroup, consumerId, "OrderTopic", "HASH");
            logger.info("Consumer {} allocated queues: {}", consumerId,
                       allocated.stream().map(mq -> String.valueOf(mq.getQueueId())).toList());
        }

        // Show stats
        LoadBalanceManager.ConsumerGroupStats stats = loadBalanceManager.getConsumerGroupStats();
        logger.info("Load balance stats - Groups: {}, Consumers: {}, Topics: {}",
                   stats.getTotalGroups(), stats.getTotalConsumers(), stats.getTotalTopics());
    }

    private static void demonstrateClientIntegration() throws Exception {
        logger.info("=== Demonstrating Client Integration ===");

        DefaultZephyrProducer producer = new DefaultZephyrProducer("enhanced-producer");
        producer.start();

        try {
            // Send messages using default selector (Round Robin)
            logger.info("--- Sending with Default Selector ---");
            for (int i = 0; i < 5; i++) {
                Message msg = createTestMessage("TestTopic", "key-" + i, "message-" + i);
                SendResult result = producer.send(msg);
                logger.info("Sent message {} to queue: {}", i, result.getMessageQueue().getQueueId());
            }

            // Send messages using Hash selector
            logger.info("--- Sending with Hash Selector ---");
            SelectMessageQueueByHash hashSelector = new SelectMessageQueueByHash();
            for (int i = 0; i < 5; i++) {
                Message msg = createTestMessage("TestTopic", "hash-key", "hash-message-" + i);
                SendResult result = producer.send(msg, hashSelector, null);
                logger.info("Sent hash message {} to queue: {}", i, result.getMessageQueue().getQueueId());
            }

            // Show topic route info
            List<MessageQueue> queues = producer.fetchPublishMessageQueues("TestTopic");
            logger.info("Available queues for TestTopic: {}",
                       queues.stream().map(mq -> String.valueOf(mq.getQueueId())).toList());

        } finally {
            producer.shutdown();
        }
    }

    private static Message createTestMessage(String topic, String key, String content) {
        Message message = new Message();
        message.setTopic(topic);
        message.setKeys(key);
        message.setBody(content.getBytes());
        return message;
    }
}