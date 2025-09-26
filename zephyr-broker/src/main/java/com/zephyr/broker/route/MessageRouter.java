package com.zephyr.broker.route;

import com.zephyr.broker.partition.PartitionStrategy;
import com.zephyr.broker.partition.PartitionStrategyManager;
import com.zephyr.broker.topic.TopicConfigManager;
import com.zephyr.protocol.message.Message;
import com.zephyr.protocol.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Message router for selecting appropriate message queue
 */
public class MessageRouter {

    private static final Logger logger = LoggerFactory.getLogger(MessageRouter.class);

    private final TopicConfigManager topicConfigManager;
    private final PartitionStrategyManager partitionStrategyManager;

    public MessageRouter(TopicConfigManager topicConfigManager,
                        PartitionStrategyManager partitionStrategyManager) {
        this.topicConfigManager = topicConfigManager;
        this.partitionStrategyManager = partitionStrategyManager;
    }

    /**
     * Route message to appropriate message queue
     *
     * @param message the message to route
     * @param strategyName partition strategy name (optional)
     * @param arg additional routing argument
     * @return selected message queue
     */
    public MessageQueue route(Message message, String strategyName, Object arg) {
        if (message == null || message.getTopic() == null) {
            logger.warn("Invalid message or topic is null");
            return null;
        }

        String topic = message.getTopic();

        // Get topic configuration
        TopicConfigManager.TopicConfig topicConfig = topicConfigManager.getTopicConfig(topic);
        if (topicConfig == null) {
            logger.warn("Topic config not found: {}", topic);
            return null;
        }

        // Build message queue list for the topic
        List<MessageQueue> messageQueues = buildMessageQueueList(topic, topicConfig);
        if (messageQueues.isEmpty()) {
            logger.warn("No available message queues for topic: {}", topic);
            return null;
        }

        // Select partition strategy
        PartitionStrategy strategy = partitionStrategyManager.getStrategy(strategyName);

        // Route message to queue
        MessageQueue selectedQueue = strategy.selectOne(message, messageQueues, arg);

        if (logger.isDebugEnabled()) {
            logger.debug("Routed message to queue: topic={}, queue={}, strategy={}",
                        topic, selectedQueue != null ? selectedQueue.getQueueId() : "null",
                        strategy.getName());
        }

        return selectedQueue;
    }

    /**
     * Route message using default strategy
     *
     * @param message the message to route
     * @return selected message queue
     */
    public MessageQueue route(Message message) {
        return route(message, null, null);
    }

    /**
     * Route message with custom argument
     *
     * @param message the message to route
     * @param arg custom routing argument
     * @return selected message queue
     */
    public MessageQueue route(Message message, Object arg) {
        return route(message, null, arg);
    }

    /**
     * Get all available message queues for a topic
     *
     * @param topic topic name
     * @return list of message queues
     */
    public List<MessageQueue> getTopicMessageQueues(String topic) {
        TopicConfigManager.TopicConfig topicConfig = topicConfigManager.getTopicConfig(topic);
        if (topicConfig == null) {
            return new ArrayList<>();
        }
        return buildMessageQueueList(topic, topicConfig);
    }

    /**
     * Build message queue list for a topic
     *
     * @param topic topic name
     * @param topicConfig topic configuration
     * @return list of message queues
     */
    private List<MessageQueue> buildMessageQueueList(String topic, TopicConfigManager.TopicConfig topicConfig) {
        List<MessageQueue> messageQueues = new ArrayList<>();

        int writeQueueNums = topicConfig.getWriteQueueNums();
        String brokerName = "defaultBroker"; // TODO: Get actual broker name from config

        for (int i = 0; i < writeQueueNums; i++) {
            MessageQueue mq = new MessageQueue(topic, brokerName, i);
            messageQueues.add(mq);
        }

        return messageQueues;
    }
}