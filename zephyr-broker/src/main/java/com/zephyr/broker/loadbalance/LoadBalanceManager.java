package com.zephyr.broker.loadbalance;

import com.zephyr.protocol.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Load balance manager for consumer groups
 * Handles consumer group rebalancing and queue allocation
 */
public class LoadBalanceManager {

    private static final Logger logger = LoggerFactory.getLogger(LoadBalanceManager.class);

    private final ConcurrentMap<String, LoadBalanceStrategy> strategies = new ConcurrentHashMap<>();
    private LoadBalanceStrategy defaultStrategy;

    // Consumer group information
    private final ConcurrentMap<String, List<String>> consumerGroupTable = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, List<MessageQueue>> topicSubscribeTable = new ConcurrentHashMap<>();

    public LoadBalanceManager() {
        // Register built-in strategies
        registerStrategy(new AllocateMessageQueueAveragely());
        registerStrategy(new AllocateMessageQueueConsistentHash());
        registerStrategy(new AllocateMessageQueueByConfig());

        // Set default strategy
        this.defaultStrategy = strategies.get("AVG");
        logger.info("LoadBalanceManager initialized with {} strategies", strategies.size());
    }

    /**
     * Register a load balance strategy
     *
     * @param strategy the strategy to register
     */
    public void registerStrategy(LoadBalanceStrategy strategy) {
        if (strategy != null && strategy.getName() != null) {
            strategies.put(strategy.getName(), strategy);
            logger.debug("Registered load balance strategy: {}", strategy.getName());
        }
    }

    /**
     * Get load balance strategy by name
     *
     * @param strategyName strategy name
     * @return load balance strategy, or default strategy if not found
     */
    public LoadBalanceStrategy getStrategy(String strategyName) {
        if (strategyName == null || strategyName.isEmpty()) {
            return defaultStrategy;
        }

        LoadBalanceStrategy strategy = strategies.get(strategyName);
        return strategy != null ? strategy : defaultStrategy;
    }

    /**
     * Set default load balance strategy
     *
     * @param strategyName strategy name
     */
    public void setDefaultStrategy(String strategyName) {
        LoadBalanceStrategy strategy = strategies.get(strategyName);
        if (strategy != null) {
            this.defaultStrategy = strategy;
            logger.info("Default load balance strategy set to: {}", strategyName);
        } else {
            logger.warn("Strategy not found: {}, keeping current default", strategyName);
        }
    }

    /**
     * Register consumer to group
     *
     * @param consumerGroup consumer group name
     * @param consumerId consumer ID
     */
    public void registerConsumer(String consumerGroup, String consumerId) {
        if (consumerGroup == null || consumerId == null) {
            return;
        }

        consumerGroupTable.computeIfAbsent(consumerGroup, k -> new ArrayList<>());
        List<String> consumers = consumerGroupTable.get(consumerGroup);

        synchronized (consumers) {
            if (!consumers.contains(consumerId)) {
                consumers.add(consumerId);
                logger.info("Registered consumer {} to group {}", consumerId, consumerGroup);

                // Trigger rebalance
                triggerRebalance(consumerGroup);
            }
        }
    }

    /**
     * Unregister consumer from group
     *
     * @param consumerGroup consumer group name
     * @param consumerId consumer ID
     */
    public void unregisterConsumer(String consumerGroup, String consumerId) {
        if (consumerGroup == null || consumerId == null) {
            return;
        }

        List<String> consumers = consumerGroupTable.get(consumerGroup);
        if (consumers != null) {
            synchronized (consumers) {
                if (consumers.remove(consumerId)) {
                    logger.info("Unregistered consumer {} from group {}", consumerId, consumerGroup);

                    // Clean up empty groups
                    if (consumers.isEmpty()) {
                        consumerGroupTable.remove(consumerGroup);
                        logger.info("Removed empty consumer group: {}", consumerGroup);
                    } else {
                        // Trigger rebalance
                        triggerRebalance(consumerGroup);
                    }
                }
            }
        }
    }

    /**
     * Allocate message queues to consumer
     *
     * @param consumerGroup consumer group name
     * @param consumerId consumer ID
     * @param topic topic name
     * @param strategyName load balance strategy name (optional)
     * @return allocated message queues
     */
    public List<MessageQueue> allocateMessageQueues(String consumerGroup,
                                                   String consumerId,
                                                   String topic,
                                                   String strategyName) {
        if (consumerGroup == null || consumerId == null || topic == null) {
            logger.warn("Invalid parameters for queue allocation");
            return new ArrayList<>();
        }

        List<String> consumers = consumerGroupTable.get(consumerGroup);
        if (consumers == null || consumers.isEmpty()) {
            logger.warn("No consumers found in group: {}", consumerGroup);
            return new ArrayList<>();
        }

        List<MessageQueue> mqAll = topicSubscribeTable.get(topic);
        if (mqAll == null || mqAll.isEmpty()) {
            logger.warn("No message queues found for topic: {}", topic);
            return new ArrayList<>();
        }

        // Get strategy
        LoadBalanceStrategy strategy = getStrategy(strategyName);

        // Allocate queues
        List<MessageQueue> allocatedQueues = strategy.allocate(consumerGroup, consumerId, mqAll, new ArrayList<>(consumers));

        logger.debug("Allocated {} queues to consumer {} in group {} for topic {}",
                    allocatedQueues.size(), consumerId, consumerGroup, topic);

        return allocatedQueues;
    }

    /**
     * Update topic message queues
     *
     * @param topic topic name
     * @param messageQueues message queues for the topic
     */
    public void updateTopicSubscribeInfo(String topic, List<MessageQueue> messageQueues) {
        if (topic != null && messageQueues != null) {
            topicSubscribeTable.put(topic, new ArrayList<>(messageQueues));
            logger.debug("Updated topic subscribe info for topic: {} with {} queues", topic, messageQueues.size());

            // Trigger rebalance for all groups subscribed to this topic
            triggerTopicRebalance(topic);
        }
    }

    /**
     * Get consumer list for group
     *
     * @param consumerGroup consumer group name
     * @return list of consumer IDs
     */
    public List<String> getConsumerList(String consumerGroup) {
        List<String> consumers = consumerGroupTable.get(consumerGroup);
        return consumers != null ? new ArrayList<>(consumers) : new ArrayList<>();
    }

    /**
     * Get message queues for topic
     *
     * @param topic topic name
     * @return list of message queues
     */
    public List<MessageQueue> getTopicMessageQueues(String topic) {
        List<MessageQueue> queues = topicSubscribeTable.get(topic);
        return queues != null ? new ArrayList<>(queues) : new ArrayList<>();
    }

    /**
     * Trigger rebalance for consumer group
     *
     * @param consumerGroup consumer group name
     */
    private void triggerRebalance(String consumerGroup) {
        logger.info("Triggering rebalance for consumer group: {}", consumerGroup);
        // TODO: Implement rebalance notification mechanism
        // This could involve notifying all consumers in the group to rebalance
    }

    /**
     * Trigger rebalance for all groups subscribed to a topic
     *
     * @param topic topic name
     */
    private void triggerTopicRebalance(String topic) {
        logger.debug("Triggering rebalance for topic: {}", topic);
        // TODO: Find all groups subscribed to this topic and trigger rebalance
    }

    /**
     * Get all available strategy names
     *
     * @return strategy names
     */
    public String[] getAvailableStrategies() {
        return strategies.keySet().toArray(new String[0]);
    }

    /**
     * Get consumer group statistics
     *
     * @return consumer group stats
     */
    public ConsumerGroupStats getConsumerGroupStats() {
        ConsumerGroupStats stats = new ConsumerGroupStats();
        stats.setTotalGroups(consumerGroupTable.size());
        stats.setTotalConsumers(consumerGroupTable.values().stream()
                                               .mapToInt(List::size)
                                               .sum());
        stats.setTotalTopics(topicSubscribeTable.size());
        return stats;
    }

    // Statistics class
    public static class ConsumerGroupStats {
        private int totalGroups;
        private int totalConsumers;
        private int totalTopics;

        public int getTotalGroups() { return totalGroups; }
        public void setTotalGroups(int totalGroups) { this.totalGroups = totalGroups; }
        public int getTotalConsumers() { return totalConsumers; }
        public void setTotalConsumers(int totalConsumers) { this.totalConsumers = totalConsumers; }
        public int getTotalTopics() { return totalTopics; }
        public void setTotalTopics(int totalTopics) { this.totalTopics = totalTopics; }
    }
}