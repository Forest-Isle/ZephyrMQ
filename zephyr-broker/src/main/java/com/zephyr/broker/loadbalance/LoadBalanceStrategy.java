package com.zephyr.broker.loadbalance;

import com.zephyr.protocol.message.MessageQueue;

import java.util.List;

/**
 * Load balance strategy interface for consumer group
 */
public interface LoadBalanceStrategy {

    /**
     * Allocate message queues to consumers
     *
     * @param consumerGroup consumer group name
     * @param currentCID current consumer ID
     * @param mqAll all available message queues
     * @param cidAll all consumer IDs in the group
     * @return allocated message queues for current consumer
     */
    List<MessageQueue> allocate(String consumerGroup,
                               String currentCID,
                               List<MessageQueue> mqAll,
                               List<String> cidAll);

    /**
     * Get strategy name
     *
     * @return strategy name
     */
    String getName();
}