package com.zephyr.broker.partition;

import com.zephyr.protocol.message.Message;
import com.zephyr.protocol.message.MessageQueue;

import java.util.List;

/**
 * Partition strategy interface for message routing
 */
public interface PartitionStrategy {

    /**
     * Select message queue based on message content
     *
     * @param message the message to be sent
     * @param mqs available message queues for the topic
     * @param arg additional argument for partition selection
     * @return selected message queue
     */
    MessageQueue selectOne(Message message, List<MessageQueue> mqs, Object arg);

    /**
     * Get strategy name
     *
     * @return strategy name
     */
    String getName();
}