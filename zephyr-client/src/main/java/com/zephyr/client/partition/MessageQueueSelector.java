package com.zephyr.client.partition;

import com.zephyr.protocol.message.Message;
import com.zephyr.protocol.message.MessageQueue;

import java.util.List;

/**
 * Client-side message queue selector interface
 */
public interface MessageQueueSelector {

    /**
     * Select message queue for message
     *
     * @param mqs available message queues
     * @param msg message to be sent
     * @param arg additional argument for selection
     * @return selected message queue
     */
    MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg);
}