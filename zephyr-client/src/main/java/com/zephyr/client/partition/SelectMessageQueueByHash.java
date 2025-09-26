package com.zephyr.client.partition;

import com.zephyr.protocol.message.Message;
import com.zephyr.protocol.message.MessageQueue;

import java.util.List;

/**
 * Hash-based message queue selector
 */
public class SelectMessageQueueByHash implements MessageQueueSelector {

    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        if (mqs == null || mqs.isEmpty()) {
            return null;
        }

        String shardingKey = msg.getKeys();
        if (shardingKey == null || shardingKey.isEmpty()) {
            // Use arg as sharding key if provided
            if (arg instanceof String) {
                shardingKey = (String) arg;
            } else {
                // Fallback to topic
                shardingKey = msg.getTopic();
            }
        }

        int hash = Math.abs(shardingKey.hashCode());
        int index = hash % mqs.size();
        return mqs.get(index);
    }
}