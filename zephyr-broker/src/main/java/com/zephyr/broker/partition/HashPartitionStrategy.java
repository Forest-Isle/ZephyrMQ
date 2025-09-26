package com.zephyr.broker.partition;

import com.zephyr.protocol.message.Message;
import com.zephyr.protocol.message.MessageQueue;

import java.util.List;

/**
 * Hash partition strategy
 * Routes messages based on hash of message key or topic
 */
public class HashPartitionStrategy implements PartitionStrategy {

    @Override
    public MessageQueue selectOne(Message message, List<MessageQueue> mqs, Object arg) {
        if (mqs == null || mqs.isEmpty()) {
            return null;
        }

        String shardingKey = message.getKeys();
        if (shardingKey == null || shardingKey.isEmpty()) {
            // Fallback to topic hash if no message key
            shardingKey = message.getTopic();
        }

        int hash = Math.abs(shardingKey.hashCode());
        int index = hash % mqs.size();
        return mqs.get(index);
    }

    @Override
    public String getName() {
        return "Hash";
    }
}