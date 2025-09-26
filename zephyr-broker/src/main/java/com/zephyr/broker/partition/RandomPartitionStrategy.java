package com.zephyr.broker.partition;

import com.zephyr.protocol.message.Message;
import com.zephyr.protocol.message.MessageQueue;

import java.util.List;
import java.util.Random;

/**
 * Random partition strategy
 * Randomly selects a queue for message routing
 */
public class RandomPartitionStrategy implements PartitionStrategy {

    private final Random random = new Random();

    @Override
    public MessageQueue selectOne(Message message, List<MessageQueue> mqs, Object arg) {
        if (mqs == null || mqs.isEmpty()) {
            return null;
        }

        int index = random.nextInt(mqs.size());
        return mqs.get(index);
    }

    @Override
    public String getName() {
        return "Random";
    }
}