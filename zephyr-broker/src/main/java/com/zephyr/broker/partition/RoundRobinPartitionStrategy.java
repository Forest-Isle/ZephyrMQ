package com.zephyr.broker.partition;

import com.zephyr.protocol.message.Message;
import com.zephyr.protocol.message.MessageQueue;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Round-robin partition strategy
 * Distributes messages evenly across all available queues
 */
public class RoundRobinPartitionStrategy implements PartitionStrategy {

    private final AtomicLong sendWhichQueue = new AtomicLong(0);

    @Override
    public MessageQueue selectOne(Message message, List<MessageQueue> mqs, Object arg) {
        if (mqs == null || mqs.isEmpty()) {
            return null;
        }

        long index = sendWhichQueue.getAndIncrement();
        int pos = (int) (Math.abs(index) % mqs.size());
        return mqs.get(pos);
    }

    @Override
    public String getName() {
        return "RoundRobin";
    }
}