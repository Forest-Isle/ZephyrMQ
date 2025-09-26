package com.zephyr.client.partition;

import com.zephyr.protocol.message.Message;
import com.zephyr.protocol.message.MessageQueue;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Round-robin message queue selector
 */
public class SelectMessageQueueByRoundRobin implements MessageQueueSelector {

    private final AtomicLong sendWhichQueue = new AtomicLong(0);

    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        if (mqs == null || mqs.isEmpty()) {
            return null;
        }

        long index = sendWhichQueue.getAndIncrement();
        int pos = (int) (Math.abs(index) % mqs.size());
        return mqs.get(pos);
    }
}