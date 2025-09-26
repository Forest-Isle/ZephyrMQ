package com.zephyr.broker.delay;

import com.zephyr.protocol.message.Message;
import com.zephyr.protocol.message.MessageQueue;
import com.zephyr.broker.store.MessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Delay Message Manager
 * Manages delayed message delivery using time wheel scheduler
 */
public class DelayMessageManager {

    private static final Logger logger = LoggerFactory.getLogger(DelayMessageManager.class);

    private final TimeWheelScheduler scheduler;
    private final MessageStore messageStore;

    // Delay levels mapping: level -> delay time in milliseconds
    private static final long[] DELAY_LEVELS = {
        1000,     // Level 1: 1 second
        5000,     // Level 2: 5 seconds
        10000,    // Level 3: 10 seconds
        30000,    // Level 4: 30 seconds
        60000,    // Level 5: 1 minute
        120000,   // Level 6: 2 minutes
        180000,   // Level 7: 3 minutes
        240000,   // Level 8: 4 minutes
        300000,   // Level 9: 5 minutes
        360000,   // Level 10: 6 minutes
        420000,   // Level 11: 7 minutes
        480000,   // Level 12: 8 minutes
        540000,   // Level 13: 9 minutes
        600000,   // Level 14: 10 minutes
        1200000,  // Level 15: 20 minutes
        1800000,  // Level 16: 30 minutes
        3600000,  // Level 17: 1 hour
        7200000   // Level 18: 2 hours
    };

    // Delayed messages: msgId -> DelayedMessage
    private final ConcurrentMap<String, DelayedMessage> delayedMessages = new ConcurrentHashMap<>();

    // Statistics
    private final AtomicLong totalDelayedMessages = new AtomicLong(0);
    private final AtomicLong deliveredDelayedMessages = new AtomicLong(0);
    private final AtomicLong cancelledDelayedMessages = new AtomicLong(0);

    public DelayMessageManager(MessageStore messageStore) {
        this.messageStore = messageStore;
        this.scheduler = new TimeWheelScheduler(3600, 1000); // 1 hour wheel with 1 second ticks
        this.scheduler.start();
    }

    /**
     * Schedule delayed message delivery
     *
     * @param message original message
     * @param delayLevel delay level (1-18)
     * @param targetQueue target message queue
     * @return message ID of scheduled message
     */
    public String scheduleDelayedMessage(Message message, int delayLevel, MessageQueue targetQueue) {
        if (delayLevel < 1 || delayLevel > DELAY_LEVELS.length) {
            logger.warn("Invalid delay level: {}, using level 1", delayLevel);
            delayLevel = 1;
        }

        long delayMs = DELAY_LEVELS[delayLevel - 1];
        return scheduleDelayedMessage(message, delayMs, targetQueue);
    }

    /**
     * Schedule delayed message delivery with custom delay
     *
     * @param message original message
     * @param delayMs delay in milliseconds
     * @param targetQueue target message queue
     * @return message ID of scheduled message
     */
    public String scheduleDelayedMessage(Message message, long delayMs, MessageQueue targetQueue) {
        String msgId = generateMessageId();
        long deliveryTime = System.currentTimeMillis() + delayMs;

        DelayedMessage delayedMessage = new DelayedMessage(msgId, message, targetQueue,
                                                         System.currentTimeMillis(), deliveryTime);
        delayedMessages.put(msgId, delayedMessage);

        // Create delay task
        TimeWheelScheduler.DelayTask task = new DelayMessageTask(msgId);

        // Schedule with time wheel
        boolean scheduled = scheduler.schedule(task, delayMs);
        if (scheduled) {
            totalDelayedMessages.incrementAndGet();
            logger.info("Scheduled delayed message: {} with delay: {}ms", msgId, delayMs);
            return msgId;
        } else {
            delayedMessages.remove(msgId);
            logger.error("Failed to schedule delayed message: {}", msgId);
            return null;
        }
    }

    /**
     * Schedule delayed message delivery at specific time
     *
     * @param message original message
     * @param deliveryTime delivery timestamp
     * @param targetQueue target message queue
     * @return message ID of scheduled message
     */
    public String scheduleDelayedMessageAt(Message message, long deliveryTime, MessageQueue targetQueue) {
        long currentTime = System.currentTimeMillis();
        if (deliveryTime <= currentTime) {
            logger.warn("Delivery time is in the past, delivering immediately");
            return deliverMessageNow(message, targetQueue);
        }

        String msgId = generateMessageId();
        DelayedMessage delayedMessage = new DelayedMessage(msgId, message, targetQueue,
                                                         currentTime, deliveryTime);
        delayedMessages.put(msgId, delayedMessage);

        // Create delay task
        TimeWheelScheduler.DelayTask task = new DelayMessageTask(msgId);

        // Schedule with time wheel
        boolean scheduled = scheduler.scheduleAt(task, deliveryTime);
        if (scheduled) {
            totalDelayedMessages.incrementAndGet();
            logger.info("Scheduled delayed message: {} for delivery at: {}", msgId, deliveryTime);
            return msgId;
        } else {
            delayedMessages.remove(msgId);
            logger.error("Failed to schedule delayed message: {}", msgId);
            return null;
        }
    }

    /**
     * Cancel a delayed message
     *
     * @param msgId message ID
     * @return true if cancelled successfully
     */
    public boolean cancelDelayedMessage(String msgId) {
        DelayedMessage delayedMessage = delayedMessages.remove(msgId);
        if (delayedMessage != null) {
            cancelledDelayedMessages.incrementAndGet();
            logger.info("Cancelled delayed message: {}", msgId);
            return true;
        } else {
            logger.warn("Delayed message not found for cancellation: {}", msgId);
            return false;
        }
    }

    /**
     * Get delayed message information
     *
     * @param msgId message ID
     * @return delayed message or null if not found
     */
    public DelayedMessage getDelayedMessage(String msgId) {
        return delayedMessages.get(msgId);
    }

    /**
     * Get delay statistics
     *
     * @return delay statistics
     */
    public DelayStatistics getStatistics() {
        return new DelayStatistics(
            totalDelayedMessages.get(),
            deliveredDelayedMessages.get(),
            cancelledDelayedMessages.get(),
            delayedMessages.size()
        );
    }

    /**
     * Get available delay levels
     *
     * @return array of delay levels in milliseconds
     */
    public static long[] getDelayLevels() {
        return DELAY_LEVELS.clone();
    }

    /**
     * Shutdown the delay message manager
     */
    public void shutdown() {
        logger.info("Shutting down DelayMessageManager...");
        scheduler.shutdown();
        logger.info("DelayMessageManager shut down successfully");
    }

    private String deliverMessageNow(Message message, MessageQueue targetQueue) {
        try {
            // TODO: Integrate with actual message store
            String msgId = generateMessageId();
            logger.info("Delivered message immediately: {} to queue: {}", msgId, targetQueue);
            deliveredDelayedMessages.incrementAndGet();
            return msgId;
        } catch (Exception e) {
            logger.error("Failed to deliver message immediately", e);
            return null;
        }
    }

    private void deliverDelayedMessage(String msgId) {
        DelayedMessage delayedMessage = delayedMessages.remove(msgId);
        if (delayedMessage == null) {
            logger.warn("Delayed message not found for delivery: {}", msgId);
            return;
        }

        try {
            // TODO: Integrate with actual message store
            // messageStore.putMessage(delayedMessage.getTargetQueue(), delayedMessage.getMessage());

            deliveredDelayedMessages.incrementAndGet();
            logger.info("Delivered delayed message: {} to queue: {}",
                       msgId, delayedMessage.getTargetQueue());

        } catch (Exception e) {
            logger.error("Failed to deliver delayed message: " + msgId, e);
            // TODO: Consider retry or dead letter queue
        }
    }

    private String generateMessageId() {
        return "DELAY_" + System.currentTimeMillis() + "_" + Thread.currentThread().getId();
    }

    /**
     * Delayed message task implementation
     */
    private class DelayMessageTask implements TimeWheelScheduler.DelayTask {
        private final String msgId;

        public DelayMessageTask(String msgId) {
            this.msgId = msgId;
        }

        @Override
        public void execute() {
            deliverDelayedMessage(msgId);
        }

        @Override
        public String getTaskId() {
            return msgId;
        }
    }

    /**
     * Delayed message wrapper
     */
    public static class DelayedMessage {
        private final String msgId;
        private final Message message;
        private final MessageQueue targetQueue;
        private final long scheduleTime;
        private final long deliveryTime;

        public DelayedMessage(String msgId, Message message, MessageQueue targetQueue,
                            long scheduleTime, long deliveryTime) {
            this.msgId = msgId;
            this.message = message;
            this.targetQueue = targetQueue;
            this.scheduleTime = scheduleTime;
            this.deliveryTime = deliveryTime;
        }

        // Getters
        public String getMsgId() { return msgId; }
        public Message getMessage() { return message; }
        public MessageQueue getTargetQueue() { return targetQueue; }
        public long getScheduleTime() { return scheduleTime; }
        public long getDeliveryTime() { return deliveryTime; }
        public long getRemainingDelay() { return Math.max(0, deliveryTime - System.currentTimeMillis()); }
    }

    /**
     * Delay statistics
     */
    public static class DelayStatistics {
        private final long totalScheduled;
        private final long totalDelivered;
        private final long totalCancelled;
        private final long currentPending;

        public DelayStatistics(long totalScheduled, long totalDelivered,
                             long totalCancelled, long currentPending) {
            this.totalScheduled = totalScheduled;
            this.totalDelivered = totalDelivered;
            this.totalCancelled = totalCancelled;
            this.currentPending = currentPending;
        }

        public long getTotalScheduled() { return totalScheduled; }
        public long getTotalDelivered() { return totalDelivered; }
        public long getTotalCancelled() { return totalCancelled; }
        public long getCurrentPending() { return currentPending; }
    }
}