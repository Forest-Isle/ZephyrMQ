package com.zephyr.broker.order;

import com.zephyr.protocol.message.Message;
import com.zephyr.protocol.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Order Message Manager
 * Ensures messages are consumed in the same order they were sent
 */
public class OrderMessageManager {

    private static final Logger logger = LoggerFactory.getLogger(OrderMessageManager.class);

    // Order locks for each queue: queueKey -> lock
    private final ConcurrentMap<String, ReentrantLock> queueLocks = new ConcurrentHashMap<>();

    // Order queues: queueKey -> ordered message queue
    private final ConcurrentMap<String, BlockingQueue<OrderedMessage>> orderQueues = new ConcurrentHashMap<>();

    // Sequence tracking: queueKey -> next expected sequence
    private final ConcurrentMap<String, AtomicLong> queueSequences = new ConcurrentHashMap<>();

    // Consumer processing state: queueKey -> processing consumer
    private final ConcurrentMap<String, String> queueConsumers = new ConcurrentHashMap<>();

    /**
     * Send ordered message to queue
     *
     * @param message message to send
     * @param messageQueue target message queue
     * @param orderKey order key for grouping related messages
     * @return message sequence number
     */
    public long sendOrderedMessage(Message message, MessageQueue messageQueue, String orderKey) {
        String queueKey = buildQueueKey(messageQueue, orderKey);
        ReentrantLock lock = queueLocks.computeIfAbsent(queueKey, k -> new ReentrantLock());

        lock.lock();
        try {
            // Get next sequence number
            AtomicLong sequence = queueSequences.computeIfAbsent(queueKey, k -> new AtomicLong(0));
            long sequenceNumber = sequence.incrementAndGet();

            // Create ordered message
            OrderedMessage orderedMessage = new OrderedMessage(message, messageQueue,
                                                             orderKey, sequenceNumber, System.currentTimeMillis());

            // Add to order queue
            BlockingQueue<OrderedMessage> orderQueue = orderQueues.computeIfAbsent(queueKey,
                                                                                  k -> new LinkedBlockingQueue<>());
            orderQueue.offer(orderedMessage);

            logger.debug("Added ordered message: queueKey={}, sequence={}, orderKey={}",
                        queueKey, sequenceNumber, orderKey);

            return sequenceNumber;

        } finally {
            lock.unlock();
        }
    }

    /**
     * Consume next ordered message from queue
     *
     * @param messageQueue message queue
     * @param orderKey order key
     * @param consumerId consumer ID
     * @param timeoutMs timeout in milliseconds
     * @return next ordered message or null if timeout
     */
    public OrderedMessage consumeOrderedMessage(MessageQueue messageQueue, String orderKey,
                                              String consumerId, long timeoutMs) {
        String queueKey = buildQueueKey(messageQueue, orderKey);

        // Check if another consumer is processing this order queue
        String currentConsumer = queueConsumers.putIfAbsent(queueKey, consumerId);
        if (currentConsumer != null && !currentConsumer.equals(consumerId)) {
            logger.warn("Order queue {} is being processed by another consumer: {}",
                       queueKey, currentConsumer);
            return null;
        }

        ReentrantLock lock = queueLocks.computeIfAbsent(queueKey, k -> new ReentrantLock());
        lock.lock();
        try {
            BlockingQueue<OrderedMessage> orderQueue = orderQueues.get(queueKey);
            if (orderQueue == null || orderQueue.isEmpty()) {
                return null;
            }

            // Get next message
            OrderedMessage orderedMessage = timeoutMs > 0 ?
                orderQueue.poll(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS) :
                orderQueue.poll();

            if (orderedMessage != null) {
                logger.debug("Consumed ordered message: queueKey={}, sequence={}, consumer={}",
                           queueKey, orderedMessage.getSequenceNumber(), consumerId);
            }

            return orderedMessage;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while waiting for ordered message: {}", queueKey);
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Release consumer from order queue processing
     *
     * @param messageQueue message queue
     * @param orderKey order key
     * @param consumerId consumer ID
     */
    public void releaseOrderQueue(MessageQueue messageQueue, String orderKey, String consumerId) {
        String queueKey = buildQueueKey(messageQueue, orderKey);
        String currentConsumer = queueConsumers.get(queueKey);

        if (consumerId.equals(currentConsumer)) {
            queueConsumers.remove(queueKey);
            logger.debug("Released order queue: queueKey={}, consumer={}", queueKey, consumerId);
        }
    }

    /**
     * Get order queue status
     *
     * @param messageQueue message queue
     * @param orderKey order key
     * @return order queue status
     */
    public OrderQueueStatus getOrderQueueStatus(MessageQueue messageQueue, String orderKey) {
        String queueKey = buildQueueKey(messageQueue, orderKey);

        BlockingQueue<OrderedMessage> orderQueue = orderQueues.get(queueKey);
        int pendingMessages = orderQueue != null ? orderQueue.size() : 0;

        AtomicLong sequence = queueSequences.get(queueKey);
        long currentSequence = sequence != null ? sequence.get() : 0;

        String processingConsumer = queueConsumers.get(queueKey);

        return new OrderQueueStatus(queueKey, pendingMessages, currentSequence, processingConsumer);
    }

    /**
     * Check if topic/queue supports ordered messaging
     *
     * @param messageQueue message queue
     * @return true if ordered messaging is supported
     */
    public boolean isOrderedQueue(MessageQueue messageQueue) {
        // In a real implementation, this would check topic configuration
        // For now, assume all queues can support ordering
        return true;
    }

    /**
     * Get total number of order queues
     *
     * @return number of active order queues
     */
    public int getOrderQueueCount() {
        return orderQueues.size();
    }

    /**
     * Clean up empty order queues and resources
     */
    public void cleanup() {
        // Remove empty queues
        orderQueues.entrySet().removeIf(entry -> entry.getValue().isEmpty());

        // Clean up unused locks and sequences
        queueLocks.keySet().removeIf(key -> !orderQueues.containsKey(key));
        queueSequences.keySet().removeIf(key -> !orderQueues.containsKey(key));

        logger.debug("Cleaned up order queues, remaining: {}", orderQueues.size());
    }

    private String buildQueueKey(MessageQueue messageQueue, String orderKey) {
        return messageQueue.getTopic() + ":" + messageQueue.getBrokerName() + ":" +
               messageQueue.getQueueId() + ":" + (orderKey != null ? orderKey : "default");
    }

    /**
     * Ordered message wrapper
     */
    public static class OrderedMessage {
        private final Message message;
        private final MessageQueue messageQueue;
        private final String orderKey;
        private final long sequenceNumber;
        private final long createTime;

        public OrderedMessage(Message message, MessageQueue messageQueue, String orderKey,
                            long sequenceNumber, long createTime) {
            this.message = message;
            this.messageQueue = messageQueue;
            this.orderKey = orderKey;
            this.sequenceNumber = sequenceNumber;
            this.createTime = createTime;
        }

        // Getters
        public Message getMessage() { return message; }
        public MessageQueue getMessageQueue() { return messageQueue; }
        public String getOrderKey() { return orderKey; }
        public long getSequenceNumber() { return sequenceNumber; }
        public long getCreateTime() { return createTime; }

        @Override
        public String toString() {
            return "OrderedMessage{" +
                    "orderKey='" + orderKey + '\'' +
                    ", sequenceNumber=" + sequenceNumber +
                    ", messageQueue=" + messageQueue +
                    ", createTime=" + createTime +
                    '}';
        }
    }

    /**
     * Order queue status information
     */
    public static class OrderQueueStatus {
        private final String queueKey;
        private final int pendingMessages;
        private final long currentSequence;
        private final String processingConsumer;

        public OrderQueueStatus(String queueKey, int pendingMessages,
                              long currentSequence, String processingConsumer) {
            this.queueKey = queueKey;
            this.pendingMessages = pendingMessages;
            this.currentSequence = currentSequence;
            this.processingConsumer = processingConsumer;
        }

        // Getters
        public String getQueueKey() { return queueKey; }
        public int getPendingMessages() { return pendingMessages; }
        public long getCurrentSequence() { return currentSequence; }
        public String getProcessingConsumer() { return processingConsumer; }

        @Override
        public String toString() {
            return "OrderQueueStatus{" +
                    "queueKey='" + queueKey + '\'' +
                    ", pendingMessages=" + pendingMessages +
                    ", currentSequence=" + currentSequence +
                    ", processingConsumer='" + processingConsumer + '\'' +
                    '}';
        }
    }
}