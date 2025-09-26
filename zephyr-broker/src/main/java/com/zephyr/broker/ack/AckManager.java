package com.zephyr.broker.ack;

import com.zephyr.protocol.ack.MessageAck;
import com.zephyr.protocol.message.MessageQueue;
import com.zephyr.protocol.message.Message;
import com.zephyr.broker.dlq.DeadLetterQueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;

/**
 * Message acknowledgment manager
 * Handles message acknowledgments, retries, and dead letter queue
 */
public class AckManager {

    private static final Logger logger = LoggerFactory.getLogger(AckManager.class);

    // Pending acknowledgments: msgId -> PendingAck
    private final ConcurrentMap<String, PendingAck> pendingAcks = new ConcurrentHashMap<>();

    // Retry queue for failed messages
    private final DelayQueue<RetryMessage> retryQueue = new DelayQueue<>();

    // Consumer offset tracking: consumerGroup:topic:queueId -> offset
    private final ConcurrentMap<String, Long> consumerOffsets = new ConcurrentHashMap<>();

    // Configuration
    private final long ackTimeoutMs;
    private final int maxRetryTimes;
    private final long retryDelayMs;

    // Dead letter queue manager
    private final DeadLetterQueueManager deadLetterQueueManager;

    // Executor for background tasks
    private final ScheduledExecutorService executor;

    public AckManager(DeadLetterQueueManager deadLetterQueueManager) {
        this(deadLetterQueueManager, 30000, 3, 5000); // 30s timeout, 3 retries, 5s delay
    }

    public AckManager(DeadLetterQueueManager deadLetterQueueManager, long ackTimeoutMs, int maxRetryTimes, long retryDelayMs) {
        this.deadLetterQueueManager = deadLetterQueueManager;
        this.ackTimeoutMs = ackTimeoutMs;
        this.maxRetryTimes = maxRetryTimes;
        this.retryDelayMs = retryDelayMs;
        this.executor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "AckManager");
            t.setDaemon(true);
            return t;
        });

        startBackgroundTasks();
    }

    /**
     * Register message for acknowledgment tracking
     *
     * @param msgId message ID
     * @param messageQueue message queue
     * @param consumerGroup consumer group
     * @param consumerId consumer ID
     */
    public void registerMessage(String msgId, MessageQueue messageQueue,
                              String consumerGroup, String consumerId) {
        PendingAck pendingAck = new PendingAck(msgId, messageQueue,
                                             consumerGroup, consumerId, System.currentTimeMillis());
        pendingAcks.put(msgId, pendingAck);
        logger.debug("Registered message for ack: {} from consumer {}", msgId, consumerId);
    }

    /**
     * Acknowledge message
     *
     * @param ack message acknowledgment
     * @return true if acknowledged successfully
     */
    public boolean acknowledgeMessage(MessageAck ack) {
        String msgId = ack.getMsgId();
        PendingAck pendingAck = pendingAcks.remove(msgId);

        if (pendingAck == null) {
            logger.warn("Received ack for unknown message: {}", msgId);
            return false;
        }

        switch (ack.getAckStatus()) {
            case SUCCESS:
                handleSuccessAck(ack, pendingAck);
                return true;

            case RETRY_LATER:
                handleRetryAck(ack, pendingAck);
                return true;

            case DEAD_LETTER:
                handleDeadLetterAck(ack, pendingAck);
                return true;

            default:
                logger.warn("Unknown ack status: {}", ack.getAckStatus());
                return false;
        }
    }

    /**
     * Get consumer offset
     *
     * @param consumerGroup consumer group
     * @param topic topic
     * @param queueId queue ID
     * @return current offset
     */
    public long getConsumerOffset(String consumerGroup, String topic, int queueId) {
        String key = buildOffsetKey(consumerGroup, topic, queueId);
        return consumerOffsets.getOrDefault(key, 0L);
    }

    /**
     * Update consumer offset
     *
     * @param consumerGroup consumer group
     * @param topic topic
     * @param queueId queue ID
     * @param offset new offset
     */
    public void updateConsumerOffset(String consumerGroup, String topic, int queueId, long offset) {
        String key = buildOffsetKey(consumerGroup, topic, queueId);
        consumerOffsets.put(key, offset);
        logger.debug("Updated consumer offset: {} -> {}", key, offset);
    }

    /**
     * Get pending acknowledgment count
     *
     * @return number of pending acknowledgments
     */
    public int getPendingAckCount() {
        return pendingAcks.size();
    }

    /**
     * Get retry queue size
     *
     * @return number of messages in retry queue
     */
    public int getRetryQueueSize() {
        return retryQueue.size();
    }

    /**
     * Shutdown the ack manager
     */
    public void shutdown() {
        logger.info("Shutting down AckManager...");
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("AckManager shut down successfully");
    }

    private void handleSuccessAck(MessageAck ack, PendingAck pendingAck) {
        // Update consumer offset
        updateConsumerOffset(ack.getConsumerGroup(), ack.getTopic(),
                           ack.getQueueId(), ack.getQueueOffset() + 1);

        logger.debug("Successfully acknowledged message: {}", ack.getMsgId());
    }

    private void handleRetryAck(MessageAck ack, PendingAck pendingAck) {
        int currentRetry = pendingAck.getRetryCount();
        if (currentRetry < maxRetryTimes) {
            // Add to retry queue
            RetryMessage retryMessage = new RetryMessage(pendingAck, currentRetry + 1,
                                                       System.currentTimeMillis() + retryDelayMs);
            retryQueue.offer(retryMessage);
            logger.info("Added message to retry queue: {} (retry: {})", ack.getMsgId(), currentRetry + 1);
        } else {
            // Send to dead letter queue
            handleDeadLetterMessage(ack, pendingAck);
        }
    }

    private void handleDeadLetterAck(MessageAck ack, PendingAck pendingAck) {
        handleDeadLetterMessage(ack, pendingAck);
    }

    private void handleDeadLetterMessage(MessageAck ack, PendingAck pendingAck) {
        try {
            // Use dead letter queue manager to handle failed message
            if (deadLetterQueueManager != null) {
                // Create a mock original message for DLQ processing
                Message originalMessage = createMessageFromPendingAck(pendingAck);
                boolean success = deadLetterQueueManager.sendToDeadLetterQueue(
                    originalMessage,
                    pendingAck.getMessageQueue().getTopic(),
                    pendingAck.getConsumerGroup(),
                    pendingAck.getRetryCount(),
                    "Max retries exceeded"
                );

                if (success) {
                    logger.info("Message sent to dead letter queue successfully: {}", ack.getMsgId());
                } else {
                    logger.error("Failed to send message to dead letter queue: {}", ack.getMsgId());
                }
            } else {
                logger.warn("Dead letter queue manager not available. Message: {} (retries: {})",
                           ack.getMsgId(), pendingAck.getRetryCount());
            }
        } catch (Exception e) {
            logger.error("Error handling dead letter message: {}", ack.getMsgId(), e);
        }
    }

    private void startBackgroundTasks() {
        // Check for timeout acknowledgments
        executor.scheduleAtFixedRate(this::checkTimeoutAcks, 10, 10, TimeUnit.SECONDS);

        // Process retry queue
        executor.scheduleAtFixedRate(this::processRetryQueue, 1, 1, TimeUnit.SECONDS);
    }

    private void checkTimeoutAcks() {
        long currentTime = System.currentTimeMillis();
        List<String> timeoutMsgIds = pendingAcks.entrySet().stream()
                .filter(entry -> currentTime - entry.getValue().getCreateTime() > ackTimeoutMs)
                .map(entry -> entry.getKey())
                .toList();

        for (String msgId : timeoutMsgIds) {
            PendingAck pendingAck = pendingAcks.remove(msgId);
            if (pendingAck != null) {
                logger.warn("Message acknowledgment timeout: {}", msgId);
                // Create retry ack
                MessageAck retryAck = new MessageAck(msgId, pendingAck.getMessageQueue().getTopic(),
                                                   pendingAck.getMessageQueue().getQueueId(), 0,
                                                   pendingAck.getConsumerGroup(), pendingAck.getConsumerId(),
                                                   MessageAck.AckStatus.RETRY_LATER);
                handleRetryAck(retryAck, pendingAck);
            }
        }
    }

    private void processRetryQueue() {
        RetryMessage retryMessage;
        while ((retryMessage = retryQueue.poll()) != null) {
            try {
                // Check if retry limit exceeded
                if (retryMessage.getRetryCount() >= maxRetryTimes) {
                    logger.warn("Retry limit exceeded for message: {}", retryMessage.getPendingAck().getMsgId());
                    // Send to dead letter queue
                    MessageAck deadLetterAck = new MessageAck(
                        retryMessage.getPendingAck().getMsgId(),
                        retryMessage.getPendingAck().getMessageQueue().getTopic(),
                        retryMessage.getPendingAck().getMessageQueue().getQueueId(),
                        0,
                        retryMessage.getPendingAck().getConsumerGroup(),
                        retryMessage.getPendingAck().getConsumerId(),
                        MessageAck.AckStatus.DEAD_LETTER
                    );
                    handleDeadLetterAck(deadLetterAck, retryMessage.getPendingAck());
                    continue;
                }

                // Attempt to resend message to consumer
                boolean resent = resendMessageToConsumer(retryMessage);
                if (resent) {
                    // Update retry count and re-track
                    PendingAck updatedPendingAck = retryMessage.getPendingAck();
                    updatedPendingAck.incrementRetryCount();
                    pendingAcks.put(updatedPendingAck.getMsgId(), updatedPendingAck);

                    logger.info("Message resent to consumer: {} (retry: {})",
                               retryMessage.getPendingAck().getMsgId(), retryMessage.getRetryCount());
                } else {
                    // Resend failed, add back to retry queue with delay
                    RetryMessage delayedRetry = new RetryMessage(retryMessage.getPendingAck(),
                                                               retryMessage.getRetryCount() + 1,
                                                               System.currentTimeMillis() + calculateRetryDelay(retryMessage.getRetryCount()));
                    retryQueue.offer(delayedRetry);

                    logger.warn("Failed to resend message, will retry later: {}",
                               retryMessage.getPendingAck().getMsgId());
                }
            } catch (Exception e) {
                logger.error("Error processing retry message: {}",
                           retryMessage.getPendingAck().getMsgId(), e);
            }
        }
    }

    private String buildOffsetKey(String consumerGroup, String topic, int queueId) {
        return consumerGroup + ":" + topic + ":" + queueId;
    }

    // Inner classes
    private static class PendingAck {
        private final String msgId;
        private final MessageQueue messageQueue;
        private final String consumerGroup;
        private final String consumerId;
        private final long createTime;
        private int retryCount = 0;

        public PendingAck(String msgId, MessageQueue messageQueue, String consumerGroup,
                         String consumerId, long createTime) {
            this.msgId = msgId;
            this.messageQueue = messageQueue;
            this.consumerGroup = consumerGroup;
            this.consumerId = consumerId;
            this.createTime = createTime;
        }

        // Getters
        public String getMsgId() { return msgId; }
        public MessageQueue getMessageQueue() { return messageQueue; }
        public String getConsumerGroup() { return consumerGroup; }
        public String getConsumerId() { return consumerId; }
        public long getCreateTime() { return createTime; }
        public int getRetryCount() { return retryCount; }
        public void setRetryCount(int retryCount) { this.retryCount = retryCount; }
        public void incrementRetryCount() { this.retryCount++; }
    }

    private static class RetryMessage implements Delayed {
        private final PendingAck pendingAck;
        private final int retryCount;
        private final long executeTime;

        public RetryMessage(PendingAck pendingAck, int retryCount, long executeTime) {
            this.pendingAck = pendingAck;
            this.retryCount = retryCount;
            this.executeTime = executeTime;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(executeTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            if (this == o) return 0;
            if (o instanceof RetryMessage) {
                return Long.compare(this.executeTime, ((RetryMessage) o).executeTime);
            }
            return Long.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
        }

        public PendingAck getPendingAck() { return pendingAck; }
        public int getRetryCount() { return retryCount; }
        public long getExecuteTime() { return executeTime; }
    }

    // Helper methods for retry functionality
    private Message createMessageFromPendingAck(PendingAck pendingAck) {
        Message message = new Message();
        message.setTopic(pendingAck.getMessageQueue().getTopic());
        message.setBody(("Retry message for " + pendingAck.getMsgId()).getBytes());

        // Add metadata
        message.putProperty("ORIGINAL_MSG_ID", pendingAck.getMsgId());
        message.putProperty("CONSUMER_GROUP", pendingAck.getConsumerGroup());
        message.putProperty("RETRY_COUNT", String.valueOf(pendingAck.getRetryCount()));

        return message;
    }

    private boolean resendMessageToConsumer(RetryMessage retryMessage) {
        try {
            // Mock implementation - in real scenario, this would send the message
            // back to the consumer through the message queue
            logger.debug("Attempting to resend message to consumer: {}",
                        retryMessage.getPendingAck().getMsgId());

            // Simulate success/failure (for demonstration)
            return Math.random() > 0.3; // 70% success rate
        } catch (Exception e) {
            logger.error("Failed to resend message to consumer: {}",
                        retryMessage.getPendingAck().getMsgId(), e);
            return false;
        }
    }

    private void scheduleRetry(RetryMessage retryMessage, long delayMs) {
        try {
            RetryMessage delayedRetry = new RetryMessage(
                retryMessage.getPendingAck(),
                retryMessage.getRetryCount(),
                System.currentTimeMillis() + delayMs
            );
            retryQueue.offer(delayedRetry);
            logger.debug("Scheduled retry for message: {} in {}ms",
                        retryMessage.getPendingAck().getMsgId(), delayMs);
        } catch (Exception e) {
            logger.error("Failed to schedule retry for message: {}",
                        retryMessage.getPendingAck().getMsgId(), e);
        }
    }

    private long calculateRetryDelay(int retryCount) {
        // Exponential backoff: base delay * (2^retryCount)
        return Math.min(retryDelayMs * (1L << retryCount), 300000L); // Max 5 minutes
    }
}