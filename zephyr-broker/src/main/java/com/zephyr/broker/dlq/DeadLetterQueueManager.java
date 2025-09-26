package com.zephyr.broker.dlq;

import com.zephyr.protocol.message.Message;
import com.zephyr.protocol.message.MessageQueue;
import com.zephyr.broker.store.MessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dead Letter Queue Manager
 * Handles messages that failed to be consumed after maximum retries
 */
public class DeadLetterQueueManager {

    private static final Logger logger = LoggerFactory.getLogger(DeadLetterQueueManager.class);

    private static final String DLQ_TOPIC_SUFFIX = "_DLQ";
    private static final String RETRY_TOPIC_SUFFIX = "_RETRY";

    private final MessageStore messageStore;

    // DLQ statistics: topic -> count
    private final ConcurrentMap<String, AtomicLong> dlqStats = new ConcurrentHashMap<>();

    // Retry statistics: topic -> count
    private final ConcurrentMap<String, AtomicLong> retryStats = new ConcurrentHashMap<>();

    public DeadLetterQueueManager(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    /**
     * Send message to dead letter queue
     *
     * @param originalMessage original message
     * @param originalTopic original topic
     * @param consumerGroup consumer group
     * @param retryTimes number of retry attempts
     * @param reason failure reason
     * @return true if sent successfully
     */
    public boolean sendToDeadLetterQueue(Message originalMessage, String originalTopic,
                                       String consumerGroup, int retryTimes, String reason) {
        try {
            // Create DLQ topic name
            String dlqTopic = buildDlqTopicName(originalTopic, consumerGroup);

            // Create DLQ message with metadata
            Message dlqMessage = createDlqMessage(originalMessage, originalTopic,
                                                consumerGroup, retryTimes, reason);
            dlqMessage.setTopic(dlqTopic);

            // Store to DLQ topic
            // TODO: Use actual message store implementation
            logger.info("Message sent to dead letter queue: topic={}, msgId={}, reason={}",
                       dlqTopic, originalMessage.getProperty("msgId"), reason);

            // Update statistics
            dlqStats.computeIfAbsent(originalTopic, k -> new AtomicLong(0)).incrementAndGet();

            return true;

        } catch (Exception e) {
            logger.error("Failed to send message to dead letter queue", e);
            return false;
        }
    }

    /**
     * Send message to retry topic
     *
     * @param originalMessage original message
     * @param originalTopic original topic
     * @param consumerGroup consumer group
     * @param retryCount current retry count
     * @param delayLevel delay level for retry
     * @return true if sent successfully
     */
    public boolean sendToRetryTopic(Message originalMessage, String originalTopic,
                                  String consumerGroup, int retryCount, int delayLevel) {
        try {
            // Create retry topic name
            String retryTopic = buildRetryTopicName(originalTopic, consumerGroup);

            // Create retry message with metadata
            Message retryMessage = createRetryMessage(originalMessage, originalTopic,
                                                    consumerGroup, retryCount);
            retryMessage.setTopic(retryTopic);
            retryMessage.setDelayTimeLevel(delayLevel);

            // Store to retry topic
            // TODO: Use actual message store implementation
            logger.info("Message sent to retry topic: topic={}, msgId={}, retryCount={}",
                       retryTopic, originalMessage.getProperty("msgId"), retryCount);

            // Update statistics
            retryStats.computeIfAbsent(originalTopic, k -> new AtomicLong(0)).incrementAndGet();

            return true;

        } catch (Exception e) {
            logger.error("Failed to send message to retry topic", e);
            return false;
        }
    }

    /**
     * Get dead letter queue statistics
     *
     * @param topic original topic
     * @return number of messages in DLQ
     */
    public long getDlqMessageCount(String topic) {
        AtomicLong count = dlqStats.get(topic);
        return count != null ? count.get() : 0;
    }

    /**
     * Get retry queue statistics
     *
     * @param topic original topic
     * @return number of retry messages
     */
    public long getRetryMessageCount(String topic) {
        AtomicLong count = retryStats.get(topic);
        return count != null ? count.get() : 0;
    }

    /**
     * Get DLQ topic name for consumer group
     *
     * @param originalTopic original topic
     * @param consumerGroup consumer group
     * @return DLQ topic name
     */
    public String getDlqTopicName(String originalTopic, String consumerGroup) {
        return buildDlqTopicName(originalTopic, consumerGroup);
    }

    /**
     * Get retry topic name for consumer group
     *
     * @param originalTopic original topic
     * @param consumerGroup consumer group
     * @return retry topic name
     */
    public String getRetryTopicName(String originalTopic, String consumerGroup) {
        return buildRetryTopicName(originalTopic, consumerGroup);
    }

    /**
     * Check if topic is a DLQ topic
     *
     * @param topic topic name
     * @return true if it's a DLQ topic
     */
    public boolean isDlqTopic(String topic) {
        return topic != null && topic.contains(DLQ_TOPIC_SUFFIX);
    }

    /**
     * Check if topic is a retry topic
     *
     * @param topic topic name
     * @return true if it's a retry topic
     */
    public boolean isRetryTopic(String topic) {
        return topic != null && topic.contains(RETRY_TOPIC_SUFFIX);
    }

    /**
     * Get DLQ statistics for all topics
     *
     * @return DLQ statistics map
     */
    public ConcurrentMap<String, AtomicLong> getAllDlqStats() {
        return new ConcurrentHashMap<>(dlqStats);
    }

    /**
     * Get retry statistics for all topics
     *
     * @return retry statistics map
     */
    public ConcurrentMap<String, AtomicLong> getAllRetryStats() {
        return new ConcurrentHashMap<>(retryStats);
    }

    private String buildDlqTopicName(String originalTopic, String consumerGroup) {
        return consumerGroup + "_" + originalTopic + DLQ_TOPIC_SUFFIX;
    }

    private String buildRetryTopicName(String originalTopic, String consumerGroup) {
        return consumerGroup + "_" + originalTopic + RETRY_TOPIC_SUFFIX;
    }

    private Message createDlqMessage(Message originalMessage, String originalTopic,
                                   String consumerGroup, int retryTimes, String reason) {
        Message dlqMessage = new Message();
        dlqMessage.setBody(originalMessage.getBody());
        dlqMessage.setTags(originalMessage.getTags());
        dlqMessage.setKeys(originalMessage.getKeys());
        dlqMessage.setFlag(originalMessage.getFlag());

        // Add DLQ metadata
        dlqMessage.putProperty("ORIGINAL_TOPIC", originalTopic);
        dlqMessage.putProperty("CONSUMER_GROUP", consumerGroup);
        dlqMessage.putProperty("RETRY_TIMES", String.valueOf(retryTimes));
        dlqMessage.putProperty("DLQ_TIME", String.valueOf(System.currentTimeMillis()));
        dlqMessage.putProperty("FAILURE_REASON", reason);

        // Copy original properties
        if (originalMessage.getProperties() != null) {
            originalMessage.getProperties().forEach((key, value) -> {
                if (!key.startsWith("DLQ_") && !key.equals("ORIGINAL_TOPIC")) {
                    dlqMessage.putProperty("ORIGINAL_" + key, value);
                }
            });
        }

        return dlqMessage;
    }

    private Message createRetryMessage(Message originalMessage, String originalTopic,
                                     String consumerGroup, int retryCount) {
        Message retryMessage = new Message();
        retryMessage.setBody(originalMessage.getBody());
        retryMessage.setTags(originalMessage.getTags());
        retryMessage.setKeys(originalMessage.getKeys());
        retryMessage.setFlag(originalMessage.getFlag());

        // Add retry metadata
        retryMessage.putProperty("ORIGINAL_TOPIC", originalTopic);
        retryMessage.putProperty("CONSUMER_GROUP", consumerGroup);
        retryMessage.putProperty("RETRY_COUNT", String.valueOf(retryCount));
        retryMessage.putProperty("RETRY_TIME", String.valueOf(System.currentTimeMillis()));

        // Copy original properties
        if (originalMessage.getProperties() != null) {
            originalMessage.getProperties().forEach(retryMessage::putProperty);
        }

        return retryMessage;
    }
}