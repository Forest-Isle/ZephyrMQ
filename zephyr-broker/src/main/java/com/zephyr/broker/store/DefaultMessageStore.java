package com.zephyr.broker.store;

import com.zephyr.common.config.BrokerConfig;
import com.zephyr.protocol.message.MessageExt;
import com.zephyr.protocol.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultMessageStore implements MessageStore {

    private static final Logger logger = LoggerFactory.getLogger(DefaultMessageStore.class);

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final BrokerConfig brokerConfig;
    private final AtomicLong currentPhyOffset = new AtomicLong(0);

    // In-memory storage for simplicity - in production would use file-based storage
    private final ConcurrentMap<String, List<MessageExt>> messageStorage = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicLong> topicOffsets = new ConcurrentHashMap<>();

    public DefaultMessageStore(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
    }

    @Override
    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            logger.info("DefaultMessageStore starting...");

            // Create storage directories
            createStorageDirectories();

            // Initialize storage components
            initializeStorage();

            logger.info("DefaultMessageStore started successfully");
        }
    }

    @Override
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            logger.info("DefaultMessageStore shutting down...");

            // Cleanup resources
            messageStorage.clear();
            topicOffsets.clear();

            logger.info("DefaultMessageStore shut down successfully");
        }
    }

    @Override
    public boolean putMessage(MessageExt messageExt) {
        try {
            if (messageExt == null) {
                logger.warn("Message is null, cannot store");
                return false;
            }

            // Set physical offset
            long physicalOffset = currentPhyOffset.getAndIncrement();
            messageExt.setCommitLogOffset(physicalOffset);

            // Set queue information
            String topic = messageExt.getTopic();
            int queueId = messageExt.getQueueId();
            String queueKey = topic + "_" + queueId;

            // Get next queue offset
            AtomicLong queueOffset = topicOffsets.computeIfAbsent(queueKey, k -> new AtomicLong(0));
            messageExt.setQueueOffset(queueOffset.getAndIncrement());

            // Set message queue
            MessageQueue messageQueue = new MessageQueue();
            messageQueue.setTopic(topic);
            messageQueue.setQueueId(queueId);
            messageQueue.setBrokerName(brokerConfig.getBrokerName());
            messageExt.setMessageQueue(messageQueue);

            // Store message
            messageStorage.computeIfAbsent(queueKey, k -> new ArrayList<>()).add(messageExt);

            logger.debug("Message stored successfully: msgId={}, topic={}, queueId={}, queueOffset={}",
                    messageExt.getMsgId(), topic, queueId, messageExt.getQueueOffset());

            return true;

        } catch (Exception e) {
            logger.error("Failed to store message", e);
            return false;
        }
    }

    @Override
    public List<MessageExt> getMessage(String topic, int queueId, long offset, int maxMsgNums) {
        try {
            String queueKey = topic + "_" + queueId;
            List<MessageExt> queueMessages = messageStorage.get(queueKey);

            if (queueMessages == null || queueMessages.isEmpty()) {
                logger.debug("No messages found for topic={}, queueId={}", topic, queueId);
                return new ArrayList<>();
            }

            List<MessageExt> result = new ArrayList<>();
            int count = 0;

            for (MessageExt message : queueMessages) {
                if (message.getQueueOffset() >= offset && count < maxMsgNums) {
                    result.add(message);
                    count++;
                }
            }

            logger.debug("Retrieved {} messages for topic={}, queueId={}, offset={}",
                    result.size(), topic, queueId, offset);

            return result;

        } catch (Exception e) {
            logger.error("Failed to get messages", e);
            return new ArrayList<>();
        }
    }

    @Override
    public MessageExt lookMessageByOffset(long commitLogOffset) {
        // Simple implementation - in production would use index
        for (List<MessageExt> messages : messageStorage.values()) {
            for (MessageExt message : messages) {
                if (message.getCommitLogOffset() == commitLogOffset) {
                    return message;
                }
            }
        }
        return null;
    }

    @Override
    public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
        return lookMessageByOffset(commitLogOffset);
    }

    @Override
    public long getMaxPhyOffset() {
        return currentPhyOffset.get();
    }

    @Override
    public long getMinPhyOffset() {
        return 0;
    }

    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        String queueKey = topic + "_" + queueId;
        List<MessageExt> messages = messageStorage.get(queueKey);
        if (messages != null && consumeQueueOffset >= 0 && consumeQueueOffset < messages.size()) {
            return messages.get((int) consumeQueueOffset).getCommitLogOffset();
        }
        return -1;
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        String queueKey = topic + "_" + queueId;
        List<MessageExt> messages = messageStorage.get(queueKey);
        if (messages != null) {
            for (int i = 0; i < messages.size(); i++) {
                if (messages.get(i).getStoreTimestamp() >= timestamp) {
                    return i;
                }
            }
        }
        return messages != null ? messages.size() : 0;
    }

    @Override
    public long getMaxOffsetInQueue(String topic, int queueId) {
        String queueKey = topic + "_" + queueId;
        AtomicLong offset = topicOffsets.get(queueKey);
        return offset != null ? offset.get() : 0;
    }

    @Override
    public long getMinOffsetInQueue(String topic, int queueId) {
        return 0;
    }

    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        String queueKey = topic + "_" + queueId;
        List<MessageExt> messages = messageStorage.get(queueKey);
        if (messages != null && !messages.isEmpty()) {
            return messages.get(0).getStoreTimestamp();
        }
        return System.currentTimeMillis();
    }

    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
        String queueKey = topic + "_" + queueId;
        List<MessageExt> messages = messageStorage.get(queueKey);
        if (messages != null && consumeQueueOffset >= 0 && consumeQueueOffset < messages.size()) {
            return messages.get((int) consumeQueueOffset).getStoreTimestamp();
        }
        return -1;
    }

    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        String queueKey = topic + "_" + queueId;
        List<MessageExt> messages = messageStorage.get(queueKey);
        return messages != null ? messages.size() : 0;
    }

    @Override
    public byte[] findMessageByOffset(long commitLogOffset, int size) {
        MessageExt message = lookMessageByOffset(commitLogOffset);
        return message != null ? message.getBody() : null;
    }

    // Simplified implementations for the remaining methods
    @Override
    public boolean cleanExpiredConsumerQueue() {
        return true;
    }

    @Override
    public boolean cleanExpiredCommitLog() {
        return true;
    }

    @Override
    public void executeDeleteFilesManualy() {
        // Do nothing for now
    }

    @Override
    public void warmMappedFile(String type, String fileName) {
        // Do nothing for now
    }

    @Override
    public long getDispatchBehindBytes() {
        return 0;
    }

    @Override
    public long getTotalSize() {
        return messageStorage.values().stream()
                .mapToLong(List::size)
                .sum();
    }

    @Override
    public boolean isOSPageCacheBusy() {
        return false;
    }

    @Override
    public long getLockTimeMills() {
        return 0;
    }

    @Override
    public boolean isTransientStorePoolDeficient() {
        return false;
    }

    @Override
    public double getDispatchMaxBuffer() {
        return 0.0;
    }

    @Override
    public boolean isSpaceRequired() {
        return false;
    }

    @Override
    public long getCommitLogCommitLogSize() {
        return getTotalSize();
    }

    @Override
    public boolean isSyncDiskFlush() {
        return false;
    }

    private void createStorageDirectories() throws Exception {
        createDirectoryIfNotExists(brokerConfig.getStorePathRootDir());
        createDirectoryIfNotExists(brokerConfig.getStorePathCommitLog());
        createDirectoryIfNotExists(brokerConfig.getStorePathConsumeQueue());
        createDirectoryIfNotExists(brokerConfig.getStorePathIndex());
    }

    private void createDirectoryIfNotExists(String path) throws Exception {
        File dir = new File(path);
        if (!dir.exists()) {
            boolean created = dir.mkdirs();
            if (!created) {
                throw new Exception("Failed to create directory: " + path);
            }
            logger.info("Created storage directory: {}", path);
        }
    }

    private void initializeStorage() {
        logger.info("Initializing message storage components...");
        // Initialize storage components here
        logger.info("Message storage components initialized");
    }
}