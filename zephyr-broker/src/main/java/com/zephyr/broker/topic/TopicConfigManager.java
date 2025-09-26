package com.zephyr.broker.topic;

import com.zephyr.common.config.BrokerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TopicConfigManager {

    private static final Logger logger = LoggerFactory.getLogger(TopicConfigManager.class);

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final BrokerConfig brokerConfig;
    private final ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
    private final TopicMetadataPersistence persistence;

    public TopicConfigManager(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
        this.persistence = new TopicMetadataPersistence(brokerConfig);
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            logger.info("TopicConfigManager starting...");

            // Start persistence service
            persistence.start();

            // Load existing configurations
            loadTopicConfigs();

            logger.info("TopicConfigManager started successfully");
        }
    }

    public void shutdown() {
        logger.info("TopicConfigManager shutting down...");

        // Shutdown persistence service (this will trigger final flush)
        persistence.shutdown();

        logger.info("TopicConfigManager shut down successfully");
    }

    public TopicConfig getTopicConfig(String topic) {
        return topicConfigTable.get(topic);
    }

    public void updateTopicConfig(TopicConfig topicConfig) {
        topicConfigTable.put(topicConfig.getTopicName(), topicConfig);

        // Persist to disk
        persistence.saveTopicConfig(topicConfig.getTopicName(), topicConfig);

        logger.info("Updated topic config: {}", topicConfig.getTopicName());
    }

    public boolean createTopicIfNotExists(String topic, int queueNums, int perm) {
        TopicConfig topicConfig = topicConfigTable.get(topic);
        if (topicConfig == null) {
            topicConfig = new TopicConfig();
            topicConfig.setTopicName(topic);
            topicConfig.setReadQueueNums(queueNums);
            topicConfig.setWriteQueueNums(queueNums);
            topicConfig.setPerm(perm);
            topicConfigTable.put(topic, topicConfig);
            logger.info("Created new topic: {} with {} queues", topic, queueNums);
            return true;
        }
        return false;
    }

    public void deleteTopicConfig(String topic) {
        TopicConfig removed = topicConfigTable.remove(topic);
        if (removed != null) {
            // Remove from persistence
            persistence.deleteTopicConfig(topic);

            logger.info("Deleted topic config: {}", topic);
        }
    }

    public ConcurrentMap<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }

    public boolean topicExists(String topic) {
        return topicConfigTable.containsKey(topic);
    }

    public int getTopicCount() {
        return topicConfigTable.size();
    }

    private void loadTopicConfigs() {
        // Load from persistence first
        var persistedConfigs = persistence.loadTopicConfigs();

        // Convert and load persisted configs
        persistedConfigs.forEach((topicName, wrapper) -> {
            TopicConfig config = new TopicConfig();
            config.setTopicName(wrapper.getTopicName());
            config.setReadQueueNums(wrapper.getReadQueueNums());
            config.setWriteQueueNums(wrapper.getWriteQueueNums());
            config.setPerm(wrapper.getPerm());
            try {
                config.setTopicFilterType(TopicFilterType.valueOf(wrapper.getTopicFilterType()));
            } catch (IllegalArgumentException e) {
                config.setTopicFilterType(TopicFilterType.SINGLE_TAG);
            }
            config.setTopicSysFlag(wrapper.getTopicSysFlag());
            config.setOrder(wrapper.isOrder());

            topicConfigTable.put(topicName, config);
        });

        // Create default topics if they don't exist
        createTopicIfNotExists("DefaultTopic", 4, 6);
        createTopicIfNotExists("TestTopic", 4, 6);

        logger.info("Loaded {} topic configs", topicConfigTable.size());
    }

    public static class TopicConfig {
        private String topicName;
        private int readQueueNums = 4;
        private int writeQueueNums = 4;
        private int perm = 6; // Read/Write permission
        private TopicFilterType topicFilterType = TopicFilterType.SINGLE_TAG;
        private int topicSysFlag = 0;
        private boolean order = false;

        public String getTopicName() {
            return topicName;
        }

        public void setTopicName(String topicName) {
            this.topicName = topicName;
        }

        public int getReadQueueNums() {
            return readQueueNums;
        }

        public void setReadQueueNums(int readQueueNums) {
            this.readQueueNums = readQueueNums;
        }

        public int getWriteQueueNums() {
            return writeQueueNums;
        }

        public void setWriteQueueNums(int writeQueueNums) {
            this.writeQueueNums = writeQueueNums;
        }

        public int getPerm() {
            return perm;
        }

        public void setPerm(int perm) {
            this.perm = perm;
        }

        public TopicFilterType getTopicFilterType() {
            return topicFilterType;
        }

        public void setTopicFilterType(TopicFilterType topicFilterType) {
            this.topicFilterType = topicFilterType;
        }

        public int getTopicSysFlag() {
            return topicSysFlag;
        }

        public void setTopicSysFlag(int topicSysFlag) {
            this.topicSysFlag = topicSysFlag;
        }

        public boolean isOrder() {
            return order;
        }

        public void setOrder(boolean order) {
            this.order = order;
        }
    }

    public enum TopicFilterType {
        SINGLE_TAG,
        MULTI_TAG
    }
}