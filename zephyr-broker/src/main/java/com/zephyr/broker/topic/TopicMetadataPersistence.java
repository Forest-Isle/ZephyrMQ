package com.zephyr.broker.topic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zephyr.common.config.BrokerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Topic metadata persistence manager
 * Handles loading and saving topic configurations to disk
 */
public class TopicMetadataPersistence {

    private static final Logger logger = LoggerFactory.getLogger(TopicMetadataPersistence.class);

    private final BrokerConfig brokerConfig;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService scheduler;
    private final String topicConfigPath;

    // Configuration for persistence
    private final long flushIntervalMs;
    private final boolean enableAutoFlush;

    public TopicMetadataPersistence(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
        this.objectMapper = new ObjectMapper();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "TopicMetadataFlush");
            t.setDaemon(true);
            return t;
        });

        // Configuration
        this.topicConfigPath = brokerConfig.getStorePathRootDir() + "/config/topics.json";
        this.flushIntervalMs = 10000; // 10 seconds
        this.enableAutoFlush = true;
    }

    /**
     * Start the persistence service
     */
    public void start() {
        logger.info("Starting TopicMetadataPersistence...");

        // Create config directory if not exists
        createConfigDirectory();

        // Start auto flush scheduler if enabled
        if (enableAutoFlush) {
            scheduler.scheduleAtFixedRate(this::flushAll,
                flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
            logger.info("Auto flush enabled with interval: {}ms", flushIntervalMs);
        }

        logger.info("TopicMetadataPersistence started successfully");
    }

    /**
     * Shutdown the persistence service
     */
    public void shutdown() {
        logger.info("Shutting down TopicMetadataPersistence...");

        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // Final flush
        flushAll();

        logger.info("TopicMetadataPersistence shut down successfully");
    }

    /**
     * Load topic configurations from disk
     *
     * @return map of topic configurations
     */
    @SuppressWarnings("unchecked")
    public Map<String, TopicConfigWrapper> loadTopicConfigs() {
        Map<String, TopicConfigWrapper> configs = new ConcurrentHashMap<>();

        try {
            File configFile = new File(topicConfigPath);
            if (!configFile.exists()) {
                logger.info("Topic config file does not exist, starting with empty configuration");
                return configs;
            }

            String json = Files.readString(configFile.toPath());
            if (json == null || json.trim().isEmpty()) {
                logger.warn("Topic config file is empty");
                return configs;
            }

            TopicConfigContainer container = objectMapper.readValue(json, TopicConfigContainer.class);
            if (container != null && container.getTopicConfigs() != null) {
                configs.putAll(container.getTopicConfigs());
                logger.info("Loaded {} topic configurations from disk", configs.size());
            }

        } catch (IOException e) {
            logger.error("Failed to load topic configurations from disk", e);
        } catch (Exception e) {
            logger.error("Unexpected error while loading topic configurations", e);
        }

        return configs;
    }

    /**
     * Save topic configurations to disk
     *
     * @param topicConfigs topic configurations to save
     */
    public void saveTopicConfigs(Map<String, TopicConfigManager.TopicConfig> topicConfigs) {
        if (topicConfigs == null || topicConfigs.isEmpty()) {
            logger.debug("No topic configs to save");
            return;
        }

        try {
            // Convert to wrapper format
            Map<String, TopicConfigWrapper> wrapperConfigs = new ConcurrentHashMap<>();
            topicConfigs.forEach((key, config) -> {
                TopicConfigWrapper wrapper = new TopicConfigWrapper();
                wrapper.setTopicName(config.getTopicName());
                wrapper.setReadQueueNums(config.getReadQueueNums());
                wrapper.setWriteQueueNums(config.getWriteQueueNums());
                wrapper.setPerm(config.getPerm());
                wrapper.setTopicFilterType(config.getTopicFilterType().name());
                wrapper.setTopicSysFlag(config.getTopicSysFlag());
                wrapper.setOrder(config.isOrder());
                wrapper.setCreateTime(System.currentTimeMillis());
                wrapper.setUpdateTime(System.currentTimeMillis());

                wrapperConfigs.put(key, wrapper);
            });

            TopicConfigContainer container = new TopicConfigContainer();
            container.setTopicConfigs(wrapperConfigs);
            container.setVersion(1);
            container.setSaveTime(System.currentTimeMillis());

            // Create parent directories if needed
            createConfigDirectory();

            // Write to temp file first, then rename for atomicity
            String tempPath = topicConfigPath + ".tmp";
            String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(container);

            Files.writeString(Paths.get(tempPath), json);

            // Atomic rename
            Files.move(Paths.get(tempPath), Paths.get(topicConfigPath));

            logger.debug("Saved {} topic configurations to disk", topicConfigs.size());

        } catch (IOException e) {
            logger.error("Failed to save topic configurations to disk", e);
        } catch (Exception e) {
            logger.error("Unexpected error while saving topic configurations", e);
        }
    }

    /**
     * Save a single topic configuration
     *
     * @param topicName topic name
     * @param topicConfig topic configuration
     */
    public void saveTopicConfig(String topicName, TopicConfigManager.TopicConfig topicConfig) {
        // Load existing configs
        Map<String, TopicConfigWrapper> existingConfigs = loadTopicConfigs();

        // Convert and add the new config
        TopicConfigWrapper wrapper = new TopicConfigWrapper();
        wrapper.setTopicName(topicConfig.getTopicName());
        wrapper.setReadQueueNums(topicConfig.getReadQueueNums());
        wrapper.setWriteQueueNums(topicConfig.getWriteQueueNums());
        wrapper.setPerm(topicConfig.getPerm());
        wrapper.setTopicFilterType(topicConfig.getTopicFilterType().name());
        wrapper.setTopicSysFlag(topicConfig.getTopicSysFlag());
        wrapper.setOrder(topicConfig.isOrder());
        wrapper.setCreateTime(existingConfigs.containsKey(topicName) ?
            existingConfigs.get(topicName).getCreateTime() : System.currentTimeMillis());
        wrapper.setUpdateTime(System.currentTimeMillis());

        existingConfigs.put(topicName, wrapper);

        // Convert back to TopicConfig format for saving
        Map<String, TopicConfigManager.TopicConfig> configsToSave = new ConcurrentHashMap<>();
        existingConfigs.forEach((key, wrapperConfig) -> {
            TopicConfigManager.TopicConfig config = new TopicConfigManager.TopicConfig();
            config.setTopicName(wrapperConfig.getTopicName());
            config.setReadQueueNums(wrapperConfig.getReadQueueNums());
            config.setWriteQueueNums(wrapperConfig.getWriteQueueNums());
            config.setPerm(wrapperConfig.getPerm());
            config.setTopicFilterType(TopicConfigManager.TopicFilterType.valueOf(wrapperConfig.getTopicFilterType()));
            config.setTopicSysFlag(wrapperConfig.getTopicSysFlag());
            config.setOrder(wrapperConfig.isOrder());
            configsToSave.put(key, config);
        });

        saveTopicConfigs(configsToSave);
    }

    /**
     * Delete a topic configuration from disk
     *
     * @param topicName topic name to delete
     */
    public void deleteTopicConfig(String topicName) {
        Map<String, TopicConfigWrapper> existingConfigs = loadTopicConfigs();
        if (existingConfigs.remove(topicName) != null) {
            // Convert back and save
            Map<String, TopicConfigManager.TopicConfig> configsToSave = new ConcurrentHashMap<>();
            existingConfigs.forEach((key, wrapperConfig) -> {
                TopicConfigManager.TopicConfig config = new TopicConfigManager.TopicConfig();
                config.setTopicName(wrapperConfig.getTopicName());
                config.setReadQueueNums(wrapperConfig.getReadQueueNums());
                config.setWriteQueueNums(wrapperConfig.getWriteQueueNums());
                config.setPerm(wrapperConfig.getPerm());
                config.setTopicFilterType(TopicConfigManager.TopicFilterType.valueOf(wrapperConfig.getTopicFilterType()));
                config.setTopicSysFlag(wrapperConfig.getTopicSysFlag());
                config.setOrder(wrapperConfig.isOrder());
                configsToSave.put(key, config);
            });

            saveTopicConfigs(configsToSave);
            logger.info("Deleted topic config from disk: {}", topicName);
        }
    }

    /**
     * Flush all topic configurations (called by scheduler)
     */
    private void flushAll() {
        // This would be called by TopicConfigManager when it needs to persist
        logger.debug("Auto flush triggered");
    }

    /**
     * Create config directory if it doesn't exist
     */
    private void createConfigDirectory() {
        try {
            Path configDir = Paths.get(topicConfigPath).getParent();
            if (!Files.exists(configDir)) {
                Files.createDirectories(configDir);
                logger.info("Created config directory: {}", configDir);
            }
        } catch (IOException e) {
            logger.error("Failed to create config directory", e);
        }
    }

    // Wrapper classes for JSON serialization
    public static class TopicConfigContainer {
        private Map<String, TopicConfigWrapper> topicConfigs;
        private int version;
        private long saveTime;

        public Map<String, TopicConfigWrapper> getTopicConfigs() { return topicConfigs; }
        public void setTopicConfigs(Map<String, TopicConfigWrapper> topicConfigs) { this.topicConfigs = topicConfigs; }
        public int getVersion() { return version; }
        public void setVersion(int version) { this.version = version; }
        public long getSaveTime() { return saveTime; }
        public void setSaveTime(long saveTime) { this.saveTime = saveTime; }
    }

    public static class TopicConfigWrapper {
        private String topicName;
        private int readQueueNums;
        private int writeQueueNums;
        private int perm;
        private String topicFilterType;
        private int topicSysFlag;
        private boolean order;
        private long createTime;
        private long updateTime;

        // Getters and setters
        public String getTopicName() { return topicName; }
        public void setTopicName(String topicName) { this.topicName = topicName; }
        public int getReadQueueNums() { return readQueueNums; }
        public void setReadQueueNums(int readQueueNums) { this.readQueueNums = readQueueNums; }
        public int getWriteQueueNums() { return writeQueueNums; }
        public void setWriteQueueNums(int writeQueueNums) { this.writeQueueNums = writeQueueNums; }
        public int getPerm() { return perm; }
        public void setPerm(int perm) { this.perm = perm; }
        public String getTopicFilterType() { return topicFilterType; }
        public void setTopicFilterType(String topicFilterType) { this.topicFilterType = topicFilterType; }
        public int getTopicSysFlag() { return topicSysFlag; }
        public void setTopicSysFlag(int topicSysFlag) { this.topicSysFlag = topicSysFlag; }
        public boolean isOrder() { return order; }
        public void setOrder(boolean order) { this.order = order; }
        public long getCreateTime() { return createTime; }
        public void setCreateTime(long createTime) { this.createTime = createTime; }
        public long getUpdateTime() { return updateTime; }
        public void setUpdateTime(long updateTime) { this.updateTime = updateTime; }
    }
}