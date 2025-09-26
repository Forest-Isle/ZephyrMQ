package com.zephyr.broker.topic;

import com.zephyr.common.config.BrokerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Topic management service
 * Provides comprehensive topic management capabilities
 */
public class TopicService {

    private static final Logger logger = LoggerFactory.getLogger(TopicService.class);

    private final TopicConfigManager topicConfigManager;
    private final BrokerConfig brokerConfig;

    // Topic routing information cache
    private final ConcurrentMap<String, TopicRouteInfo> topicRouteCache = new ConcurrentHashMap<>();

    public TopicService(TopicConfigManager topicConfigManager, BrokerConfig brokerConfig) {
        this.topicConfigManager = topicConfigManager;
        this.brokerConfig = brokerConfig;
    }

    /**
     * Create a new topic
     *
     * @param request topic creation request
     * @return creation result
     */
    public TopicCreateResult createTopic(TopicCreateRequest request) {
        if (request == null || request.getTopicName() == null || request.getTopicName().trim().isEmpty()) {
            return new TopicCreateResult(false, "Invalid topic name");
        }

        String topicName = request.getTopicName().trim();

        // Validate topic name
        if (!isValidTopicName(topicName)) {
            return new TopicCreateResult(false, "Invalid topic name format");
        }

        // Check if topic already exists
        if (topicConfigManager.getTopicConfig(topicName) != null) {
            return new TopicCreateResult(false, "Topic already exists: " + topicName);
        }

        // Create topic configuration
        TopicConfigManager.TopicConfig topicConfig = new TopicConfigManager.TopicConfig();
        topicConfig.setTopicName(topicName);
        topicConfig.setReadQueueNums(request.getReadQueueNums() > 0 ? request.getReadQueueNums() : 4);
        topicConfig.setWriteQueueNums(request.getWriteQueueNums() > 0 ? request.getWriteQueueNums() : 4);
        topicConfig.setPerm(request.getPerm() > 0 ? request.getPerm() : 6); // Default read/write permission
        topicConfig.setOrder(request.isOrder());
        topicConfig.setTopicFilterType(request.getTopicFilterType() != null ?
                                      request.getTopicFilterType() :
                                      TopicConfigManager.TopicFilterType.SINGLE_TAG);

        // Save topic configuration
        topicConfigManager.updateTopicConfig(topicConfig);

        // Build route info
        buildTopicRouteInfo(topicName);

        logger.info("Topic created successfully: {}", topicName);
        return new TopicCreateResult(true, "Topic created successfully");
    }

    /**
     * Delete a topic
     *
     * @param topicName topic name to delete
     * @return deletion result
     */
    public TopicDeleteResult deleteTopic(String topicName) {
        if (topicName == null || topicName.trim().isEmpty()) {
            return new TopicDeleteResult(false, "Invalid topic name");
        }

        topicName = topicName.trim();

        // Check if topic exists
        if (topicConfigManager.getTopicConfig(topicName) == null) {
            return new TopicDeleteResult(false, "Topic does not exist: " + topicName);
        }

        // Remove topic configuration
        topicConfigManager.deleteTopicConfig(topicName);

        // Remove from route cache
        topicRouteCache.remove(topicName);

        logger.info("Topic deleted successfully: {}", topicName);
        return new TopicDeleteResult(true, "Topic deleted successfully");
    }

    /**
     * Query topic information
     *
     * @param topicName topic name
     * @return topic info
     */
    public TopicInfo queryTopic(String topicName) {
        if (topicName == null || topicName.trim().isEmpty()) {
            return null;
        }

        topicName = topicName.trim();
        TopicConfigManager.TopicConfig topicConfig = topicConfigManager.getTopicConfig(topicName);

        if (topicConfig == null) {
            return null;
        }

        TopicInfo topicInfo = new TopicInfo();
        topicInfo.setTopicName(topicConfig.getTopicName());
        topicInfo.setReadQueueNums(topicConfig.getReadQueueNums());
        topicInfo.setWriteQueueNums(topicConfig.getWriteQueueNums());
        topicInfo.setPerm(topicConfig.getPerm());
        topicInfo.setOrder(topicConfig.isOrder());
        topicInfo.setTopicFilterType(topicConfig.getTopicFilterType());
        topicInfo.setTopicSysFlag(topicConfig.getTopicSysFlag());

        // Add route info
        TopicRouteInfo routeInfo = topicRouteCache.get(topicName);
        if (routeInfo == null) {
            routeInfo = buildTopicRouteInfo(topicName);
        }
        topicInfo.setRouteInfo(routeInfo);

        return topicInfo;
    }

    /**
     * List all topics
     *
     * @return list of topic names
     */
    public List<String> listTopics() {
        return topicConfigManager.getTopicConfigTable().keySet().stream()
                .collect(Collectors.toList());
    }

    /**
     * Update topic configuration
     *
     * @param request topic update request
     * @return update result
     */
    public TopicUpdateResult updateTopic(TopicUpdateRequest request) {
        if (request == null || request.getTopicName() == null || request.getTopicName().trim().isEmpty()) {
            return new TopicUpdateResult(false, "Invalid topic name");
        }

        String topicName = request.getTopicName().trim();
        TopicConfigManager.TopicConfig existingConfig = topicConfigManager.getTopicConfig(topicName);

        if (existingConfig == null) {
            return new TopicUpdateResult(false, "Topic does not exist: " + topicName);
        }

        // Update configuration
        if (request.getReadQueueNums() > 0) {
            existingConfig.setReadQueueNums(request.getReadQueueNums());
        }
        if (request.getWriteQueueNums() > 0) {
            existingConfig.setWriteQueueNums(request.getWriteQueueNums());
        }
        if (request.getPerm() > 0) {
            existingConfig.setPerm(request.getPerm());
        }
        if (request.getTopicFilterType() != null) {
            existingConfig.setTopicFilterType(request.getTopicFilterType());
        }
        existingConfig.setOrder(request.isOrder());

        // Save updated configuration
        topicConfigManager.updateTopicConfig(existingConfig);

        // Refresh route info
        buildTopicRouteInfo(topicName);

        logger.info("Topic updated successfully: {}", topicName);
        return new TopicUpdateResult(true, "Topic updated successfully");
    }

    /**
     * Get topic route information
     *
     * @param topicName topic name
     * @return topic route info
     */
    public TopicRouteInfo getTopicRouteInfo(String topicName) {
        if (topicName == null || topicName.trim().isEmpty()) {
            return null;
        }

        topicName = topicName.trim();
        TopicRouteInfo routeInfo = topicRouteCache.get(topicName);

        if (routeInfo == null) {
            routeInfo = buildTopicRouteInfo(topicName);
        }

        return routeInfo;
    }

    /**
     * Validate topic name format
     *
     * @param topicName topic name
     * @return true if valid
     */
    private boolean isValidTopicName(String topicName) {
        if (topicName == null || topicName.length() == 0) {
            return false;
        }

        // Check length
        if (topicName.length() > 127) {
            return false;
        }

        // Check characters (alphanumeric, underscore, dash)
        return topicName.matches("^[a-zA-Z0-9_-]+$");
    }

    /**
     * Build topic route information
     *
     * @param topicName topic name
     * @return topic route info
     */
    private TopicRouteInfo buildTopicRouteInfo(String topicName) {
        TopicConfigManager.TopicConfig topicConfig = topicConfigManager.getTopicConfig(topicName);
        if (topicConfig == null) {
            return null;
        }

        TopicRouteInfo routeInfo = new TopicRouteInfo();
        routeInfo.setTopicName(topicName);
        routeInfo.setBrokerName(brokerConfig.getBrokerName());
        routeInfo.setReadQueueNums(topicConfig.getReadQueueNums());
        routeInfo.setWriteQueueNums(topicConfig.getWriteQueueNums());

        // Cache the route info
        topicRouteCache.put(topicName, routeInfo);

        return routeInfo;
    }

    // Request/Response/Info classes
    public static class TopicCreateRequest {
        private String topicName;
        private int readQueueNums = 4;
        private int writeQueueNums = 4;
        private int perm = 6;
        private boolean order = false;
        private TopicConfigManager.TopicFilterType topicFilterType = TopicConfigManager.TopicFilterType.SINGLE_TAG;

        // Getters and setters
        public String getTopicName() { return topicName; }
        public void setTopicName(String topicName) { this.topicName = topicName; }
        public int getReadQueueNums() { return readQueueNums; }
        public void setReadQueueNums(int readQueueNums) { this.readQueueNums = readQueueNums; }
        public int getWriteQueueNums() { return writeQueueNums; }
        public void setWriteQueueNums(int writeQueueNums) { this.writeQueueNums = writeQueueNums; }
        public int getPerm() { return perm; }
        public void setPerm(int perm) { this.perm = perm; }
        public boolean isOrder() { return order; }
        public void setOrder(boolean order) { this.order = order; }
        public TopicConfigManager.TopicFilterType getTopicFilterType() { return topicFilterType; }
        public void setTopicFilterType(TopicConfigManager.TopicFilterType topicFilterType) { this.topicFilterType = topicFilterType; }
    }

    public static class TopicUpdateRequest extends TopicCreateRequest {
        // Inherits all fields from create request
    }

    public static class TopicCreateResult {
        private final boolean success;
        private final String message;

        public TopicCreateResult(boolean success, String message) {
            this.success = success;
            this.message = message;
        }

        public boolean isSuccess() { return success; }
        public String getMessage() { return message; }
    }

    public static class TopicDeleteResult {
        private final boolean success;
        private final String message;

        public TopicDeleteResult(boolean success, String message) {
            this.success = success;
            this.message = message;
        }

        public boolean isSuccess() { return success; }
        public String getMessage() { return message; }
    }

    public static class TopicUpdateResult {
        private final boolean success;
        private final String message;

        public TopicUpdateResult(boolean success, String message) {
            this.success = success;
            this.message = message;
        }

        public boolean isSuccess() { return success; }
        public String getMessage() { return message; }
    }

    public static class TopicInfo {
        private String topicName;
        private int readQueueNums;
        private int writeQueueNums;
        private int perm;
        private boolean order;
        private TopicConfigManager.TopicFilterType topicFilterType;
        private int topicSysFlag;
        private TopicRouteInfo routeInfo;

        // Getters and setters
        public String getTopicName() { return topicName; }
        public void setTopicName(String topicName) { this.topicName = topicName; }
        public int getReadQueueNums() { return readQueueNums; }
        public void setReadQueueNums(int readQueueNums) { this.readQueueNums = readQueueNums; }
        public int getWriteQueueNums() { return writeQueueNums; }
        public void setWriteQueueNums(int writeQueueNums) { this.writeQueueNums = writeQueueNums; }
        public int getPerm() { return perm; }
        public void setPerm(int perm) { this.perm = perm; }
        public boolean isOrder() { return order; }
        public void setOrder(boolean order) { this.order = order; }
        public TopicConfigManager.TopicFilterType getTopicFilterType() { return topicFilterType; }
        public void setTopicFilterType(TopicConfigManager.TopicFilterType topicFilterType) { this.topicFilterType = topicFilterType; }
        public int getTopicSysFlag() { return topicSysFlag; }
        public void setTopicSysFlag(int topicSysFlag) { this.topicSysFlag = topicSysFlag; }
        public TopicRouteInfo getRouteInfo() { return routeInfo; }
        public void setRouteInfo(TopicRouteInfo routeInfo) { this.routeInfo = routeInfo; }
    }

    public static class TopicRouteInfo {
        private String topicName;
        private String brokerName;
        private int readQueueNums;
        private int writeQueueNums;

        // Getters and setters
        public String getTopicName() { return topicName; }
        public void setTopicName(String topicName) { this.topicName = topicName; }
        public String getBrokerName() { return brokerName; }
        public void setBrokerName(String brokerName) { this.brokerName = brokerName; }
        public int getReadQueueNums() { return readQueueNums; }
        public void setReadQueueNums(int readQueueNums) { this.readQueueNums = readQueueNums; }
        public int getWriteQueueNums() { return writeQueueNums; }
        public void setWriteQueueNums(int writeQueueNums) { this.writeQueueNums = writeQueueNums; }
    }
}