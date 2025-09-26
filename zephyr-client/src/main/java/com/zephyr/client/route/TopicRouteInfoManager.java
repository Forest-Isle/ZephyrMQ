package com.zephyr.client.route;

import com.zephyr.protocol.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Client-side topic route information manager
 * Caches topic routing information and provides queue selection
 */
public class TopicRouteInfoManager {

    private static final Logger logger = LoggerFactory.getLogger(TopicRouteInfoManager.class);

    // Cache for topic route information
    private final ConcurrentMap<String, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();

    // Default broker and queue configuration
    private String defaultBrokerName = "defaultBroker";
    private int defaultQueueCount = 4;

    /**
     * Get or create topic route data
     *
     * @param topic topic name
     * @return topic route data
     */
    public TopicRouteData getTopicRouteData(String topic) {
        return getTopicRouteData(topic, true);
    }

    /**
     * Get topic route data
     *
     * @param topic topic name
     * @param createIfAbsent whether to create if not exists
     * @return topic route data
     */
    public TopicRouteData getTopicRouteData(String topic, boolean createIfAbsent) {
        if (topic == null || topic.isEmpty()) {
            return null;
        }

        TopicRouteData routeData = topicRouteTable.get(topic);
        if (routeData == null && createIfAbsent) {
            routeData = createDefaultRouteData(topic);
            topicRouteTable.put(topic, routeData);
            logger.info("Created default route data for topic: {}", topic);
        }

        return routeData;
    }

    /**
     * Update topic route data from nameserver
     *
     * @param topic topic name
     * @param routeData route data from nameserver
     */
    public void updateTopicRouteData(String topic, TopicRouteData routeData) {
        if (topic != null && routeData != null) {
            topicRouteTable.put(topic, routeData);
            logger.debug("Updated route data for topic: {} with {} queues",
                        topic, routeData.getMessageQueues().size());
        }
    }

    /**
     * Get message queues for publish
     *
     * @param topic topic name
     * @return list of message queues for publishing
     */
    public List<MessageQueue> getPublishMessageQueues(String topic) {
        TopicRouteData routeData = getTopicRouteData(topic);
        if (routeData == null) {
            return new ArrayList<>();
        }

        return routeData.getMessageQueues().stream()
                .filter(MessageQueue::isWritable)
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    }

    /**
     * Get message queues for subscribe
     *
     * @param topic topic name
     * @return list of message queues for subscribing
     */
    public List<MessageQueue> getSubscribeMessageQueues(String topic) {
        TopicRouteData routeData = getTopicRouteData(topic);
        if (routeData == null) {
            return new ArrayList<>();
        }

        return routeData.getMessageQueues().stream()
                .filter(MessageQueue::isReadable)
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    }

    /**
     * Clear cached route data for topic
     *
     * @param topic topic name
     */
    public void clearTopicRouteData(String topic) {
        topicRouteTable.remove(topic);
        logger.info("Cleared route data for topic: {}", topic);
    }

    /**
     * Clear all cached route data
     */
    public void clearAllRouteData() {
        topicRouteTable.clear();
        logger.info("Cleared all route data");
    }

    /**
     * Get cached topic count
     *
     * @return number of cached topics
     */
    public int getCachedTopicCount() {
        return topicRouteTable.size();
    }

    /**
     * Create default route data for topic
     *
     * @param topic topic name
     * @return default route data
     */
    private TopicRouteData createDefaultRouteData(String topic) {
        TopicRouteData routeData = new TopicRouteData();
        routeData.setTopic(topic);

        List<MessageQueue> messageQueues = new ArrayList<>();
        for (int i = 0; i < defaultQueueCount; i++) {
            MessageQueue mq = new MessageQueue(topic, defaultBrokerName, i);
            mq.setWritable(true);
            mq.setReadable(true);
            messageQueues.add(mq);
        }
        routeData.setMessageQueues(messageQueues);

        return routeData;
    }

    // Getters and setters
    public String getDefaultBrokerName() {
        return defaultBrokerName;
    }

    public void setDefaultBrokerName(String defaultBrokerName) {
        this.defaultBrokerName = defaultBrokerName;
    }

    public int getDefaultQueueCount() {
        return defaultQueueCount;
    }

    public void setDefaultQueueCount(int defaultQueueCount) {
        this.defaultQueueCount = defaultQueueCount;
    }

    /**
     * Topic route data
     */
    public static class TopicRouteData {
        private String topic;
        private List<MessageQueue> messageQueues = new ArrayList<>();
        private long updateTime;

        public TopicRouteData() {
            this.updateTime = System.currentTimeMillis();
        }

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        public List<MessageQueue> getMessageQueues() { return messageQueues; }
        public void setMessageQueues(List<MessageQueue> messageQueues) { this.messageQueues = messageQueues; }
        public long getUpdateTime() { return updateTime; }
        public void setUpdateTime(long updateTime) { this.updateTime = updateTime; }
    }
}