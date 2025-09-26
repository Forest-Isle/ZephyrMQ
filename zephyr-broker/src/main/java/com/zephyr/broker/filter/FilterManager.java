package com.zephyr.broker.filter;

import com.zephyr.protocol.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.List;
import java.util.ArrayList;

/**
 * Message Filter Manager
 * Manages different types of message filters and applies them to messages
 */
public class FilterManager {

    private static final Logger logger = LoggerFactory.getLogger(FilterManager.class);

    // Available filters: type -> filter implementation
    private final ConcurrentMap<String, MessageFilter> filters = new ConcurrentHashMap<>();

    // Consumer subscriptions: consumerGroup:topic -> filter info
    private final ConcurrentMap<String, FilterSubscription> subscriptions = new ConcurrentHashMap<>();

    public FilterManager() {
        // Register built-in filters
        registerFilter(new TagFilter());
        registerFilter(new SqlFilter());

        logger.info("FilterManager initialized with {} filters", filters.size());
    }

    /**
     * Register a message filter
     *
     * @param filter the filter to register
     */
    public void registerFilter(MessageFilter filter) {
        if (filter != null && filter.getType() != null) {
            filters.put(filter.getType(), filter);
            logger.debug("Registered message filter: {}", filter.getType());
        }
    }

    /**
     * Subscribe consumer to topic with filter
     *
     * @param consumerGroup consumer group
     * @param topic topic name
     * @param filterType filter type (TAG, SQL)
     * @param filterExpression filter expression
     * @return true if subscribed successfully
     */
    public boolean subscribe(String consumerGroup, String topic, String filterType, String filterExpression) {
        if (consumerGroup == null || topic == null) {
            logger.warn("Invalid subscription parameters");
            return false;
        }

        // Default to TAG filter if not specified
        if (filterType == null || filterType.isEmpty()) {
            filterType = "TAG";
            filterExpression = "*"; // Accept all by default
        }

        MessageFilter filter = filters.get(filterType);
        if (filter == null) {
            logger.warn("Unknown filter type: {}", filterType);
            return false;
        }

        // Validate filter expression
        if (!filter.isValidExpression(filterExpression)) {
            logger.warn("Invalid filter expression: {} for type: {}", filterExpression, filterType);
            return false;
        }

        String key = buildSubscriptionKey(consumerGroup, topic);
        FilterSubscription subscription = new FilterSubscription(consumerGroup, topic,
                                                                filterType, filterExpression);
        subscriptions.put(key, subscription);

        logger.info("Consumer subscribed: group={}, topic={}, filter={}:{}",
                   consumerGroup, topic, filterType, filterExpression);
        return true;
    }

    /**
     * Unsubscribe consumer from topic
     *
     * @param consumerGroup consumer group
     * @param topic topic name
     * @return true if unsubscribed successfully
     */
    public boolean unsubscribe(String consumerGroup, String topic) {
        String key = buildSubscriptionKey(consumerGroup, topic);
        FilterSubscription removed = subscriptions.remove(key);

        if (removed != null) {
            logger.info("Consumer unsubscribed: group={}, topic={}", consumerGroup, topic);
            return true;
        } else {
            logger.warn("Subscription not found: group={}, topic={}", consumerGroup, topic);
            return false;
        }
    }

    /**
     * Check if message matches consumer's filter criteria
     *
     * @param message the message to check
     * @param consumerGroup consumer group
     * @return true if message matches filter
     */
    public boolean matches(Message message, String consumerGroup) {
        if (message == null || consumerGroup == null) {
            return false;
        }

        String topic = message.getTopic();
        String key = buildSubscriptionKey(consumerGroup, topic);
        FilterSubscription subscription = subscriptions.get(key);

        if (subscription == null) {
            // No subscription found, default to accept
            logger.debug("No subscription found for group={}, topic={}, accepting message", consumerGroup, topic);
            return true;
        }

        MessageFilter filter = filters.get(subscription.getFilterType());
        if (filter == null) {
            logger.warn("Filter not found: {}, accepting message", subscription.getFilterType());
            return true;
        }

        boolean matches = filter.matches(message, subscription.getFilterExpression());

        logger.debug("Filter result: group={}, topic={}, filter={}:{}, matches={}",
                    consumerGroup, topic, subscription.getFilterType(),
                    subscription.getFilterExpression(), matches);

        return matches;
    }

    /**
     * Get subscription information
     *
     * @param consumerGroup consumer group
     * @param topic topic name
     * @return filter subscription or null if not found
     */
    public FilterSubscription getSubscription(String consumerGroup, String topic) {
        String key = buildSubscriptionKey(consumerGroup, topic);
        return subscriptions.get(key);
    }

    /**
     * Get all subscriptions for consumer group
     *
     * @param consumerGroup consumer group
     * @return list of subscriptions
     */
    public List<FilterSubscription> getSubscriptions(String consumerGroup) {
        return subscriptions.values().stream()
                          .filter(sub -> consumerGroup.equals(sub.getConsumerGroup()))
                          .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    }

    /**
     * Get subscription count
     *
     * @return number of active subscriptions
     */
    public int getSubscriptionCount() {
        return subscriptions.size();
    }

    /**
     * Get available filter types
     *
     * @return array of filter type names
     */
    public String[] getAvailableFilterTypes() {
        return filters.keySet().toArray(new String[0]);
    }

    private String buildSubscriptionKey(String consumerGroup, String topic) {
        return consumerGroup + ":" + topic;
    }

    /**
     * Filter subscription information
     */
    public static class FilterSubscription {
        private final String consumerGroup;
        private final String topic;
        private final String filterType;
        private final String filterExpression;
        private final long createTime;

        public FilterSubscription(String consumerGroup, String topic,
                                String filterType, String filterExpression) {
            this.consumerGroup = consumerGroup;
            this.topic = topic;
            this.filterType = filterType;
            this.filterExpression = filterExpression;
            this.createTime = System.currentTimeMillis();
        }

        // Getters
        public String getConsumerGroup() { return consumerGroup; }
        public String getTopic() { return topic; }
        public String getFilterType() { return filterType; }
        public String getFilterExpression() { return filterExpression; }
        public long getCreateTime() { return createTime; }

        @Override
        public String toString() {
            return "FilterSubscription{" +
                    "consumerGroup='" + consumerGroup + '\'' +
                    ", topic='" + topic + '\'' +
                    ", filterType='" + filterType + '\'' +
                    ", filterExpression='" + filterExpression + '\'' +
                    ", createTime=" + createTime +
                    '}';
        }
    }
}