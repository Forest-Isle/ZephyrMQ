package com.zephyr.broker.subscription;

import com.zephyr.common.config.BrokerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class SubscriptionGroupManager {

    private static final Logger logger = LoggerFactory.getLogger(SubscriptionGroupManager.class);

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final BrokerConfig brokerConfig;
    private final ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable = new ConcurrentHashMap<>();

    public SubscriptionGroupManager(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            logger.info("SubscriptionGroupManager starting...");
            loadSubscriptionGroups();
            logger.info("SubscriptionGroupManager started successfully");
        }
    }

    public void shutdown() {
        logger.info("SubscriptionGroupManager shutting down...");
        logger.info("SubscriptionGroupManager shut down successfully");
    }

    public SubscriptionGroupConfig getSubscriptionGroupConfig(String group) {
        return subscriptionGroupTable.get(group);
    }

    public void updateSubscriptionGroupConfig(SubscriptionGroupConfig config) {
        subscriptionGroupTable.put(config.getGroupName(), config);
        logger.info("Updated subscription group config: {}", config.getGroupName());
    }

    public boolean createSubscriptionGroupIfNotExists(String group) {
        SubscriptionGroupConfig config = subscriptionGroupTable.get(group);
        if (config == null) {
            config = new SubscriptionGroupConfig();
            config.setGroupName(group);
            subscriptionGroupTable.put(group, config);
            logger.info("Created subscription group: {}", group);
            return true;
        }
        return false;
    }

    public void deleteSubscriptionGroupConfig(String group) {
        SubscriptionGroupConfig removed = subscriptionGroupTable.remove(group);
        if (removed != null) {
            logger.info("Deleted subscription group config: {}", group);
        }
    }

    private void loadSubscriptionGroups() {
        createSubscriptionGroupIfNotExists("DefaultConsumerGroup");
        createSubscriptionGroupIfNotExists("example_consumer_group");
        logger.info("Loaded {} subscription groups", subscriptionGroupTable.size());
    }

    public static class SubscriptionGroupConfig {
        private String groupName;
        private boolean consumeEnable = true;
        private boolean consumeFromMinEnable = true;
        private boolean consumeBroadcastEnable = true;
        private int retryQueueNums = 1;
        private int retryMaxTimes = 16;
        private long suspendTimeoutMillis = 60000; // 60s
        private boolean notifyConsumerIdsChangedEnable = true;

        public String getGroupName() {
            return groupName;
        }

        public void setGroupName(String groupName) {
            this.groupName = groupName;
        }

        public boolean isConsumeEnable() {
            return consumeEnable;
        }

        public void setConsumeEnable(boolean consumeEnable) {
            this.consumeEnable = consumeEnable;
        }

        public boolean isConsumeFromMinEnable() {
            return consumeFromMinEnable;
        }

        public void setConsumeFromMinEnable(boolean consumeFromMinEnable) {
            this.consumeFromMinEnable = consumeFromMinEnable;
        }

        public boolean isConsumeBroadcastEnable() {
            return consumeBroadcastEnable;
        }

        public void setConsumeBroadcastEnable(boolean consumeBroadcastEnable) {
            this.consumeBroadcastEnable = consumeBroadcastEnable;
        }

        public int getRetryQueueNums() {
            return retryQueueNums;
        }

        public void setRetryQueueNums(int retryQueueNums) {
            this.retryQueueNums = retryQueueNums;
        }

        public int getRetryMaxTimes() {
            return retryMaxTimes;
        }

        public void setRetryMaxTimes(int retryMaxTimes) {
            this.retryMaxTimes = retryMaxTimes;
        }

        public long getSuspendTimeoutMillis() {
            return suspendTimeoutMillis;
        }

        public void setSuspendTimeoutMillis(long suspendTimeoutMillis) {
            this.suspendTimeoutMillis = suspendTimeoutMillis;
        }

        public boolean isNotifyConsumerIdsChangedEnable() {
            return notifyConsumerIdsChangedEnable;
        }

        public void setNotifyConsumerIdsChangedEnable(boolean notifyConsumerIdsChangedEnable) {
            this.notifyConsumerIdsChangedEnable = notifyConsumerIdsChangedEnable;
        }
    }
}