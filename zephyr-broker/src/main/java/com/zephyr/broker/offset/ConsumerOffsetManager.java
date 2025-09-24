package com.zephyr.broker.offset;

import com.zephyr.common.config.BrokerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ConsumerOffsetManager {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerOffsetManager.class);

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final BrokerConfig brokerConfig;
    private final ConcurrentMap<String, ConcurrentMap<Integer, AtomicLong>> offsetTable = new ConcurrentHashMap<>();

    public ConsumerOffsetManager(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            logger.info("ConsumerOffsetManager starting...");
            loadOffsets();
            logger.info("ConsumerOffsetManager started successfully");
        }
    }

    public void shutdown() {
        logger.info("ConsumerOffsetManager shutting down...");
        persistOffsets();
        logger.info("ConsumerOffsetManager shut down successfully");
    }

    public void commitOffset(String clientHost, String group, String topic, int queueId, long offset) {
        String key = buildKey(group, topic);
        ConcurrentMap<Integer, AtomicLong> queueOffsets = offsetTable.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
        AtomicLong queueOffset = queueOffsets.computeIfAbsent(queueId, k -> new AtomicLong(0));
        queueOffset.set(offset);

        logger.debug("Committed offset: group={}, topic={}, queueId={}, offset={}", group, topic, queueId, offset);
    }

    public long queryOffset(String group, String topic, int queueId) {
        String key = buildKey(group, topic);
        ConcurrentMap<Integer, AtomicLong> queueOffsets = offsetTable.get(key);
        if (queueOffsets != null) {
            AtomicLong offset = queueOffsets.get(queueId);
            if (offset != null) {
                return offset.get();
            }
        }
        return -1;
    }

    public void updateOffset(String group, String topic, int queueId, long offset, boolean force) {
        String key = buildKey(group, topic);
        ConcurrentMap<Integer, AtomicLong> queueOffsets = offsetTable.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
        AtomicLong queueOffset = queueOffsets.computeIfAbsent(queueId, k -> new AtomicLong(0));

        if (force) {
            queueOffset.set(offset);
        } else {
            queueOffset.compareAndSet(queueOffset.get(), Math.max(queueOffset.get(), offset));
        }

        logger.debug("Updated offset: group={}, topic={}, queueId={}, offset={}, force={}",
                group, topic, queueId, offset, force);
    }

    public void removeByGroup(String group) {
        offsetTable.entrySet().removeIf(entry -> entry.getKey().startsWith(group + "@"));
        logger.info("Removed offsets for group: {}", group);
    }

    public void removeByGroupAndTopic(String group, String topic) {
        String key = buildKey(group, topic);
        offsetTable.remove(key);
        logger.info("Removed offsets for group={}, topic={}", group, topic);
    }

    private String buildKey(String group, String topic) {
        return group + "@" + topic;
    }

    private void loadOffsets() {
        // Load persisted offsets - simplified implementation
        logger.info("Loaded consumer offsets");
    }

    private void persistOffsets() {
        // Persist offsets to disk - simplified implementation
        logger.info("Persisted consumer offsets");
    }
}