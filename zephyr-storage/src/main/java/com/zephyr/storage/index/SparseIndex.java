package com.zephyr.storage.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 稀疏索引实现
 * 用于快速定位消息在CommitLog中的位置，提升查询性能
 */
public class SparseIndex {

    private static final Logger logger = LoggerFactory.getLogger(SparseIndex.class);

    /**
     * 索引项
     */
    public static class IndexEntry {
        private final long logicalOffset;    // 逻辑偏移量（消息在队列中的位置）
        private final long physicalOffset;   // 物理偏移量（消息在CommitLog中的位置）
        private final long timestamp;        // 消息时间戳
        private final int messageSize;       // 消息大小
        private final String topic;          // 主题名称
        private final int queueId;          // 队列ID

        public IndexEntry(long logicalOffset, long physicalOffset, long timestamp,
                         int messageSize, String topic, int queueId) {
            this.logicalOffset = logicalOffset;
            this.physicalOffset = physicalOffset;
            this.timestamp = timestamp;
            this.messageSize = messageSize;
            this.topic = topic;
            this.queueId = queueId;
        }

        // Getters
        public long getLogicalOffset() { return logicalOffset; }
        public long getPhysicalOffset() { return physicalOffset; }
        public long getTimestamp() { return timestamp; }
        public int getMessageSize() { return messageSize; }
        public String getTopic() { return topic; }
        public int getQueueId() { return queueId; }

        @Override
        public String toString() {
            return String.format("IndexEntry{logical=%d, physical=%d, timestamp=%d, size=%d, topic='%s', queueId=%d}",
                               logicalOffset, physicalOffset, timestamp, messageSize, topic, queueId);
        }
    }

    /**
     * 查询结果范围
     */
    public static class QueryRange {
        private final IndexEntry startEntry;
        private final IndexEntry endEntry;
        private final long startPhysicalOffset;
        private final long endPhysicalOffset;

        public QueryRange(IndexEntry startEntry, IndexEntry endEntry) {
            this.startEntry = startEntry;
            this.endEntry = endEntry;
            this.startPhysicalOffset = startEntry != null ? startEntry.getPhysicalOffset() : -1;
            this.endPhysicalOffset = endEntry != null ? endEntry.getPhysicalOffset() : -1;
        }

        public IndexEntry getStartEntry() { return startEntry; }
        public IndexEntry getEndEntry() { return endEntry; }
        public long getStartPhysicalOffset() { return startPhysicalOffset; }
        public long getEndPhysicalOffset() { return endPhysicalOffset; }

        public boolean isValid() {
            return startEntry != null && endEntry != null &&
                   startPhysicalOffset <= endPhysicalOffset;
        }

        @Override
        public String toString() {
            return String.format("QueryRange{start=%d, end=%d, entries=[%s, %s]}",
                               startPhysicalOffset, endPhysicalOffset, startEntry, endEntry);
        }
    }

    // 索引间隔：每隔N条消息创建一个索引项
    private final int indexInterval;
    // 最大索引项数量
    private final int maxIndexEntries;

    // 基于逻辑偏移量的索引（用于顺序查询）
    private final ConcurrentSkipListMap<Long, IndexEntry> logicalOffsetIndex = new ConcurrentSkipListMap<>();
    // 基于时间戳的索引（用于时间范围查询）
    private final ConcurrentSkipListMap<Long, IndexEntry> timestampIndex = new ConcurrentSkipListMap<>();
    // 基于物理偏移量的索引（用于物理位置查询）
    private final ConcurrentSkipListMap<Long, IndexEntry> physicalOffsetIndex = new ConcurrentSkipListMap<>();

    // 统计信息
    private final AtomicLong totalIndexEntries = new AtomicLong(0);
    private final AtomicLong totalIndexHits = new AtomicLong(0);
    private final AtomicLong totalQueries = new AtomicLong(0);

    public SparseIndex(int indexInterval, int maxIndexEntries) {
        this.indexInterval = Math.max(1, indexInterval);
        this.maxIndexEntries = Math.max(100, maxIndexEntries);

        logger.info("SparseIndex initialized: interval={}, maxEntries={}",
                   this.indexInterval, this.maxIndexEntries);
    }

    public SparseIndex() {
        this(100, 100000); // 默认每100条消息一个索引，最多10万个索引项
    }

    /**
     * 添加索引项
     */
    public void addIndexEntry(long logicalOffset, long physicalOffset, long timestamp,
                             int messageSize, String topic, int queueId) {
        // 检查是否需要创建索引项（按间隔创建稀疏索引）
        if (!shouldCreateIndex(logicalOffset)) {
            return;
        }

        IndexEntry entry = new IndexEntry(logicalOffset, physicalOffset, timestamp,
                                         messageSize, topic, queueId);

        // 添加到各个索引
        logicalOffsetIndex.put(logicalOffset, entry);
        timestampIndex.put(timestamp, entry);
        physicalOffsetIndex.put(physicalOffset, entry);

        totalIndexEntries.incrementAndGet();

        // 检查索引容量，清理旧索引
        if (totalIndexEntries.get() > maxIndexEntries) {
            cleanupOldEntries();
        }

        logger.debug("Added index entry: {}", entry);
    }

    /**
     * 根据逻辑偏移量查找最近的索引项
     */
    public IndexEntry findByLogicalOffset(long logicalOffset) {
        totalQueries.incrementAndGet();

        // 查找小于等于目标偏移量的最大索引项
        ConcurrentNavigableMap<Long, IndexEntry> headMap = logicalOffsetIndex.headMap(logicalOffset, true);
        if (!headMap.isEmpty()) {
            IndexEntry entry = headMap.lastEntry().getValue();
            totalIndexHits.incrementAndGet();
            logger.debug("Index hit for logical offset {}: {}", logicalOffset, entry);
            return entry;
        }

        logger.debug("No index found for logical offset: {}", logicalOffset);
        return null;
    }

    /**
     * 根据物理偏移量查找最近的索引项
     */
    public IndexEntry findByPhysicalOffset(long physicalOffset) {
        totalQueries.incrementAndGet();

        ConcurrentNavigableMap<Long, IndexEntry> headMap = physicalOffsetIndex.headMap(physicalOffset, true);
        if (!headMap.isEmpty()) {
            IndexEntry entry = headMap.lastEntry().getValue();
            totalIndexHits.incrementAndGet();
            logger.debug("Index hit for physical offset {}: {}", physicalOffset, entry);
            return entry;
        }

        logger.debug("No index found for physical offset: {}", physicalOffset);
        return null;
    }

    /**
     * 根据时间戳查找最近的索引项
     */
    public IndexEntry findByTimestamp(long timestamp) {
        totalQueries.incrementAndGet();

        ConcurrentNavigableMap<Long, IndexEntry> headMap = timestampIndex.headMap(timestamp, true);
        if (!headMap.isEmpty()) {
            IndexEntry entry = headMap.lastEntry().getValue();
            totalIndexHits.incrementAndGet();
            logger.debug("Index hit for timestamp {}: {}", timestamp, entry);
            return entry;
        }

        logger.debug("No index found for timestamp: {}", timestamp);
        return null;
    }

    /**
     * 时间范围查询
     */
    public QueryRange queryByTimeRange(long startTime, long endTime) {
        totalQueries.incrementAndGet();

        // 查找开始时间的索引
        IndexEntry startEntry = findByTimestamp(startTime);

        // 查找结束时间的索引
        ConcurrentNavigableMap<Long, IndexEntry> tailMap = timestampIndex.tailMap(endTime, true);
        IndexEntry endEntry = null;
        if (!tailMap.isEmpty()) {
            endEntry = tailMap.firstEntry().getValue();
        } else {
            // 如果没有找到大于等于结束时间的索引，使用最后一个索引
            if (!timestampIndex.isEmpty()) {
                endEntry = timestampIndex.lastEntry().getValue();
            }
        }

        QueryRange range = new QueryRange(startEntry, endEntry);
        logger.debug("Time range query [{}, {}]: {}", startTime, endTime, range);
        return range;
    }

    /**
     * 逻辑偏移量范围查询
     */
    public QueryRange queryByLogicalRange(long startOffset, long endOffset) {
        totalQueries.incrementAndGet();

        IndexEntry startEntry = findByLogicalOffset(startOffset);

        ConcurrentNavigableMap<Long, IndexEntry> tailMap = logicalOffsetIndex.tailMap(endOffset, true);
        IndexEntry endEntry = null;
        if (!tailMap.isEmpty()) {
            endEntry = tailMap.firstEntry().getValue();
        } else {
            if (!logicalOffsetIndex.isEmpty()) {
                endEntry = logicalOffsetIndex.lastEntry().getValue();
            }
        }

        QueryRange range = new QueryRange(startEntry, endEntry);
        logger.debug("Logical range query [{}, {}]: {}", startOffset, endOffset, range);
        return range;
    }

    /**
     * 物理偏移量范围查询
     */
    public QueryRange queryByPhysicalRange(long startOffset, long endOffset) {
        totalQueries.incrementAndGet();

        IndexEntry startEntry = findByPhysicalOffset(startOffset);

        ConcurrentNavigableMap<Long, IndexEntry> tailMap = physicalOffsetIndex.tailMap(endOffset, true);
        IndexEntry endEntry = null;
        if (!tailMap.isEmpty()) {
            endEntry = tailMap.firstEntry().getValue();
        } else {
            if (!physicalOffsetIndex.isEmpty()) {
                endEntry = physicalOffsetIndex.lastEntry().getValue();
            }
        }

        QueryRange range = new QueryRange(startEntry, endEntry);
        logger.debug("Physical range query [{}, {}]: {}", startOffset, endOffset, range);
        return range;
    }

    /**
     * 获取最新的索引项
     */
    public IndexEntry getLatestEntry() {
        if (logicalOffsetIndex.isEmpty()) {
            return null;
        }
        return logicalOffsetIndex.lastEntry().getValue();
    }

    /**
     * 获取最早的索引项
     */
    public IndexEntry getEarliestEntry() {
        if (logicalOffsetIndex.isEmpty()) {
            return null;
        }
        return logicalOffsetIndex.firstEntry().getValue();
    }

    /**
     * 判断是否应该创建索引
     */
    private boolean shouldCreateIndex(long logicalOffset) {
        // 第一条消息总是创建索引
        if (logicalOffset == 0) {
            return true;
        }

        // 按间隔创建索引
        return (logicalOffset % indexInterval) == 0;
    }

    /**
     * 清理旧的索引项
     */
    private void cleanupOldEntries() {
        int entriesToRemove = (int) (totalIndexEntries.get() - maxIndexEntries * 0.8); // 清理到80%
        if (entriesToRemove <= 0) {
            return;
        }

        logger.info("Cleaning up {} old index entries", entriesToRemove);

        int removed = 0;
        var iterator = logicalOffsetIndex.entrySet().iterator();
        while (iterator.hasNext() && removed < entriesToRemove) {
            var entry = iterator.next();
            IndexEntry indexEntry = entry.getValue();

            // 从所有索引中移除
            iterator.remove();
            timestampIndex.remove(indexEntry.getTimestamp());
            physicalOffsetIndex.remove(indexEntry.getPhysicalOffset());

            removed++;
        }

        totalIndexEntries.addAndGet(-removed);
        logger.info("Cleaned up {} index entries, remaining: {}", removed, totalIndexEntries.get());
    }

    /**
     * 清空所有索引
     */
    public void clear() {
        logicalOffsetIndex.clear();
        timestampIndex.clear();
        physicalOffsetIndex.clear();
        totalIndexEntries.set(0);
        totalIndexHits.set(0);
        totalQueries.set(0);

        logger.info("SparseIndex cleared");
    }

    /**
     * 获取索引统计信息
     */
    public IndexStatistics getStatistics() {
        return new IndexStatistics(
            totalIndexEntries.get(),
            totalQueries.get(),
            totalIndexHits.get(),
            indexInterval,
            maxIndexEntries
        );
    }

    /**
     * 索引统计信息
     */
    public static class IndexStatistics {
        private final long totalEntries;
        private final long totalQueries;
        private final long totalHits;
        private final int indexInterval;
        private final int maxIndexEntries;

        public IndexStatistics(long totalEntries, long totalQueries, long totalHits,
                             int indexInterval, int maxIndexEntries) {
            this.totalEntries = totalEntries;
            this.totalQueries = totalQueries;
            this.totalHits = totalHits;
            this.indexInterval = indexInterval;
            this.maxIndexEntries = maxIndexEntries;
        }

        public long getTotalEntries() { return totalEntries; }
        public long getTotalQueries() { return totalQueries; }
        public long getTotalHits() { return totalHits; }
        public int getIndexInterval() { return indexInterval; }
        public int getMaxIndexEntries() { return maxIndexEntries; }

        public double getHitRatio() {
            return totalQueries > 0 ? (double) totalHits / totalQueries : 0.0;
        }

        public double getCompressionRatio() {
            // 假设如果没有稀疏索引，每条消息都需要一个索引项
            // 压缩率 = 实际索引项数 / 理论最大索引项数
            long theoreticalMaxEntries = totalQueries * indexInterval;
            return theoreticalMaxEntries > 0 ? (double) totalEntries / theoreticalMaxEntries : 1.0;
        }

        @Override
        public String toString() {
            return String.format("IndexStatistics{entries=%d, queries=%d, hits=%d, hitRatio=%.2f%%, compressionRatio=%.2f%%}",
                               totalEntries, totalQueries, totalHits, getHitRatio() * 100, getCompressionRatio() * 100);
        }
    }

    /**
     * 序列化索引到字节数组（用于持久化）
     */
    public byte[] serialize() {
        // 计算需要的空间
        int entrySize = 8 + 8 + 8 + 4 + 4 + 4; // long*3 + int*3 + topic length + topic bytes + queueId
        int estimatedSize = 4 + (int) totalIndexEntries.get() * (entrySize + 50); // 预估topic平均长度50

        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);

        // 写入索引项数量
        buffer.putInt((int) totalIndexEntries.get());

        // 写入每个索引项
        for (IndexEntry entry : logicalOffsetIndex.values()) {
            buffer.putLong(entry.getLogicalOffset());
            buffer.putLong(entry.getPhysicalOffset());
            buffer.putLong(entry.getTimestamp());
            buffer.putInt(entry.getMessageSize());
            buffer.putInt(entry.getQueueId());

            // 写入topic
            byte[] topicBytes = entry.getTopic().getBytes();
            buffer.putInt(topicBytes.length);
            buffer.put(topicBytes);
        }

        // 返回实际使用的字节
        byte[] result = new byte[buffer.position()];
        buffer.rewind();
        buffer.get(result);

        logger.info("Serialized {} index entries to {} bytes", totalIndexEntries.get(), result.length);
        return result;
    }

    /**
     * 从字节数组反序列化索引（用于恢复）
     */
    public void deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            return;
        }

        clear(); // 清空现有索引

        ByteBuffer buffer = ByteBuffer.wrap(data);

        try {
            // 读取索引项数量
            int entryCount = buffer.getInt();
            logger.info("Deserializing {} index entries from {} bytes", entryCount, data.length);

            // 读取每个索引项
            for (int i = 0; i < entryCount; i++) {
                long logicalOffset = buffer.getLong();
                long physicalOffset = buffer.getLong();
                long timestamp = buffer.getLong();
                int messageSize = buffer.getInt();
                int queueId = buffer.getInt();

                // 读取topic
                int topicLength = buffer.getInt();
                byte[] topicBytes = new byte[topicLength];
                buffer.get(topicBytes);
                String topic = new String(topicBytes);

                // 重建索引项
                IndexEntry entry = new IndexEntry(logicalOffset, physicalOffset, timestamp,
                                                messageSize, topic, queueId);
                logicalOffsetIndex.put(logicalOffset, entry);
                timestampIndex.put(timestamp, entry);
                physicalOffsetIndex.put(physicalOffset, entry);
                totalIndexEntries.incrementAndGet();
            }

            logger.info("Successfully deserialized {} index entries", totalIndexEntries.get());

        } catch (Exception e) {
            logger.error("Error deserializing index data", e);
            clear(); // 出错时清空，避免不一致状态
        }
    }
}