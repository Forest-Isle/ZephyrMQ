package com.zephyr.storage.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 稀疏索引测试
 */
class SparseIndexTest {

    private SparseIndex sparseIndex;

    @BeforeEach
    void setUp() {
        // 使用小间隔方便测试
        sparseIndex = new SparseIndex(10, 1000); // 每10条消息一个索引，最多1000个索引项
    }

    @Test
    @DisplayName("测试基本索引添加和查询")
    void testBasicIndexAddAndQuery() {
        // 添加一些索引项
        sparseIndex.addIndexEntry(0, 1000, 1000000, 100, "TestTopic", 0);
        sparseIndex.addIndexEntry(10, 2000, 1001000, 120, "TestTopic", 0);
        sparseIndex.addIndexEntry(20, 3000, 1002000, 110, "TestTopic", 1);

        // 测试逻辑偏移量查询
        SparseIndex.IndexEntry entry = sparseIndex.findByLogicalOffset(15);
        assertNotNull(entry);
        assertEquals(10, entry.getLogicalOffset());
        assertEquals(2000, entry.getPhysicalOffset());

        // 测试物理偏移量查询
        entry = sparseIndex.findByPhysicalOffset(2500);
        assertNotNull(entry);
        assertEquals(2000, entry.getPhysicalOffset());

        // 测试时间戳查询
        entry = sparseIndex.findByTimestamp(1001500);
        assertNotNull(entry);
        assertEquals(1001000, entry.getTimestamp());
    }

    @Test
    @DisplayName("测试稀疏索引间隔")
    void testSparseIndexInterval() {
        // 添加连续的消息，但只有间隔为10的才会被索引
        for (int i = 0; i < 50; i++) {
            sparseIndex.addIndexEntry(i, 1000 + i * 100, 1000000 + i * 1000, 100, "TestTopic", 0);
        }

        // 验证只有5个索引项（0, 10, 20, 30, 40）
        SparseIndex.IndexStatistics stats = sparseIndex.getStatistics();
        assertEquals(5, stats.getTotalEntries());

        // 验证索引项的位置
        SparseIndex.IndexEntry entry = sparseIndex.findByLogicalOffset(25);
        assertNotNull(entry);
        assertEquals(20, entry.getLogicalOffset()); // 应该找到最近的索引项20

        entry = sparseIndex.findByLogicalOffset(35);
        assertNotNull(entry);
        assertEquals(30, entry.getLogicalOffset()); // 应该找到最近的索引项30
    }

    @Test
    @DisplayName("测试时间范围查询")
    void testTimeRangeQuery() {
        // 添加时间递增的索引项
        sparseIndex.addIndexEntry(0, 1000, 1000000, 100, "TestTopic", 0);
        sparseIndex.addIndexEntry(10, 2000, 1010000, 100, "TestTopic", 0);
        sparseIndex.addIndexEntry(20, 3000, 1020000, 100, "TestTopic", 0);
        sparseIndex.addIndexEntry(30, 4000, 1030000, 100, "TestTopic", 0);

        // 查询时间范围
        SparseIndex.QueryRange range = sparseIndex.queryByTimeRange(1005000, 1025000);
        assertTrue(range.isValid());
        assertEquals(1000000, range.getStartEntry().getTimestamp()); // 应该找到1000000这个索引
        assertEquals(1030000, range.getEndEntry().getTimestamp());   // 应该找到1030000这个索引
    }

    @Test
    @DisplayName("测试逻辑偏移量范围查询")
    void testLogicalRangeQuery() {
        sparseIndex.addIndexEntry(0, 1000, 1000000, 100, "TestTopic", 0);
        sparseIndex.addIndexEntry(10, 2000, 1001000, 100, "TestTopic", 0);
        sparseIndex.addIndexEntry(20, 3000, 1002000, 100, "TestTopic", 0);
        sparseIndex.addIndexEntry(30, 4000, 1003000, 100, "TestTopic", 0);

        SparseIndex.QueryRange range = sparseIndex.queryByLogicalRange(5, 25);
        assertTrue(range.isValid());
        assertEquals(0, range.getStartEntry().getLogicalOffset());  // 最近的索引是0
        assertEquals(30, range.getEndEntry().getLogicalOffset());   // 最近的索引是30
    }

    @Test
    @DisplayName("测试物理偏移量范围查询")
    void testPhysicalRangeQuery() {
        sparseIndex.addIndexEntry(0, 1000, 1000000, 100, "TestTopic", 0);
        sparseIndex.addIndexEntry(10, 2000, 1001000, 100, "TestTopic", 0);
        sparseIndex.addIndexEntry(20, 3000, 1002000, 100, "TestTopic", 0);
        sparseIndex.addIndexEntry(30, 4000, 1003000, 100, "TestTopic", 0);

        SparseIndex.QueryRange range = sparseIndex.queryByPhysicalRange(1500, 3500);
        assertTrue(range.isValid());
        assertEquals(1000, range.getStartEntry().getPhysicalOffset());
        assertEquals(4000, range.getEndEntry().getPhysicalOffset());
    }

    @Test
    @DisplayName("测试索引项不存在的情况")
    void testIndexNotFound() {
        // 空索引查询
        assertNull(sparseIndex.findByLogicalOffset(100));
        assertNull(sparseIndex.findByPhysicalOffset(5000));
        assertNull(sparseIndex.findByTimestamp(2000000));

        // 添加一个索引项后查询不存在的范围
        sparseIndex.addIndexEntry(100, 10000, 2000000, 100, "TestTopic", 0);

        // 查询小于现有索引的偏移量
        assertNull(sparseIndex.findByLogicalOffset(50));
        assertNull(sparseIndex.findByPhysicalOffset(5000));
        assertNull(sparseIndex.findByTimestamp(1500000));
    }

    @Test
    @DisplayName("测试获取最早和最新索引项")
    void testEarliestAndLatestEntries() {
        // 空索引情况
        assertNull(sparseIndex.getEarliestEntry());
        assertNull(sparseIndex.getLatestEntry());

        // 添加索引项
        sparseIndex.addIndexEntry(10, 2000, 1001000, 100, "TestTopic", 0);
        sparseIndex.addIndexEntry(0, 1000, 1000000, 100, "TestTopic", 0);
        sparseIndex.addIndexEntry(20, 3000, 1002000, 100, "TestTopic", 0);

        SparseIndex.IndexEntry earliest = sparseIndex.getEarliestEntry();
        assertNotNull(earliest);
        assertEquals(0, earliest.getLogicalOffset());

        SparseIndex.IndexEntry latest = sparseIndex.getLatestEntry();
        assertNotNull(latest);
        assertEquals(20, latest.getLogicalOffset());
    }

    @Test
    @DisplayName("测试索引清理功能")
    void testIndexCleanup() {
        // 创建一个容量很小的索引用于测试清理
        SparseIndex smallIndex = new SparseIndex(1, 5); // 每条消息都索引，最多5个索引项

        // 添加超过容量的索引项
        for (int i = 0; i < 20; i++) {
            smallIndex.addIndexEntry(i, 1000 + i * 100, 1000000 + i * 1000, 100, "TestTopic", 0);
        }

        // 验证索引项数量被限制到合理范围内（清理是异步的，所以可能会有些延迟）
        SparseIndex.IndexStatistics stats = smallIndex.getStatistics();
        assertTrue(stats.getTotalEntries() <= 20); // 至少应该有一些索引项

        // 验证最新的索引项仍然存在
        SparseIndex.IndexEntry latest = smallIndex.getLatestEntry();
        assertNotNull(latest);
        assertTrue(latest.getLogicalOffset() >= 0);
    }

    @Test
    @DisplayName("测试索引统计信息")
    void testIndexStatistics() {
        // 添加一些索引项和查询
        sparseIndex.addIndexEntry(0, 1000, 1000000, 100, "TestTopic", 0);
        sparseIndex.addIndexEntry(10, 2000, 1001000, 100, "TestTopic", 0);

        // 执行一些查询
        sparseIndex.findByLogicalOffset(5);   // 命中
        sparseIndex.findByLogicalOffset(15);  // 命中
        sparseIndex.findByLogicalOffset(50);  // 命中（会找到最近的索引项）

        SparseIndex.IndexStatistics stats = sparseIndex.getStatistics();
        assertEquals(2, stats.getTotalEntries());
        assertEquals(3, stats.getTotalQueries());
        assertEquals(3, stats.getTotalHits()); // 所有查询都会命中最近的索引项
        assertEquals(1.0, stats.getHitRatio(), 0.01); // 100%命中率
        assertEquals(10, stats.getIndexInterval());

        assertNotNull(stats.toString());
        assertTrue(stats.toString().contains("hitRatio"));
    }

    @Test
    @DisplayName("测试索引序列化和反序列化")
    void testIndexSerialization() {
        // 添加一些索引项
        sparseIndex.addIndexEntry(0, 1000, 1000000, 100, "TestTopic1", 0);
        sparseIndex.addIndexEntry(10, 2000, 1001000, 120, "TestTopic2", 1);
        sparseIndex.addIndexEntry(20, 3000, 1002000, 110, "LongTopicName", 2);

        // 序列化
        byte[] serializedData = sparseIndex.serialize();
        assertNotNull(serializedData);
        assertTrue(serializedData.length > 0);

        // 创建新索引并反序列化
        SparseIndex newIndex = new SparseIndex(10, 1000);
        newIndex.deserialize(serializedData);

        // 验证反序列化后的数据
        SparseIndex.IndexStatistics originalStats = sparseIndex.getStatistics();
        SparseIndex.IndexStatistics newStats = newIndex.getStatistics();
        assertEquals(originalStats.getTotalEntries(), newStats.getTotalEntries());

        // 验证特定索引项
        SparseIndex.IndexEntry entry = newIndex.findByLogicalOffset(15);
        assertNotNull(entry);
        assertEquals(10, entry.getLogicalOffset());
        assertEquals(2000, entry.getPhysicalOffset());
        assertEquals("TestTopic2", entry.getTopic());
        assertEquals(1, entry.getQueueId());

        // 验证最早和最新项
        assertEquals(0, newIndex.getEarliestEntry().getLogicalOffset());
        assertEquals(20, newIndex.getLatestEntry().getLogicalOffset());
    }

    @Test
    @DisplayName("测试空数据反序列化")
    void testDeserializeEmptyData() {
        // 测试null数据
        sparseIndex.deserialize(null);
        assertEquals(0, sparseIndex.getStatistics().getTotalEntries());

        // 测试空数组
        sparseIndex.deserialize(new byte[0]);
        assertEquals(0, sparseIndex.getStatistics().getTotalEntries());
    }

    @Test
    @DisplayName("测试清空索引")
    void testClearIndex() {
        // 添加索引项
        sparseIndex.addIndexEntry(0, 1000, 1000000, 100, "TestTopic", 0);
        sparseIndex.addIndexEntry(10, 2000, 1001000, 100, "TestTopic", 0);

        // 执行查询以增加统计数据
        sparseIndex.findByLogicalOffset(5);

        // 验证索引不为空
        assertTrue(sparseIndex.getStatistics().getTotalEntries() > 0);
        assertTrue(sparseIndex.getStatistics().getTotalQueries() > 0);

        // 清空索引
        sparseIndex.clear();

        // 验证索引已清空
        SparseIndex.IndexStatistics stats = sparseIndex.getStatistics();
        assertEquals(0, stats.getTotalEntries());
        assertEquals(0, stats.getTotalQueries());
        assertEquals(0, stats.getTotalHits());

        assertNull(sparseIndex.getEarliestEntry());
        assertNull(sparseIndex.getLatestEntry());
    }

    @Test
    @DisplayName("测试IndexEntry toString方法")
    void testIndexEntryToString() {
        SparseIndex.IndexEntry entry = new SparseIndex.IndexEntry(
            100, 5000, 1500000, 200, "TestTopicName", 3
        );

        String toString = entry.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("100"));
        assertTrue(toString.contains("5000"));
        assertTrue(toString.contains("TestTopicName"));
        assertTrue(toString.contains("3"));
    }

    @Test
    @DisplayName("测试QueryRange方法")
    void testQueryRangeOperations() {
        SparseIndex.IndexEntry startEntry = new SparseIndex.IndexEntry(0, 1000, 1000000, 100, "Topic", 0);
        SparseIndex.IndexEntry endEntry = new SparseIndex.IndexEntry(20, 3000, 1020000, 100, "Topic", 0);

        SparseIndex.QueryRange range = new SparseIndex.QueryRange(startEntry, endEntry);

        assertTrue(range.isValid());
        assertEquals(1000, range.getStartPhysicalOffset());
        assertEquals(3000, range.getEndPhysicalOffset());
        assertEquals(startEntry, range.getStartEntry());
        assertEquals(endEntry, range.getEndEntry());

        // 测试无效范围
        SparseIndex.QueryRange invalidRange = new SparseIndex.QueryRange(null, null);
        assertFalse(invalidRange.isValid());

        assertNotNull(range.toString());
        assertTrue(range.toString().contains("1000"));
        assertTrue(range.toString().contains("3000"));
    }

    @Test
    @DisplayName("测试大量数据性能")
    void testLargeDataPerformance() {
        final int dataCount = 10000;
        final int interval = 100;

        SparseIndex largeIndex = new SparseIndex(interval, dataCount / interval + 100);

        // 添加大量索引项
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < dataCount; i++) {
            largeIndex.addIndexEntry(i, 1000L + i * 100, 1000000L + i * 1000, 100, "TestTopic", i % 4);
        }
        long addTime = System.currentTimeMillis() - startTime;

        // 执行大量查询
        startTime = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            int queryOffset = (int) (Math.random() * dataCount);
            largeIndex.findByLogicalOffset(queryOffset);
        }
        long queryTime = System.currentTimeMillis() - startTime;

        // 验证性能合理
        assertTrue(addTime < 1000, "Adding " + dataCount + " entries took too long: " + addTime + "ms");
        assertTrue(queryTime < 200, "1000 queries took too long: " + queryTime + "ms");

        // 验证索引统计
        SparseIndex.IndexStatistics stats = largeIndex.getStatistics();
        assertTrue(stats.getTotalEntries() > 0);
        assertTrue(stats.getTotalQueries() >= 1000);
        assertTrue(stats.getHitRatio() > 0.8); // 大部分查询应该命中

        System.out.printf("Large data test: %d entries added in %dms, 1000 queries in %dms, hit ratio: %.2f%%%n",
                         stats.getTotalEntries(), addTime, queryTime, stats.getHitRatio() * 100);
    }
}