package com.zephyr.storage.flush;

import com.zephyr.common.config.BrokerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.AfterEach;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 异步刷盘服务测试（简化版）
 */
class AsyncFlushServiceTest {

    private BrokerConfig brokerConfig;
    private AsyncFlushService asyncFlushService;

    @BeforeEach
    void setUp() {
        brokerConfig = new BrokerConfig();
        brokerConfig.setFlushStrategy("ASYNC_HYBRID");
        brokerConfig.setFlushBatchSize(4);
        brokerConfig.setFlushIntervalMs(100L);
        brokerConfig.setFlushThreadPoolSize(2);

        asyncFlushService = new AsyncFlushService(brokerConfig);
    }

    @AfterEach
    void tearDown() {
        if (asyncFlushService != null && asyncFlushService.isRunning()) {
            asyncFlushService.shutdown();
        }
    }

    @Test
    @DisplayName("测试异步刷盘服务初始化")
    void testAsyncFlushServiceInitialization() {
        assertNotNull(asyncFlushService);
        assertEquals(AsyncFlushService.FlushStrategy.ASYNC_HYBRID, asyncFlushService.getFlushStrategy());
        assertFalse(asyncFlushService.isRunning());
    }

    @Test
    @DisplayName("测试服务启动和关闭")
    void testServiceStartupAndShutdown() {
        // 测试启动
        asyncFlushService.start();
        assertTrue(asyncFlushService.isRunning());

        // 测试重复启动
        asyncFlushService.start(); // 应该不会有问题

        // 测试关闭
        asyncFlushService.shutdown();
        assertFalse(asyncFlushService.isRunning());

        // 测试重复关闭
        asyncFlushService.shutdown(); // 应该不会有问题
    }

    @Test
    @DisplayName("测试刷盘条件判断")
    void testShouldFlush() {
        // 测试不同策略的刷盘条件

        // 同步刷盘：总是刷盘
        brokerConfig.setFlushStrategy("SYNC");
        AsyncFlushService syncService = new AsyncFlushService(brokerConfig);
        assertTrue(syncService.shouldFlush(1000, System.currentTimeMillis()));

        // 批量刷盘：达到大小阈值时刷盘
        brokerConfig.setFlushStrategy("ASYNC_BATCH");
        brokerConfig.setFlushBatchSize(4); // 4页 = 16KB
        AsyncFlushService batchService = new AsyncFlushService(brokerConfig);

        assertFalse(batchService.shouldFlush(1000, System.currentTimeMillis())); // 小于阈值
        assertTrue(batchService.shouldFlush(20480, System.currentTimeMillis())); // 大于阈值

        // 定时刷盘：达到时间阈值时刷盘
        brokerConfig.setFlushStrategy("ASYNC_TIMER");
        brokerConfig.setFlushIntervalMs(100L);
        AsyncFlushService timerService = new AsyncFlushService(brokerConfig);

        long currentTime = System.currentTimeMillis();
        assertFalse(timerService.shouldFlush(1000, currentTime)); // 时间未到
        assertTrue(timerService.shouldFlush(1000, currentTime - 200)); // 时间已到

        // 混合策略：满足任一条件时刷盘
        brokerConfig.setFlushStrategy("ASYNC_HYBRID");
        AsyncFlushService hybridService = new AsyncFlushService(brokerConfig);

        assertTrue(hybridService.shouldFlush(20480, currentTime)); // 大小条件满足
        assertTrue(hybridService.shouldFlush(1000, currentTime - 200)); // 时间条件满足
        assertFalse(hybridService.shouldFlush(1000, currentTime)); // 都不满足
    }

    @Test
    @DisplayName("测试刷盘统计信息")
    void testFlushStatistics() {
        // 初始统计信息
        AsyncFlushService.FlushStatistics initialStats = asyncFlushService.getStatistics();
        assertEquals(0, initialStats.getTotalFlushCount());
        assertEquals(0, initialStats.getSuccessfulFlushCount());
        assertEquals(0, initialStats.getFailedFlushCount());
        assertEquals(0.0, initialStats.getSuccessRate(), 0.01);

        // 测试统计信息toString
        assertNotNull(initialStats.toString());
        assertTrue(initialStats.toString().contains("total=0"));

        // 重置统计信息
        asyncFlushService.resetStatistics();
        AsyncFlushService.FlushStatistics resetStats = asyncFlushService.getStatistics();
        assertEquals(0, resetStats.getTotalFlushCount());
    }

    @Test
    @DisplayName("测试无效的刷盘策略")
    void testInvalidFlushStrategy() {
        brokerConfig.setFlushStrategy("INVALID_STRATEGY");
        AsyncFlushService service = new AsyncFlushService(brokerConfig);

        // 应该回退到默认策略
        assertEquals(AsyncFlushService.FlushStrategy.ASYNC_HYBRID, service.getFlushStrategy());
    }

    @Test
    @DisplayName("测试刷盘结果对象")
    void testFlushResultObjects() {
        // 测试成功结果
        AsyncFlushService.FlushResult successResult =
            AsyncFlushService.FlushResult.success(1024, 10);
        assertTrue(successResult.isSuccess());
        assertEquals(1024, successResult.getFlushedBytes());
        assertEquals(10, successResult.getFlushDurationMs());
        assertNull(successResult.getErrorMessage());

        String successString = successResult.toString();
        assertNotNull(successString);
        assertTrue(successString.contains("success=true"));
        assertTrue(successString.contains("bytes=1024"));

        // 测试失败结果
        AsyncFlushService.FlushResult failureResult =
            AsyncFlushService.FlushResult.failure("Test error");
        assertFalse(failureResult.isSuccess());
        assertEquals(0, failureResult.getFlushedBytes());
        assertEquals(0, failureResult.getFlushDurationMs());
        assertEquals("Test error", failureResult.getErrorMessage());

        String failureString = failureResult.toString();
        assertNotNull(failureString);
        assertTrue(failureString.contains("success=false"));
        assertTrue(failureString.contains("Test error"));
    }

    @Test
    @DisplayName("测试刷盘策略枚举")
    void testFlushStrategyEnum() {
        // 测试所有策略枚举值
        assertNotNull(AsyncFlushService.FlushStrategy.SYNC);
        assertNotNull(AsyncFlushService.FlushStrategy.ASYNC_BATCH);
        assertNotNull(AsyncFlushService.FlushStrategy.ASYNC_TIMER);
        assertNotNull(AsyncFlushService.FlushStrategy.ASYNC_HYBRID);

        // 测试策略转换
        assertEquals("SYNC", AsyncFlushService.FlushStrategy.SYNC.toString());
        assertEquals("ASYNC_BATCH", AsyncFlushService.FlushStrategy.ASYNC_BATCH.toString());
        assertEquals("ASYNC_TIMER", AsyncFlushService.FlushStrategy.ASYNC_TIMER.toString());
        assertEquals("ASYNC_HYBRID", AsyncFlushService.FlushStrategy.ASYNC_HYBRID.toString());
    }

    @Test
    @DisplayName("测试刷盘统计详细信息")
    void testFlushStatisticsDetails() {
        // 创建统计信息对象
        AsyncFlushService.FlushStatistics stats = new AsyncFlushService.FlushStatistics(
            10, 8, 2, 8192, 100
        );

        assertEquals(10, stats.getTotalFlushCount());
        assertEquals(8, stats.getSuccessfulFlushCount());
        assertEquals(2, stats.getFailedFlushCount());
        assertEquals(8192, stats.getTotalFlushedBytes());
        assertEquals(100, stats.getTotalFlushTimeMs());
        assertEquals(10.0, stats.getAverageFlushTimeMs(), 0.01);
        assertEquals(0.8, stats.getSuccessRate(), 0.01);
        assertTrue(stats.getThroughputMBps() > 0);

        String statsString = stats.toString();
        assertNotNull(statsString);
        assertTrue(statsString.contains("total=10"));
        assertTrue(statsString.contains("success=8"));
        assertTrue(statsString.contains("failed=2"));
    }

    @Test
    @DisplayName("测试空统计信息")
    void testEmptyFlushStatistics() {
        AsyncFlushService.FlushStatistics emptyStats = new AsyncFlushService.FlushStatistics(
            0, 0, 0, 0, 0
        );

        assertEquals(0, emptyStats.getTotalFlushCount());
        assertEquals(0.0, emptyStats.getAverageFlushTimeMs(), 0.01);
        assertEquals(0.0, emptyStats.getSuccessRate(), 0.01);
        assertEquals(0.0, emptyStats.getThroughputMBps(), 0.01);
    }

    @Test
    @DisplayName("测试不同BrokerConfig设置")
    void testDifferentBrokerConfigSettings() {
        // 测试不同的配置组合
        BrokerConfig config1 = new BrokerConfig();
        config1.setFlushStrategy("SYNC");
        config1.setFlushBatchSize(8);
        config1.setFlushIntervalMs(200L);
        config1.setFlushThreadPoolSize(4);

        AsyncFlushService service1 = new AsyncFlushService(config1);
        assertEquals(AsyncFlushService.FlushStrategy.SYNC, service1.getFlushStrategy());

        BrokerConfig config2 = new BrokerConfig();
        config2.setFlushStrategy("ASYNC_TIMER");
        config2.setFlushIntervalMs(50L);

        AsyncFlushService service2 = new AsyncFlushService(config2);
        assertEquals(AsyncFlushService.FlushStrategy.ASYNC_TIMER, service2.getFlushStrategy());
    }
}