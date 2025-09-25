package com.zephyr.storage.flush;

import com.zephyr.common.config.BrokerConfig;
import com.zephyr.storage.commitlog.MappedFileQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 异步刷盘服务
 * 提供高性能的异步刷盘机制，支持批量刷盘和定时刷盘
 */
public class AsyncFlushService {

    private static final Logger logger = LoggerFactory.getLogger(AsyncFlushService.class);

    /**
     * 刷盘策略
     */
    public enum FlushStrategy {
        SYNC,           // 同步刷盘：每次写入都立即刷盘
        ASYNC_BATCH,    // 异步批量刷盘：达到一定数量后刷盘
        ASYNC_TIMER,    // 异步定时刷盘：按时间间隔刷盘
        ASYNC_HYBRID    // 混合策略：批量+定时
    }

    /**
     * 刷盘结果
     */
    public static class FlushResult {
        private final boolean success;
        private final long flushedBytes;
        private final long flushDurationMs;
        private final String errorMessage;

        public FlushResult(boolean success, long flushedBytes, long flushDurationMs, String errorMessage) {
            this.success = success;
            this.flushedBytes = flushedBytes;
            this.flushDurationMs = flushDurationMs;
            this.errorMessage = errorMessage;
        }

        public static FlushResult success(long flushedBytes, long flushDurationMs) {
            return new FlushResult(true, flushedBytes, flushDurationMs, null);
        }

        public static FlushResult failure(String errorMessage) {
            return new FlushResult(false, 0, 0, errorMessage);
        }

        public boolean isSuccess() { return success; }
        public long getFlushedBytes() { return flushedBytes; }
        public long getFlushDurationMs() { return flushDurationMs; }
        public String getErrorMessage() { return errorMessage; }

        @Override
        public String toString() {
            return String.format("FlushResult{success=%s, bytes=%d, duration=%dms, error='%s'}",
                               success, flushedBytes, flushDurationMs, errorMessage);
        }
    }

    /**
     * 刷盘统计信息
     */
    public static class FlushStatistics {
        private final long totalFlushCount;
        private final long successfulFlushCount;
        private final long failedFlushCount;
        private final long totalFlushedBytes;
        private final long totalFlushTimeMs;
        private final double averageFlushTimeMs;
        private final double throughputMBps;

        public FlushStatistics(long totalFlushCount, long successfulFlushCount, long failedFlushCount,
                             long totalFlushedBytes, long totalFlushTimeMs) {
            this.totalFlushCount = totalFlushCount;
            this.successfulFlushCount = successfulFlushCount;
            this.failedFlushCount = failedFlushCount;
            this.totalFlushedBytes = totalFlushedBytes;
            this.totalFlushTimeMs = totalFlushTimeMs;
            this.averageFlushTimeMs = totalFlushCount > 0 ? (double) totalFlushTimeMs / totalFlushCount : 0.0;
            this.throughputMBps = totalFlushTimeMs > 0 ? (double) totalFlushedBytes / (1024 * 1024) / (totalFlushTimeMs / 1000.0) : 0.0;
        }

        public long getTotalFlushCount() { return totalFlushCount; }
        public long getSuccessfulFlushCount() { return successfulFlushCount; }
        public long getFailedFlushCount() { return failedFlushCount; }
        public long getTotalFlushedBytes() { return totalFlushedBytes; }
        public long getTotalFlushTimeMs() { return totalFlushTimeMs; }
        public double getAverageFlushTimeMs() { return averageFlushTimeMs; }
        public double getThroughputMBps() { return throughputMBps; }
        public double getSuccessRate() { return totalFlushCount > 0 ? (double) successfulFlushCount / totalFlushCount : 0.0; }

        @Override
        public String toString() {
            return String.format("FlushStatistics{total=%d, success=%d, failed=%d, bytes=%d, avgTime=%.2fms, throughput=%.2fMB/s, successRate=%.2f%%}",
                               totalFlushCount, successfulFlushCount, failedFlushCount, totalFlushedBytes,
                               averageFlushTimeMs, throughputMBps, getSuccessRate() * 100);
        }
    }

    private final BrokerConfig brokerConfig;
    private final FlushStrategy flushStrategy;

    // 刷盘配置参数
    private final int flushBatchSize;           // 批量刷盘大小（页数）
    private final long flushIntervalMs;         // 刷盘间隔（毫秒）
    private final int flushThreadPoolSize;     // 刷盘线程池大小

    // 线程池和调度器
    private ScheduledExecutorService flushScheduler;
    private volatile boolean running = false;

    // 统计信息
    private final AtomicLong totalFlushCount = new AtomicLong(0);
    private final AtomicLong successfulFlushCount = new AtomicLong(0);
    private final AtomicLong failedFlushCount = new AtomicLong(0);
    private final AtomicLong totalFlushedBytes = new AtomicLong(0);
    private final AtomicLong totalFlushTimeMs = new AtomicLong(0);
    private final AtomicBoolean flushInProgress = new AtomicBoolean(false);

    public AsyncFlushService(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
        this.flushStrategy = parseFlushStrategy(brokerConfig.getFlushStrategy());
        this.flushBatchSize = brokerConfig.getFlushBatchSize();
        this.flushIntervalMs = brokerConfig.getFlushIntervalMs();
        this.flushThreadPoolSize = brokerConfig.getFlushThreadPoolSize();

        logger.info("AsyncFlushService initialized: strategy={}, batchSize={}, intervalMs={}, threadPoolSize={}",
                   flushStrategy, flushBatchSize, flushIntervalMs, flushThreadPoolSize);
    }

    /**
     * 启动异步刷盘服务
     */
    public void start() {
        if (running) {
            logger.warn("AsyncFlushService is already running");
            return;
        }

        running = true;

        // 创建刷盘调度器
        flushScheduler = Executors.newScheduledThreadPool(flushThreadPoolSize,
            r -> new Thread(r, "AsyncFlushService-" + System.currentTimeMillis()));

        // 根据策略启动不同的刷盘机制
        switch (flushStrategy) {
            case SYNC:
                // 同步刷盘不需要后台服务
                break;
            case ASYNC_TIMER:
            case ASYNC_HYBRID:
                // 启动定时刷盘任务
                flushScheduler.scheduleAtFixedRate(
                    this::performTimerFlush,
                    flushIntervalMs,
                    flushIntervalMs,
                    TimeUnit.MILLISECONDS
                );
                break;
            case ASYNC_BATCH:
                // 批量刷盘由外部调用触发，不需要定时任务
                break;
        }

        logger.info("AsyncFlushService started with strategy: {}", flushStrategy);
    }

    /**
     * 停止异步刷盘服务
     */
    public void shutdown() {
        if (!running) {
            return;
        }

        running = false;

        if (flushScheduler != null) {
            flushScheduler.shutdown();
            try {
                if (!flushScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    flushScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                flushScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        logger.info("AsyncFlushService shutdown complete");
    }

    /**
     * 异步刷盘
     */
    public CompletableFuture<FlushResult> flushAsync(MappedFileQueue mappedFileQueue) {
        if (!running) {
            return CompletableFuture.completedFuture(FlushResult.failure("AsyncFlushService is not running"));
        }

        switch (flushStrategy) {
            case SYNC:
                return performSyncFlush(mappedFileQueue);
            case ASYNC_BATCH:
            case ASYNC_TIMER:
            case ASYNC_HYBRID:
                return performAsyncFlush(mappedFileQueue);
            default:
                return CompletableFuture.completedFuture(FlushResult.failure("Unknown flush strategy: " + flushStrategy));
        }
    }

    /**
     * 同步刷盘
     */
    private CompletableFuture<FlushResult> performSyncFlush(MappedFileQueue mappedFileQueue) {
        long startTime = System.currentTimeMillis();
        totalFlushCount.incrementAndGet();

        try {
            boolean success = mappedFileQueue.flush(0); // 刷盘所有数据
            long duration = System.currentTimeMillis() - startTime;
            totalFlushTimeMs.addAndGet(duration);

            if (success) {
                successfulFlushCount.incrementAndGet();
                // 估算刷盘字节数（简化实现）
                long flushedBytes = 4096; // 假设每页4KB
                totalFlushedBytes.addAndGet(flushedBytes);
                return CompletableFuture.completedFuture(FlushResult.success(flushedBytes, duration));
            } else {
                failedFlushCount.incrementAndGet();
                return CompletableFuture.completedFuture(FlushResult.failure("Sync flush failed"));
            }
        } catch (Exception e) {
            failedFlushCount.incrementAndGet();
            long duration = System.currentTimeMillis() - startTime;
            totalFlushTimeMs.addAndGet(duration);
            logger.error("Sync flush error", e);
            return CompletableFuture.completedFuture(FlushResult.failure("Sync flush error: " + e.getMessage()));
        }
    }

    /**
     * 异步刷盘
     */
    private CompletableFuture<FlushResult> performAsyncFlush(MappedFileQueue mappedFileQueue) {
        return CompletableFuture.supplyAsync(() -> {
            long startTime = System.currentTimeMillis();
            totalFlushCount.incrementAndGet();

            try {
                boolean success = mappedFileQueue.flush(flushBatchSize);
                long duration = System.currentTimeMillis() - startTime;
                totalFlushTimeMs.addAndGet(duration);

                if (success) {
                    successfulFlushCount.incrementAndGet();
                    long flushedBytes = flushBatchSize * 4096L; // 估算刷盘字节数
                    totalFlushedBytes.addAndGet(flushedBytes);
                    return FlushResult.success(flushedBytes, duration);
                } else {
                    failedFlushCount.incrementAndGet();
                    return FlushResult.failure("Async flush failed");
                }
            } catch (Exception e) {
                failedFlushCount.incrementAndGet();
                long duration = System.currentTimeMillis() - startTime;
                totalFlushTimeMs.addAndGet(duration);
                logger.error("Async flush error", e);
                return FlushResult.failure("Async flush error: " + e.getMessage());
            }
        }, flushScheduler);
    }

    /**
     * 定时刷盘任务
     */
    private void performTimerFlush() {
        // 这里需要外部提供MappedFileQueue实例
        // 实际实现中可以通过回调或者注册机制来获取
        logger.debug("Timer flush triggered (implementation needed)");
    }

    /**
     * 批量刷盘检查
     */
    public boolean shouldFlush(long writtenBytes, long lastFlushTime) {
        switch (flushStrategy) {
            case SYNC:
                return true; // 同步刷盘总是立即刷盘

            case ASYNC_BATCH:
                // 达到批量大小时刷盘
                return writtenBytes >= flushBatchSize * 4096L;

            case ASYNC_TIMER:
                // 达到时间间隔时刷盘
                return System.currentTimeMillis() - lastFlushTime >= flushIntervalMs;

            case ASYNC_HYBRID:
                // 满足任一条件时刷盘
                return writtenBytes >= flushBatchSize * 4096L ||
                       System.currentTimeMillis() - lastFlushTime >= flushIntervalMs;

            default:
                return false;
        }
    }

    /**
     * 强制刷盘所有数据
     */
    public CompletableFuture<FlushResult> forceFlush(MappedFileQueue mappedFileQueue) {
        if (flushInProgress.compareAndSet(false, true)) {
            try {
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        long startTime = System.currentTimeMillis();
                        boolean success = mappedFileQueue.flush(0); // 刷盘所有数据
                        long duration = System.currentTimeMillis() - startTime;

                        totalFlushCount.incrementAndGet();
                        totalFlushTimeMs.addAndGet(duration);

                        if (success) {
                            successfulFlushCount.incrementAndGet();
                            long flushedBytes = 4096 * 10; // 估算值
                            totalFlushedBytes.addAndGet(flushedBytes);
                            return FlushResult.success(flushedBytes, duration);
                        } else {
                            failedFlushCount.incrementAndGet();
                            return FlushResult.failure("Force flush failed");
                        }
                    } finally {
                        flushInProgress.set(false);
                    }
                }, flushScheduler);
            } catch (Exception e) {
                flushInProgress.set(false);
                return CompletableFuture.completedFuture(FlushResult.failure("Force flush error: " + e.getMessage()));
            }
        } else {
            return CompletableFuture.completedFuture(FlushResult.failure("Another flush operation is in progress"));
        }
    }

    /**
     * 获取刷盘统计信息
     */
    public FlushStatistics getStatistics() {
        return new FlushStatistics(
            totalFlushCount.get(),
            successfulFlushCount.get(),
            failedFlushCount.get(),
            totalFlushedBytes.get(),
            totalFlushTimeMs.get()
        );
    }

    /**
     * 重置统计信息
     */
    public void resetStatistics() {
        totalFlushCount.set(0);
        successfulFlushCount.set(0);
        failedFlushCount.set(0);
        totalFlushedBytes.set(0);
        totalFlushTimeMs.set(0);
        logger.info("Flush statistics reset");
    }

    /**
     * 检查服务状态
     */
    public boolean isRunning() {
        return running;
    }

    public FlushStrategy getFlushStrategy() {
        return flushStrategy;
    }

    /**
     * 解析刷盘策略
     */
    private FlushStrategy parseFlushStrategy(String strategyStr) {
        if (strategyStr == null) {
            return FlushStrategy.ASYNC_HYBRID;
        }

        try {
            return FlushStrategy.valueOf(strategyStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid flush strategy: {}, fallback to ASYNC_HYBRID", strategyStr);
            return FlushStrategy.ASYNC_HYBRID;
        }
    }
}