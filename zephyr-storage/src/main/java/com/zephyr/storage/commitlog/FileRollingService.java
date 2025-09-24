package com.zephyr.storage.commitlog;

import com.zephyr.common.config.BrokerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 文件滚动服务
 * 负责根据时间、大小等策略管理文件滚动和清理
 */
public class FileRollingService {

    private static final Logger logger = LoggerFactory.getLogger(FileRollingService.class);

    public enum RollingStrategy {
        SIZE,           // 基于文件大小滚动
        TIME,           // 基于时间滚动
        SIZE_TIME       // 基于大小和时间混合策略
    }

    public enum RollingTrigger {
        FILE_SIZE_EXCEEDED,
        FILE_AGE_EXCEEDED,
        MANUAL_TRIGGER
    }

    private final BrokerConfig brokerConfig;
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile boolean started = false;

    public FileRollingService(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "FileRollingService");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 启动文件滚动服务
     */
    public void start() {
        if (!started && brokerConfig.isFileRollingEnable()) {
            started = true;

            // 定期检查并清理过期文件
            if (brokerConfig.isAutoCleanExpiredFiles()) {
                scheduledExecutorService.scheduleAtFixedRate(
                    this::cleanExpiredFilesTask,
                    brokerConfig.getCleanExpiredFilesInterval(),
                    brokerConfig.getCleanExpiredFilesInterval(),
                    TimeUnit.MILLISECONDS
                );
                logger.info("File rolling service started with auto cleanup enabled");
            }
        }
    }

    /**
     * 停止文件滚动服务
     */
    public void shutdown() {
        if (started) {
            started = false;
            scheduledExecutorService.shutdown();
            try {
                if (!scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduledExecutorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduledExecutorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("File rolling service shut down");
        }
    }

    /**
     * 检查文件是否需要滚动
     */
    public boolean shouldRollFile(MappedFile mappedFile) {
        if (!brokerConfig.isFileRollingEnable() || mappedFile == null) {
            return false;
        }

        RollingStrategy strategy = parseRollingStrategy(brokerConfig.getFileRollingStrategy());

        switch (strategy) {
            case SIZE:
                return shouldRollBySize(mappedFile);
            case TIME:
                return shouldRollByTime(mappedFile);
            case SIZE_TIME:
                return shouldRollBySize(mappedFile) || shouldRollByTime(mappedFile);
            default:
                return false;
        }
    }

    /**
     * 获取滚动触发原因
     */
    public RollingTrigger getRollingTrigger(MappedFile mappedFile) {
        if (shouldRollBySize(mappedFile)) {
            return RollingTrigger.FILE_SIZE_EXCEEDED;
        }
        if (shouldRollByTime(mappedFile)) {
            return RollingTrigger.FILE_AGE_EXCEEDED;
        }
        return RollingTrigger.MANUAL_TRIGGER;
    }

    /**
     * 清理过期文件
     */
    public int cleanExpiredFiles(MappedFileQueue mappedFileQueue) {
        if (!brokerConfig.isAutoCleanExpiredFiles()) {
            return 0;
        }

        long retentionMillis = brokerConfig.getFileRetentionHours() * 60 * 60 * 1000;
        int maxFileCount = brokerConfig.getMaxFileCount();

        List<MappedFile> mappedFiles = mappedFileQueue.getMappedFiles();
        if (mappedFiles.isEmpty()) {
            return 0;
        }

        List<MappedFile> expiredFiles = new ArrayList<>();
        long currentTime = System.currentTimeMillis();

        // 按时间清理过期文件（保留最新的文件）
        for (int i = 0; i < mappedFiles.size() - 1; i++) { // 保留最后一个文件
            MappedFile mappedFile = mappedFiles.get(i);
            long fileAge = currentTime - mappedFile.getStoreTimestamp();

            if (fileAge > retentionMillis) {
                expiredFiles.add(mappedFile);
                logger.info("File {} expired by time, age: {} hours",
                    mappedFile.getFileName(), fileAge / (60 * 60 * 1000));
            }
        }

        // 按数量清理超出限制的文件
        if (mappedFiles.size() > maxFileCount) {
            int excessCount = mappedFiles.size() - maxFileCount;
            for (int i = 0; i < excessCount && i < mappedFiles.size() - 1; i++) {
                MappedFile mappedFile = mappedFiles.get(i);
                if (!expiredFiles.contains(mappedFile)) {
                    expiredFiles.add(mappedFile);
                    logger.info("File {} expired by count limit", mappedFile.getFileName());
                }
            }
        }

        // 执行清理
        int cleanedCount = 0;
        for (MappedFile expiredFile : expiredFiles) {
            if (cleanExpiredFile(mappedFileQueue, expiredFile)) {
                cleanedCount++;
            }
        }

        if (cleanedCount > 0) {
            logger.info("Cleaned {} expired files", cleanedCount);
        }

        return cleanedCount;
    }

    /**
     * 根据文件大小判断是否需要滚动
     */
    private boolean shouldRollBySize(MappedFile mappedFile) {
        File file = new File(mappedFile.getFileName());
        return file.exists() && file.length() >= brokerConfig.getFileMaxSizeBytes();
    }

    /**
     * 根据文件时间判断是否需要滚动
     */
    private boolean shouldRollByTime(MappedFile mappedFile) {
        long currentTime = System.currentTimeMillis();
        long fileAge = currentTime - mappedFile.getStoreTimestamp();
        return fileAge >= brokerConfig.getFileMaxAgeMillis();
    }

    /**
     * 解析滚动策略
     */
    private RollingStrategy parseRollingStrategy(String strategy) {
        try {
            return RollingStrategy.valueOf(strategy.toUpperCase());
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid rolling strategy: {}, using default SIZE_TIME", strategy);
            return RollingStrategy.SIZE_TIME;
        }
    }

    /**
     * 定期清理任务
     */
    private void cleanExpiredFilesTask() {
        try {
            // 这里需要获取所有MappedFileQueue实例进行清理
            // 由于这是一个通用服务，具体的队列实例需要由调用方注册
            logger.debug("Running scheduled expired files cleanup task");
        } catch (Exception e) {
            logger.error("Error in scheduled cleanup task", e);
        }
    }

    /**
     * 清理单个过期文件
     */
    private boolean cleanExpiredFile(MappedFileQueue mappedFileQueue, MappedFile expiredFile) {
        try {
            // 确保文件不在使用中
            if (expiredFile.isAvailable()) {
                logger.warn("Cannot clean file {} - still in use", expiredFile.getFileName());
                return false;
            }

            // 关闭文件句柄
            expiredFile.shutdown(1000);

            // 从队列中移除
            boolean removed = mappedFileQueue.getMappedFiles().remove(expiredFile);

            // 删除物理文件
            boolean destroyed = expiredFile.destroy(3000);

            if (removed && destroyed) {
                logger.info("Successfully cleaned expired file: {}", expiredFile.getFileName());
                return true;
            } else {
                logger.warn("Failed to clean file: {}, removed: {}, destroyed: {}",
                    expiredFile.getFileName(), removed, destroyed);
                return false;
            }
        } catch (Exception e) {
            logger.error("Error cleaning expired file: " + expiredFile.getFileName(), e);
            return false;
        }
    }

    /**
     * 获取文件信息统计
     */
    public FileRollingStats getFileRollingStats(MappedFileQueue mappedFileQueue) {
        List<MappedFile> mappedFiles = mappedFileQueue.getMappedFiles();
        long totalSize = 0;
        long oldestTimestamp = Long.MAX_VALUE;
        long newestTimestamp = 0;

        for (MappedFile mappedFile : mappedFiles) {
            File file = new File(mappedFile.getFileName());
            if (file.exists()) {
                totalSize += file.length();
            }

            long timestamp = mappedFile.getStoreTimestamp();
            if (timestamp > 0) {
                oldestTimestamp = Math.min(oldestTimestamp, timestamp);
                newestTimestamp = Math.max(newestTimestamp, timestamp);
            }
        }

        return new FileRollingStats(
            mappedFiles.size(),
            totalSize,
            oldestTimestamp == Long.MAX_VALUE ? 0 : oldestTimestamp,
            newestTimestamp
        );
    }

    /**
     * 文件滚动统计信息
     */
    public static class FileRollingStats {
        private final int fileCount;
        private final long totalSize;
        private final long oldestTimestamp;
        private final long newestTimestamp;

        public FileRollingStats(int fileCount, long totalSize, long oldestTimestamp, long newestTimestamp) {
            this.fileCount = fileCount;
            this.totalSize = totalSize;
            this.oldestTimestamp = oldestTimestamp;
            this.newestTimestamp = newestTimestamp;
        }

        public int getFileCount() { return fileCount; }
        public long getTotalSize() { return totalSize; }
        public long getOldestTimestamp() { return oldestTimestamp; }
        public long getNewestTimestamp() { return newestTimestamp; }

        public long getAgeSpanMillis() {
            return newestTimestamp > oldestTimestamp ? newestTimestamp - oldestTimestamp : 0;
        }

        @Override
        public String toString() {
            return String.format("FileRollingStats{files=%d, totalSize=%d bytes, ageSpan=%d ms}",
                fileCount, totalSize, getAgeSpanMillis());
        }
    }
}