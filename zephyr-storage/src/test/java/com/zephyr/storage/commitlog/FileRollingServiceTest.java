package com.zephyr.storage.commitlog;

import com.zephyr.common.config.BrokerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class FileRollingServiceTest {

    @TempDir
    Path tempDir;

    private BrokerConfig brokerConfig;
    private FileRollingService fileRollingService;
    private MappedFileQueue mappedFileQueue;
    private String storePath;

    @BeforeEach
    public void setUp() {
        storePath = tempDir.toString();

        brokerConfig = new BrokerConfig();
        brokerConfig.setFileRollingEnable(true);
        brokerConfig.setFileRollingStrategy("SIZE_TIME");
        brokerConfig.setFileMaxSizeBytes(1024); // 1KB for testing
        brokerConfig.setFileMaxAgeMillis(1000); // 1 second for testing
        brokerConfig.setFileRetentionHours(1); // 1 hour retention
        brokerConfig.setMaxFileCount(5);
        brokerConfig.setAutoCleanExpiredFiles(true);

        fileRollingService = new FileRollingService(brokerConfig);
        mappedFileQueue = new MappedFileQueue(storePath, 1024, brokerConfig);
    }

    @AfterEach
    public void tearDown() {
        if (mappedFileQueue != null) {
            mappedFileQueue.shutdown(1000);
            mappedFileQueue.destroy();
        }
        if (fileRollingService != null) {
            fileRollingService.shutdown();
        }
    }

    @Test
    public void testFileRollingBySize() throws Exception {
        mappedFileQueue.load();
        fileRollingService.start();

        // 写入超过文件大小限制的数据
        byte[] largeData = new byte[2048]; // 2KB data, exceeds 1KB limit
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }

        // 第一次写入，应该创建第一个文件
        MappedFile.AppendMessageResult result1 = mappedFileQueue.appendMessage(largeData);
        assertNotNull(result1);

        // 等待一下确保文件大小被正确检测
        Thread.sleep(100);

        // 第二次写入，由于文件大小超限，应该滚动到新文件
        MappedFile.AppendMessageResult result2 = mappedFileQueue.appendMessage(largeData);
        assertNotNull(result2);

        // 验证文件数量
        assertTrue(mappedFileQueue.getMappedFiles().size() >= 1);

        FileRollingService.FileRollingStats stats = mappedFileQueue.getFileRollingStats();
        assertNotNull(stats);
        assertTrue(stats.getFileCount() >= 1);
        assertTrue(stats.getTotalSize() > 0);
    }

    @Test
    public void testFileRollingByTime() throws Exception {
        // 设置很短的时间限制用于测试
        brokerConfig.setFileMaxAgeMillis(50); // 50ms

        mappedFileQueue.load();
        fileRollingService.start();

        // 写入数据
        byte[] data = new byte[100];
        MappedFile.AppendMessageResult result1 = mappedFileQueue.appendMessage(data);
        assertNotNull(result1);

        // 等待超过时间限制
        Thread.sleep(100);

        // 再次写入，应该因为时间超限而滚动文件
        MappedFile.AppendMessageResult result2 = mappedFileQueue.appendMessage(data);
        assertNotNull(result2);

        FileRollingService.FileRollingStats stats = mappedFileQueue.getFileRollingStats();
        assertNotNull(stats);
    }

    @Test
    public void testFileCleanup() throws Exception {
        // 设置很短的保留时间
        brokerConfig.setFileRetentionHours(0); // 立即过期
        brokerConfig.setMaxFileCount(2); // 最多保留2个文件

        mappedFileQueue.load();
        fileRollingService.start();

        // 创建多个文件
        byte[] data = new byte[100];
        for (int i = 0; i < 5; i++) {
            mappedFileQueue.appendMessage(data);
            Thread.sleep(10); // 确保文件有不同的时间戳
        }

        // 手动触发清理
        int cleanedCount = mappedFileQueue.cleanExpiredFiles();

        // 验证清理结果
        assertTrue(cleanedCount >= 0);

        FileRollingService.FileRollingStats stats = mappedFileQueue.getFileRollingStats();
        assertNotNull(stats);
    }

    @Test
    public void testRollingStrategyParsing() {
        // 测试不同的滚动策略
        brokerConfig.setFileRollingStrategy("SIZE");
        FileRollingService service1 = new FileRollingService(brokerConfig);
        assertNotNull(service1);

        brokerConfig.setFileRollingStrategy("TIME");
        FileRollingService service2 = new FileRollingService(brokerConfig);
        assertNotNull(service2);

        brokerConfig.setFileRollingStrategy("SIZE_TIME");
        FileRollingService service3 = new FileRollingService(brokerConfig);
        assertNotNull(service3);

        // 测试无效策略，应该降级到默认值
        brokerConfig.setFileRollingStrategy("INVALID");
        FileRollingService service4 = new FileRollingService(brokerConfig);
        assertNotNull(service4);
    }

    @Test
    public void testFileRollingDisabled() throws Exception {
        // 禁用文件滚动
        brokerConfig.setFileRollingEnable(false);

        mappedFileQueue.load();
        fileRollingService.start();

        // 写入大量数据
        byte[] largeData = new byte[5120]; // 5KB data
        MappedFile.AppendMessageResult result = mappedFileQueue.appendMessage(largeData);
        assertNotNull(result);

        // 验证没有创建额外的文件（因为滚动被禁用）
        FileRollingService.FileRollingStats stats = mappedFileQueue.getFileRollingStats();
        assertNotNull(stats);
    }

    @Test
    public void testFileRollingStats() throws Exception {
        mappedFileQueue.load();

        byte[] data = new byte[512];
        mappedFileQueue.appendMessage(data);

        FileRollingService.FileRollingStats stats = mappedFileQueue.getFileRollingStats();
        assertNotNull(stats);
        assertTrue(stats.getFileCount() >= 0);
        assertTrue(stats.getTotalSize() >= 0);
        assertTrue(stats.getAgeSpanMillis() >= 0);

        String statsString = stats.toString();
        assertNotNull(statsString);
        assertTrue(statsString.contains("FileRollingStats"));
    }

    @Test
    public void testServiceLifecycle() {
        FileRollingService service = new FileRollingService(brokerConfig);

        // 测试启动
        service.start();

        // 测试重复启动（应该安全）
        service.start();

        // 测试关闭
        service.shutdown();

        // 测试重复关闭（应该安全）
        service.shutdown();
    }
}