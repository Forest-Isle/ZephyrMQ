package com.zephyr.storage.compression;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * MessageCompressor测试
 */
public class MessageCompressorTest {

    @Test
    public void testLZ4Compression() {
        // 创建一个可压缩的测试数据（重复的字符串）
        String testStr = "Hello ZephyrMQ! ".repeat(100); // 重复100次，约1.6KB
        byte[] originalData = testStr.getBytes();

        // 压缩
        byte[] compressed = MessageCompressor.compress(originalData, MessageCompressor.CompressionType.LZ4);

        // 验证压缩效果
        assertTrue(compressed.length < originalData.length, "Compressed data should be smaller");
        assertTrue(MessageCompressor.isCompressed(compressed), "Data should be marked as compressed");
        assertEquals(MessageCompressor.CompressionType.LZ4, MessageCompressor.getCompressionType(compressed));

        // 解压缩
        byte[] decompressed = MessageCompressor.decompress(compressed);

        // 验证解压缩结果
        assertArrayEquals(originalData, decompressed, "Decompressed data should match original");
    }

    @Test
    public void testSnappyCompression() {
        // 创建一个可压缩的测试数据
        String testStr = "ZephyrMQ is a high-performance message queue! ".repeat(50);
        byte[] originalData = testStr.getBytes();

        // 压缩
        byte[] compressed = MessageCompressor.compress(originalData, MessageCompressor.CompressionType.SNAPPY);

        // 验证压缩效果
        assertTrue(compressed.length < originalData.length, "Compressed data should be smaller");
        assertTrue(MessageCompressor.isCompressed(compressed), "Data should be marked as compressed");
        assertEquals(MessageCompressor.CompressionType.SNAPPY, MessageCompressor.getCompressionType(compressed));

        // 解压缩
        byte[] decompressed = MessageCompressor.decompress(compressed);

        // 验证解压缩结果
        assertArrayEquals(originalData, decompressed, "Decompressed data should match original");
    }

    @Test
    public void testNoCompressionForSmallData() {
        // 创建小于阈值的数据
        byte[] smallData = "Small message".getBytes();

        // 尝试压缩
        byte[] result = MessageCompressor.compress(smallData, MessageCompressor.CompressionType.LZ4);

        // 验证没有压缩
        assertEquals(MessageCompressor.CompressionType.NONE, MessageCompressor.getCompressionType(result));

        // 解压缩应该返回原始数据
        byte[] decompressed = MessageCompressor.decompress(result);
        assertArrayEquals(smallData, decompressed);
    }

    @Test
    public void testCompressionRatio() {
        int originalSize = 1000;
        int compressedSize = 300;

        double ratio = MessageCompressor.getCompressionRatio(originalSize, compressedSize);
        assertEquals(0.7, ratio, 0.01, "Compression ratio should be 70%");
    }

    @Test
    public void testInvalidCompressionType() {
        byte[] data = "test data".getBytes();

        // 模拟无效的压缩类型
        byte[] invalidData = new byte[data.length + 5];
        invalidData[0] = (byte) 99; // 无效的压缩类型代码
        System.arraycopy(data, 0, invalidData, 5, data.length);

        // 解压缩应该返回原始数据部分
        byte[] result = MessageCompressor.decompress(invalidData);
        assertArrayEquals(data, result);
    }

    @Test
    public void testNullAndEmptyData() {
        // 测试null数据
        assertNull(MessageCompressor.compress(null, MessageCompressor.CompressionType.LZ4));
        assertNull(MessageCompressor.decompress(null));

        // 测试空数据
        byte[] emptyData = new byte[0];
        byte[] result = MessageCompressor.compress(emptyData, MessageCompressor.CompressionType.LZ4);
        assertArrayEquals(emptyData, result);
    }
}