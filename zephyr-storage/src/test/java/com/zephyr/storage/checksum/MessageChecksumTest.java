package com.zephyr.storage.checksum;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 消息校验和测试
 */
class MessageChecksumTest {

    @Test
    @DisplayName("测试CRC32校验和计算 - 字节数组")
    void testCalculateCRC32WithByteArray() {
        // 准备测试数据
        byte[] data = "Hello, ZephyrMQ!".getBytes();

        // 计算CRC32
        int crc32 = MessageChecksum.calculateCRC32(data);

        // 验证结果不为0且具有确定性
        assertNotEquals(0, crc32);

        // 多次计算应该得到相同结果
        int crc32Second = MessageChecksum.calculateCRC32(data);
        assertEquals(crc32, crc32Second);
    }

    @Test
    @DisplayName("测试CRC32校验和计算 - ByteBuffer")
    void testCalculateCRC32WithByteBuffer() {
        // 准备测试数据
        byte[] data = "Hello, ZephyrMQ with ByteBuffer!".getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // 计算CRC32
        int crc32 = MessageChecksum.calculateCRC32(buffer);

        // 验证结果不为0
        assertNotEquals(0, crc32);

        // 验证Buffer位置没有改变
        assertEquals(0, buffer.position());
        assertEquals(data.length, buffer.limit());

        // 与字节数组方法结果一致
        int crc32Array = MessageChecksum.calculateCRC32(data);
        assertEquals(crc32Array, crc32);
    }

    @Test
    @DisplayName("测试CRC32校验和验证")
    void testVerifyCRC32() {
        // 准备测试数据
        byte[] data = "Test message for verification".getBytes();

        // 计算期望的校验和
        int expectedCRC = MessageChecksum.calculateCRC32(data);

        // 验证正确的校验和
        assertTrue(MessageChecksum.verifyCRC32(data, expectedCRC));

        // 验证错误的校验和
        assertFalse(MessageChecksum.verifyCRC32(data, expectedCRC + 1));
    }

    @Test
    @DisplayName("测试空数据和null数据的处理")
    void testNullAndEmptyData() {
        // null数据
        assertEquals(0, MessageChecksum.calculateCRC32((byte[]) null));
        assertEquals(0, MessageChecksum.calculateCRC32((ByteBuffer) null));

        // 空数据
        assertEquals(0, MessageChecksum.calculateCRC32(new byte[0]));
        assertEquals(0, MessageChecksum.calculateCRC32(ByteBuffer.allocate(0)));

        // 验证null和空数据
        assertTrue(MessageChecksum.verifyCRC32((byte[]) null, 0));
        assertTrue(MessageChecksum.verifyCRC32(new byte[0], 0));
    }

    @Test
    @DisplayName("测试消息完整性校验")
    void testCalculateMessageIntegrity() {
        // 准备测试数据：模拟消息头 + 消息体
        byte[] messageHeader = "HEADER1234567890".getBytes(); // 16字节消息头
        byte[] messageBody = "This is message body".getBytes();

        // 组合完整消息
        byte[] fullMessage = new byte[messageHeader.length + messageBody.length];
        System.arraycopy(messageHeader, 0, fullMessage, 0, messageHeader.length);
        System.arraycopy(messageBody, 0, fullMessage, messageHeader.length, messageBody.length);

        // 计算完整性校验和
        int integrityCRC = MessageChecksum.calculateMessageIntegrity(fullMessage, messageHeader.length);

        // 验证结果
        assertNotEquals(0, integrityCRC);

        // 多次计算应该得到相同结果
        int integrityCRCSecond = MessageChecksum.calculateMessageIntegrity(fullMessage, messageHeader.length);
        assertEquals(integrityCRC, integrityCRCSecond);
    }

    @Test
    @DisplayName("测试批量CRC32计算")
    void testCalculateCombinedCRC32() {
        // 准备多个数据块
        byte[] block1 = "Block1".getBytes();
        byte[] block2 = "Block2".getBytes();
        byte[] block3 = "Block3".getBytes();

        // 计算组合CRC32
        int combinedCRC = MessageChecksum.calculateCombinedCRC32(block1, block2, block3);

        // 验证结果
        assertNotEquals(0, combinedCRC);

        // 计算连接后的数据CRC32进行对比
        byte[] concatenated = new byte[block1.length + block2.length + block3.length];
        System.arraycopy(block1, 0, concatenated, 0, block1.length);
        System.arraycopy(block2, 0, concatenated, block1.length, block2.length);
        System.arraycopy(block3, 0, concatenated, block1.length + block2.length, block3.length);

        int concatenatedCRC = MessageChecksum.calculateCRC32(concatenated);
        assertEquals(concatenatedCRC, combinedCRC);
    }

    @Test
    @DisplayName("测试不同校验算法")
    void testDifferentChecksumAlgorithms() {
        // 准备测试数据
        byte[] data = "Test data for different algorithms".getBytes();

        // 测试CRC32算法
        long crc32Value = MessageChecksum.calculateChecksum(data, MessageChecksum.ChecksumAlgorithm.CRC32);
        assertNotEquals(0, crc32Value);

        // 测试Adler32算法
        long adler32Value = MessageChecksum.calculateChecksum(data, MessageChecksum.ChecksumAlgorithm.ADLER32);
        assertNotEquals(0, adler32Value);

        // 两种算法结果应该不同
        assertNotEquals(crc32Value, adler32Value);
    }

    @Test
    @DisplayName("测试校验结果类")
    void testChecksumResult() {
        // 测试成功结果
        MessageChecksum.ChecksumResult successResult = MessageChecksum.ChecksumResult.success(12345);
        assertTrue(successResult.isValid());
        assertEquals(12345, successResult.getExpectedChecksum());
        assertEquals(12345, successResult.getActualChecksum());
        assertNull(successResult.getErrorMessage());

        // 测试失败结果
        MessageChecksum.ChecksumResult failureResult = MessageChecksum.ChecksumResult.failure(
            12345, 54321, "Test error");
        assertFalse(failureResult.isValid());
        assertEquals(12345, failureResult.getExpectedChecksum());
        assertEquals(54321, failureResult.getActualChecksum());
        assertEquals("Test error", failureResult.getErrorMessage());

        // 测试toString方法
        assertNotNull(successResult.toString());
        assertNotNull(failureResult.toString());
        assertTrue(successResult.toString().contains("valid=true"));
        assertTrue(failureResult.toString().contains("valid=false"));
    }

    @Test
    @DisplayName("测试大数据量CRC32计算性能")
    void testLargeDataCRC32Performance() {
        // 创建大数据量（1MB）
        byte[] largeData = new byte[1024 * 1024];
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }

        // 测量计算时间
        long startTime = System.currentTimeMillis();
        int crc32 = MessageChecksum.calculateCRC32(largeData);
        long endTime = System.currentTimeMillis();

        // 验证结果
        assertNotEquals(0, crc32);

        // 性能应该在合理范围内（<100ms）
        long duration = endTime - startTime;
        assertTrue(duration < 100, "CRC32 calculation took too long: " + duration + "ms");

        System.out.println("CRC32 calculation for 1MB data took: " + duration + "ms");
    }

    @Test
    @DisplayName("测试ByteBuffer的不同状态")
    void testByteBufferDifferentStates() {
        // 准备测试数据
        byte[] data = "ByteBuffer state test".getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // 移动position
        buffer.position(5);
        buffer.limit(data.length - 3);

        // 计算CRC32
        int originalPosition = buffer.position();
        int originalLimit = buffer.limit();

        int crc32 = MessageChecksum.calculateCRC32(buffer);

        // 验证buffer状态没有改变
        assertEquals(originalPosition, buffer.position());
        assertEquals(originalLimit, buffer.limit());

        // 验证结果
        assertNotEquals(0, crc32);

        // 验证结果与对应子数组一致
        byte[] subArray = new byte[buffer.remaining()];
        buffer.get(subArray);
        buffer.position(originalPosition); // 重置位置

        int subArrayCRC = MessageChecksum.calculateCRC32(subArray);
        assertEquals(subArrayCRC, crc32);
    }
}