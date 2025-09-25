package com.zephyr.storage.integrity;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 消息完整性检查器测试
 */
class MessageIntegrityCheckerTest {

    private static final int MESSAGE_MAGIC_CODE = 0xDAA320A7;
    private static final int MAX_MESSAGE_SIZE = 1024 * 1024; // 1MB

    @Test
    @DisplayName("测试null和空消息检查")
    void testNullAndEmptyMessageCheck() {
        // null消息
        MessageIntegrityChecker.IntegrityCheckResult result =
            MessageIntegrityChecker.checkMessageIntegrity(null,
                MessageIntegrityChecker.IntegrityCheckType.BASIC, MAX_MESSAGE_SIZE);

        assertFalse(result.isPassed());
        assertEquals(1, result.getViolations().size());
        assertEquals(MessageIntegrityChecker.Severity.CRITICAL,
                    result.getViolations().get(0).getSeverity());

        // 空消息
        result = MessageIntegrityChecker.checkMessageIntegrity(new byte[0],
            MessageIntegrityChecker.IntegrityCheckType.BASIC, MAX_MESSAGE_SIZE);

        assertFalse(result.isPassed());
        assertEquals(1, result.getViolations().size());
    }

    @Test
    @DisplayName("测试消息长度检查")
    void testMessageLengthCheck() {
        // 消息太短
        byte[] shortMessage = new byte[8]; // 小于最小长度12
        MessageIntegrityChecker.IntegrityCheckResult result =
            MessageIntegrityChecker.checkMessageIntegrity(shortMessage,
                MessageIntegrityChecker.IntegrityCheckType.BASIC, MAX_MESSAGE_SIZE);

        assertFalse(result.isPassed());
        assertTrue(result.hasViolationsOfSeverity(MessageIntegrityChecker.Severity.CRITICAL));
    }

    @Test
    @DisplayName("测试魔数检查")
    void testMagicCodeCheck() {
        // 创建有效长度但错误魔数的消息
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putInt(16); // totalSize
        buffer.putInt(0x12345678); // 错误的魔数
        buffer.putInt(0); // bodyCRC
        buffer.putInt(0); // padding

        MessageIntegrityChecker.IntegrityCheckResult result =
            MessageIntegrityChecker.checkMessageIntegrity(buffer.array(),
                MessageIntegrityChecker.IntegrityCheckType.BASIC, MAX_MESSAGE_SIZE);

        assertFalse(result.isPassed());
        assertTrue(result.getViolations().stream()
            .anyMatch(v -> v.getCheckName().equals("MAGIC_CODE_CHECK")));
    }

    @Test
    @DisplayName("测试正确的基础消息检查")
    void testValidBasicMessage() {
        // 创建最小的有效消息
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putInt(16); // totalSize
        buffer.putInt(MESSAGE_MAGIC_CODE); // 正确的魔数
        buffer.putInt(0); // bodyCRC
        buffer.putInt(0); // padding

        MessageIntegrityChecker.IntegrityCheckResult result =
            MessageIntegrityChecker.checkMessageIntegrity(buffer.array(),
                MessageIntegrityChecker.IntegrityCheckType.BASIC, MAX_MESSAGE_SIZE);

        // 应该通过基础检查（虽然可能有警告）
        assertTrue(result.isPassed() ||
                  result.getViolations().stream().noneMatch(v ->
                      v.getSeverity() == MessageIntegrityChecker.Severity.CRITICAL ||
                      v.getSeverity() == MessageIntegrityChecker.Severity.ERROR));
    }

    @Test
    @DisplayName("测试消息大小检查")
    void testMessageSizeCheck() {
        // 创建超大消息声明
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putInt(MAX_MESSAGE_SIZE + 1); // 超出最大大小
        buffer.putInt(MESSAGE_MAGIC_CODE);
        buffer.putInt(0);
        buffer.putInt(0);

        MessageIntegrityChecker.IntegrityCheckResult result =
            MessageIntegrityChecker.checkMessageIntegrity(buffer.array(),
                MessageIntegrityChecker.IntegrityCheckType.BASIC, MAX_MESSAGE_SIZE);

        assertFalse(result.isPassed());
        assertTrue(result.getViolations().stream()
            .anyMatch(v -> v.getCheckName().equals("MAX_SIZE_CHECK")));
    }

    @Test
    @DisplayName("测试大小一致性检查")
    void testSizeConsistencyCheck() {
        // 创建大小不一致的消息
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putInt(20); // 声明20字节，但实际只有16字节
        buffer.putInt(MESSAGE_MAGIC_CODE);
        buffer.putInt(0);
        buffer.putInt(0);

        MessageIntegrityChecker.IntegrityCheckResult result =
            MessageIntegrityChecker.checkMessageIntegrity(buffer.array(),
                MessageIntegrityChecker.IntegrityCheckType.BASIC, MAX_MESSAGE_SIZE);

        assertFalse(result.isPassed());
        assertTrue(result.getViolations().stream()
            .anyMatch(v -> v.getCheckName().equals("SIZE_CONSISTENCY_CHECK")));
    }

    @Test
    @DisplayName("测试检查结果严重程度过滤")
    void testSeverityFiltering() {
        // 创建一个会产生多种严重程度违规的消息
        byte[] shortMessage = new byte[8];

        MessageIntegrityChecker.IntegrityCheckResult result =
            MessageIntegrityChecker.checkMessageIntegrity(shortMessage,
                MessageIntegrityChecker.IntegrityCheckType.FULL, MAX_MESSAGE_SIZE);

        // 测试严重程度过滤方法
        assertTrue(result.hasViolationsOfSeverity(MessageIntegrityChecker.Severity.CRITICAL));
        assertEquals(1, result.getViolationCount(MessageIntegrityChecker.Severity.CRITICAL));
    }

    @Test
    @DisplayName("测试批量完整性检查")
    void testBatchIntegrityCheck() {
        // 创建测试消息列表
        List<byte[]> messages = Arrays.asList(
            createValidMessage(),
            createInvalidMagicCodeMessage(),
            new byte[8], // 太短的消息
            createValidMessage()
        );

        MessageIntegrityChecker.BatchIntegrityCheckResult batchResult =
            MessageIntegrityChecker.batchCheckIntegrity(messages,
                MessageIntegrityChecker.IntegrityCheckType.BASIC, MAX_MESSAGE_SIZE);

        assertEquals(4, batchResult.getTotalCount());
        assertEquals(2, batchResult.getPassedCount());
        assertEquals(2, batchResult.getFailedCount());
        assertEquals(0.5, batchResult.getSuccessRate(), 0.01);

        assertNotNull(batchResult.toString());
    }

    @Test
    @DisplayName("测试检查性能")
    void testCheckPerformance() {
        byte[] message = createValidMessage();

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            MessageIntegrityChecker.checkMessageIntegrity(message,
                MessageIntegrityChecker.IntegrityCheckType.BASIC, MAX_MESSAGE_SIZE);
        }
        long duration = System.currentTimeMillis() - startTime;

        // 1000次检查应该在合理时间内完成（<100ms）
        assertTrue(duration < 100, "Integrity check performance too slow: " + duration + "ms");

        System.out.println("1000 integrity checks took: " + duration + "ms");
    }

    @Test
    @DisplayName("测试检查违规信息")
    void testCheckViolationInfo() {
        MessageIntegrityChecker.CheckViolation violation =
            new MessageIntegrityChecker.CheckViolation(
                "TEST_CHECK",
                MessageIntegrityChecker.Severity.ERROR,
                "Test violation message",
                "expected",
                "actual"
            );

        assertEquals("TEST_CHECK", violation.getCheckName());
        assertEquals(MessageIntegrityChecker.Severity.ERROR, violation.getSeverity());
        assertEquals("Test violation message", violation.getMessage());
        assertEquals("expected", violation.getExpectedValue());
        assertEquals("actual", violation.getActualValue());

        assertNotNull(violation.toString());
        assertTrue(violation.toString().contains("ERROR"));
    }

    @Test
    @DisplayName("测试不同检查类型")
    void testDifferentCheckTypes() {
        byte[] message = createComplexValidMessage();

        // 基础检查
        MessageIntegrityChecker.IntegrityCheckResult basicResult =
            MessageIntegrityChecker.checkMessageIntegrity(message,
                MessageIntegrityChecker.IntegrityCheckType.BASIC, MAX_MESSAGE_SIZE);

        // 校验和检查
        MessageIntegrityChecker.IntegrityCheckResult checksumResult =
            MessageIntegrityChecker.checkMessageIntegrity(message,
                MessageIntegrityChecker.IntegrityCheckType.CHECKSUM, MAX_MESSAGE_SIZE);

        // 完整检查
        MessageIntegrityChecker.IntegrityCheckResult fullResult =
            MessageIntegrityChecker.checkMessageIntegrity(message,
                MessageIntegrityChecker.IntegrityCheckType.FULL, MAX_MESSAGE_SIZE);

        // 检查类型越全面，检查时间应该越长
        assertTrue(fullResult.getCheckDurationMs() >= basicResult.getCheckDurationMs());
    }

    // 辅助方法：创建有效的基础消息
    private byte[] createValidMessage() {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putInt(16); // totalSize
        buffer.putInt(MESSAGE_MAGIC_CODE); // 正确魔数
        buffer.putInt(0); // bodyCRC
        buffer.putInt(0); // padding
        return buffer.array();
    }

    // 辅助方法：创建错误魔数的消息
    private byte[] createInvalidMagicCodeMessage() {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putInt(16);
        buffer.putInt(0x12345678); // 错误魔数
        buffer.putInt(0);
        buffer.putInt(0);
        return buffer.array();
    }

    // 辅助方法：创建复杂的有效消息
    private byte[] createComplexValidMessage() {
        ByteBuffer buffer = ByteBuffer.allocate(200);

        // 消息头
        buffer.putInt(200); // totalSize
        buffer.putInt(MESSAGE_MAGIC_CODE); // magicCode
        buffer.putInt(12345); // bodyCRC
        buffer.putInt(1); // queueId
        buffer.putInt(0); // flag
        buffer.putLong(100L); // queueOffset
        buffer.putLong(1000L); // physicalOffset
        buffer.putInt(0); // sysFlag
        buffer.putLong(System.currentTimeMillis()); // bornTimestamp
        buffer.putLong(0L); // bornHost
        buffer.putLong(System.currentTimeMillis()); // storeTimestamp
        buffer.putLong(0L); // storeHost
        buffer.putInt(0); // reconsumeTimes
        buffer.putLong(0L); // preparedTransactionOffset

        // 压缩标志
        buffer.put((byte) 0);

        // Topic
        String topic = "TestTopic";
        buffer.putInt(topic.length());
        buffer.put(topic.getBytes());

        // Tags
        buffer.put((byte) 0);

        // Keys
        buffer.putShort((short) 0);

        // Body
        String body = "Test message body";
        buffer.putInt(body.length());
        buffer.put(body.getBytes());

        // 填充剩余空间
        while (buffer.hasRemaining()) {
            buffer.put((byte) 0);
        }

        return buffer.array();
    }
}