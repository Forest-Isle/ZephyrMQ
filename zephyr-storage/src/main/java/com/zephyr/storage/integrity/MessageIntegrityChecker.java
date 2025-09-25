package com.zephyr.storage.integrity;

import com.zephyr.storage.checksum.MessageChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 消息完整性检查器
 * 提供多层次的消息完整性验证功能
 */
public class MessageIntegrityChecker {

    private static final Logger logger = LoggerFactory.getLogger(MessageIntegrityChecker.class);

    /**
     * 完整性检查类型
     */
    public enum IntegrityCheckType {
        BASIC,          // 基础检查：魔数、长度等
        CHECKSUM,       // 校验和检查
        FULL           // 完整检查：包含所有检查项
    }

    /**
     * 检查结果严重程度
     */
    public enum Severity {
        INFO,           // 信息
        WARNING,        // 警告
        ERROR,          // 错误
        CRITICAL        // 严重错误
    }

    /**
     * 完整性检查结果
     */
    public static class IntegrityCheckResult {
        private final boolean passed;
        private final List<CheckViolation> violations;
        private final long checkDurationMs;

        public IntegrityCheckResult(boolean passed, List<CheckViolation> violations, long checkDurationMs) {
            this.passed = passed;
            this.violations = violations != null ? violations : new ArrayList<>();
            this.checkDurationMs = checkDurationMs;
        }

        public boolean isPassed() { return passed; }
        public List<CheckViolation> getViolations() { return violations; }
        public long getCheckDurationMs() { return checkDurationMs; }

        public boolean hasViolationsOfSeverity(Severity severity) {
            return violations.stream().anyMatch(v -> v.getSeverity() == severity);
        }

        public int getViolationCount(Severity severity) {
            return (int) violations.stream().filter(v -> v.getSeverity() == severity).count();
        }

        @Override
        public String toString() {
            return String.format("IntegrityCheckResult{passed=%s, violations=%d, duration=%dms}",
                               passed, violations.size(), checkDurationMs);
        }
    }

    /**
     * 检查违规信息
     */
    public static class CheckViolation {
        private final String checkName;
        private final Severity severity;
        private final String message;
        private final Object expectedValue;
        private final Object actualValue;

        public CheckViolation(String checkName, Severity severity, String message,
                            Object expectedValue, Object actualValue) {
            this.checkName = checkName;
            this.severity = severity;
            this.message = message;
            this.expectedValue = expectedValue;
            this.actualValue = actualValue;
        }

        public String getCheckName() { return checkName; }
        public Severity getSeverity() { return severity; }
        public String getMessage() { return message; }
        public Object getExpectedValue() { return expectedValue; }
        public Object getActualValue() { return actualValue; }

        @Override
        public String toString() {
            return String.format("[%s] %s: %s (expected: %s, actual: %s)",
                               severity, checkName, message, expectedValue, actualValue);
        }
    }

    // 消息魔数常量
    public static final int MESSAGE_MAGIC_CODE = 0xDAA320A7;
    public static final int BLANK_MAGIC_CODE = 0xBB10F7B4;

    /**
     * 执行消息完整性检查
     *
     * @param messageBytes 消息字节数据
     * @param checkType 检查类型
     * @param maxMessageSize 最大消息大小
     * @return 检查结果
     */
    public static IntegrityCheckResult checkMessageIntegrity(byte[] messageBytes,
                                                           IntegrityCheckType checkType,
                                                           int maxMessageSize) {
        long startTime = System.currentTimeMillis();
        List<CheckViolation> violations = new ArrayList<>();

        try {
            if (messageBytes == null || messageBytes.length == 0) {
                violations.add(new CheckViolation("NULL_CHECK", Severity.CRITICAL,
                    "Message bytes is null or empty", "non-null", messageBytes));
                return new IntegrityCheckResult(false, violations, System.currentTimeMillis() - startTime);
            }

            // 基础检查
            performBasicChecks(messageBytes, maxMessageSize, violations);

            // 校验和检查
            if (checkType == IntegrityCheckType.CHECKSUM || checkType == IntegrityCheckType.FULL) {
                performChecksumChecks(messageBytes, violations);
            }

            // 完整检查
            if (checkType == IntegrityCheckType.FULL) {
                performExtendedChecks(messageBytes, violations);
            }

            boolean passed = violations.stream().noneMatch(v -> v.getSeverity() == Severity.ERROR || v.getSeverity() == Severity.CRITICAL);
            return new IntegrityCheckResult(passed, violations, System.currentTimeMillis() - startTime);

        } catch (Exception e) {
            logger.error("Error during integrity check", e);
            violations.add(new CheckViolation("EXCEPTION_CHECK", Severity.CRITICAL,
                "Exception during integrity check: " + e.getMessage(), "no exception", e.getClass().getSimpleName()));
            return new IntegrityCheckResult(false, violations, System.currentTimeMillis() - startTime);
        }
    }

    /**
     * 执行基础完整性检查
     */
    private static void performBasicChecks(byte[] messageBytes, int maxMessageSize, List<CheckViolation> violations) {
        ByteBuffer buffer = ByteBuffer.wrap(messageBytes);

        // 检查最小长度
        if (messageBytes.length < 12) { // 至少需要 totalSize(4) + magicCode(4) + bodyCRC(4)
            violations.add(new CheckViolation("MIN_LENGTH_CHECK", Severity.CRITICAL,
                "Message too short", ">=12 bytes", messageBytes.length + " bytes"));
            return;
        }

        // 检查总长度
        int totalSize = buffer.getInt();
        if (totalSize <= 0) {
            violations.add(new CheckViolation("TOTAL_SIZE_CHECK", Severity.CRITICAL,
                "Invalid total size", ">0", totalSize));
        } else if (totalSize > maxMessageSize) {
            violations.add(new CheckViolation("MAX_SIZE_CHECK", Severity.ERROR,
                "Message exceeds maximum size", "<=" + maxMessageSize, totalSize));
        } else if (totalSize != messageBytes.length) {
            violations.add(new CheckViolation("SIZE_CONSISTENCY_CHECK", Severity.ERROR,
                "Total size doesn't match message length", totalSize, messageBytes.length));
        }

        // 检查魔数
        int magicCode = buffer.getInt();
        if (magicCode != MESSAGE_MAGIC_CODE && magicCode != BLANK_MAGIC_CODE) {
            violations.add(new CheckViolation("MAGIC_CODE_CHECK", Severity.CRITICAL,
                "Invalid magic code", String.format("0x%08X", MESSAGE_MAGIC_CODE), String.format("0x%08X", magicCode)));
        }
    }

    /**
     * 执行校验和检查
     */
    private static void performChecksumChecks(byte[] messageBytes, List<CheckViolation> violations) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(messageBytes);

            // 跳过totalSize和magicCode
            buffer.getInt(); // totalSize
            buffer.getInt(); // magicCode

            // 读取存储的CRC
            int storedCRC = buffer.getInt();

            // 找到消息体位置（这需要根据实际消息格式调整）
            if (buffer.remaining() < 80) { // 消息头至少80字节
                violations.add(new CheckViolation("HEADER_SIZE_CHECK", Severity.WARNING,
                    "Message header too short for CRC verification", ">=80 bytes", buffer.remaining() + " bytes"));
                return;
            }

            // 跳过消息头到消息体
            buffer.position(buffer.position() + 76); // 跳过其他头部字段

            // 读取压缩标志
            boolean isCompressed = buffer.get() == 1;

            // 读取topic长度和内容
            int topicLen = buffer.getInt();
            if (topicLen < 0 || topicLen > 255 || buffer.remaining() < topicLen) {
                violations.add(new CheckViolation("TOPIC_LENGTH_CHECK", Severity.ERROR,
                    "Invalid topic length", "0-255 and fits in buffer", topicLen));
                return;
            }
            buffer.position(buffer.position() + topicLen);

            // 读取tags长度和内容
            if (buffer.remaining() < 1) return;
            int tagsLen = buffer.get() & 0xFF;
            if (buffer.remaining() < tagsLen) {
                violations.add(new CheckViolation("TAGS_LENGTH_CHECK", Severity.ERROR,
                    "Invalid tags length", "fits in buffer", tagsLen));
                return;
            }
            buffer.position(buffer.position() + tagsLen);

            // 读取keys长度和内容
            if (buffer.remaining() < 2) return;
            int keysLen = buffer.getShort() & 0xFFFF;
            if (buffer.remaining() < keysLen) {
                violations.add(new CheckViolation("KEYS_LENGTH_CHECK", Severity.ERROR,
                    "Invalid keys length", "fits in buffer", keysLen));
                return;
            }
            buffer.position(buffer.position() + keysLen);

            // 读取消息体长度和内容
            if (buffer.remaining() < 4) return;
            int bodyLen = buffer.getInt();
            if (bodyLen < 0 || buffer.remaining() < bodyLen) {
                violations.add(new CheckViolation("BODY_LENGTH_CHECK", Severity.ERROR,
                    "Invalid body length", ">=0 and fits in buffer", bodyLen));
                return;
            }

            // 读取消息体
            byte[] bodyBytes = new byte[bodyLen];
            buffer.get(bodyBytes);

            // 如果是压缩消息，需要解压后再校验（这里简化处理）
            byte[] finalBodyBytes = bodyBytes;
            if (isCompressed) {
                // 注意：实际应用中需要解压缩，这里为了演示简化处理
                logger.debug("Compressed message detected in integrity check");
            }

            // 验证CRC
            int calculatedCRC = MessageChecksum.calculateCRC32(finalBodyBytes);
            if (calculatedCRC != storedCRC) {
                violations.add(new CheckViolation("CRC_CHECK", Severity.CRITICAL,
                    "CRC checksum mismatch", storedCRC, calculatedCRC));
            }

        } catch (Exception e) {
            logger.warn("Error during checksum verification", e);
            violations.add(new CheckViolation("CHECKSUM_EXCEPTION", Severity.WARNING,
                "Exception during checksum check: " + e.getMessage(), "no exception", e.getClass().getSimpleName()));
        }
    }

    /**
     * 执行扩展检查
     */
    private static void performExtendedChecks(byte[] messageBytes, List<CheckViolation> violations) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(messageBytes);

            // 跳过基本字段
            buffer.getInt(); // totalSize
            buffer.getInt(); // magicCode
            buffer.getInt(); // bodyCRC

            if (buffer.remaining() < 76) return;

            int queueId = buffer.getInt();
            int flag = buffer.getInt();
            long queueOffset = buffer.getLong();
            long physicalOffset = buffer.getLong();
            int sysFlag = buffer.getInt();
            long bornTimestamp = buffer.getLong();
            buffer.getLong(); // bornHost
            long storeTimestamp = buffer.getLong();
            buffer.getLong(); // storeHost
            int reconsumeTimes = buffer.getInt();
            long preparedTransactionOffset = buffer.getLong();

            // 验证时间戳合理性
            long currentTime = System.currentTimeMillis();
            if (bornTimestamp > currentTime + 60000) { // 允许1分钟的时钟偏差
                violations.add(new CheckViolation("BORN_TIMESTAMP_CHECK", Severity.WARNING,
                    "Born timestamp is in future", "<=current+1min", bornTimestamp));
            }

            if (storeTimestamp > currentTime + 60000) {
                violations.add(new CheckViolation("STORE_TIMESTAMP_CHECK", Severity.WARNING,
                    "Store timestamp is in future", "<=current+1min", storeTimestamp));
            }

            // 验证队列ID范围
            if (queueId < 0 || queueId > 1000000) {
                violations.add(new CheckViolation("QUEUE_ID_CHECK", Severity.WARNING,
                    "Queue ID out of reasonable range", "0-1000000", queueId));
            }

            // 验证重消费次数
            if (reconsumeTimes < 0 || reconsumeTimes > 1000) {
                violations.add(new CheckViolation("RECONSUME_TIMES_CHECK", Severity.WARNING,
                    "Reconsume times out of reasonable range", "0-1000", reconsumeTimes));
            }

            // 验证偏移量
            if (queueOffset < 0) {
                violations.add(new CheckViolation("QUEUE_OFFSET_CHECK", Severity.ERROR,
                    "Invalid queue offset", ">=0", queueOffset));
            }

            if (physicalOffset < 0) {
                violations.add(new CheckViolation("PHYSICAL_OFFSET_CHECK", Severity.ERROR,
                    "Invalid physical offset", ">=0", physicalOffset));
            }

        } catch (Exception e) {
            logger.warn("Error during extended checks", e);
            violations.add(new CheckViolation("EXTENDED_CHECK_EXCEPTION", Severity.WARNING,
                "Exception during extended check: " + e.getMessage(), "no exception", e.getClass().getSimpleName()));
        }
    }

    /**
     * 批量检查多个消息
     *
     * @param messages 消息数组
     * @param checkType 检查类型
     * @param maxMessageSize 最大消息大小
     * @return 批量检查结果
     */
    public static BatchIntegrityCheckResult batchCheckIntegrity(List<byte[]> messages,
                                                              IntegrityCheckType checkType,
                                                              int maxMessageSize) {
        long startTime = System.currentTimeMillis();
        int passedCount = 0;
        int failedCount = 0;
        List<IntegrityCheckResult> results = new ArrayList<>();

        for (int i = 0; i < messages.size(); i++) {
            byte[] message = messages.get(i);
            IntegrityCheckResult result = checkMessageIntegrity(message, checkType, maxMessageSize);
            results.add(result);

            if (result.isPassed()) {
                passedCount++;
            } else {
                failedCount++;
                logger.warn("Message {} failed integrity check: {}", i, result);
            }
        }

        long totalDuration = System.currentTimeMillis() - startTime;
        return new BatchIntegrityCheckResult(messages.size(), passedCount, failedCount, results, totalDuration);
    }

    /**
     * 批量完整性检查结果
     */
    public static class BatchIntegrityCheckResult {
        private final int totalCount;
        private final int passedCount;
        private final int failedCount;
        private final List<IntegrityCheckResult> results;
        private final long totalDurationMs;

        public BatchIntegrityCheckResult(int totalCount, int passedCount, int failedCount,
                                       List<IntegrityCheckResult> results, long totalDurationMs) {
            this.totalCount = totalCount;
            this.passedCount = passedCount;
            this.failedCount = failedCount;
            this.results = results;
            this.totalDurationMs = totalDurationMs;
        }

        public int getTotalCount() { return totalCount; }
        public int getPassedCount() { return passedCount; }
        public int getFailedCount() { return failedCount; }
        public List<IntegrityCheckResult> getResults() { return results; }
        public long getTotalDurationMs() { return totalDurationMs; }

        public double getSuccessRate() {
            return totalCount > 0 ? (double) passedCount / totalCount : 0.0;
        }

        @Override
        public String toString() {
            return String.format("BatchIntegrityCheckResult{total=%d, passed=%d, failed=%d, successRate=%.2f%%, duration=%dms}",
                               totalCount, passedCount, failedCount, getSuccessRate() * 100, totalDurationMs);
        }
    }
}