package com.zephyr.storage.checksum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * 消息校验和工具类
 * 提供CRC32校验和计算和验证功能
 */
public class MessageChecksum {

    private static final Logger logger = LoggerFactory.getLogger(MessageChecksum.class);

    /**
     * 计算消息体的CRC32校验和
     *
     * @param data 消息体数据
     * @return CRC32校验和值
     */
    public static int calculateCRC32(byte[] data) {
        if (data == null || data.length == 0) {
            return 0;
        }

        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return (int) crc32.getValue();
    }

    /**
     * 计算ByteBuffer的CRC32校验和
     *
     * @param buffer ByteBuffer数据
     * @return CRC32校验和值
     */
    public static int calculateCRC32(ByteBuffer buffer) {
        if (buffer == null || !buffer.hasRemaining()) {
            return 0;
        }

        CRC32 crc32 = new CRC32();

        // 保存原始位置
        int originalPosition = buffer.position();
        int originalLimit = buffer.limit();

        try {
            // 更新校验和
            if (buffer.hasArray()) {
                // 如果有底层数组，直接使用数组计算
                crc32.update(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
            } else {
                // 否则逐字节计算
                while (buffer.hasRemaining()) {
                    crc32.update(buffer.get());
                }
            }

            return (int) crc32.getValue();
        } finally {
            // 恢复原始位置
            buffer.position(originalPosition);
            buffer.limit(originalLimit);
        }
    }

    /**
     * 验证消息体校验和
     *
     * @param data 消息体数据
     * @param expectedChecksum 期望的校验和
     * @return 校验是否通过
     */
    public static boolean verifyCRC32(byte[] data, int expectedChecksum) {
        int actualChecksum = calculateCRC32(data);
        boolean isValid = actualChecksum == expectedChecksum;

        if (!isValid) {
            logger.warn("CRC32 checksum verification failed: expected={}, actual={}",
                       expectedChecksum, actualChecksum);
        }

        return isValid;
    }

    /**
     * 验证ByteBuffer校验和
     *
     * @param buffer ByteBuffer数据
     * @param expectedChecksum 期望的校验和
     * @return 校验是否通过
     */
    public static boolean verifyCRC32(ByteBuffer buffer, int expectedChecksum) {
        int actualChecksum = calculateCRC32(buffer);
        boolean isValid = actualChecksum == expectedChecksum;

        if (!isValid) {
            logger.warn("CRC32 checksum verification failed: expected={}, actual={}",
                       expectedChecksum, actualChecksum);
        }

        return isValid;
    }

    /**
     * 计算消息完整性校验和（包含消息头和消息体）
     *
     * @param messageBytes 完整消息字节
     * @param bodyOffset 消息体在完整消息中的偏移量
     * @return 完整性校验和
     */
    public static int calculateMessageIntegrity(byte[] messageBytes, int bodyOffset) {
        if (messageBytes == null || messageBytes.length <= bodyOffset) {
            return 0;
        }

        CRC32 crc32 = new CRC32();

        // 计算消息头校验和（排除原始的CRC字段）
        if (bodyOffset > 8) { // 假设CRC字段在消息头的第8-12字节位置
            crc32.update(messageBytes, 0, 8); // 计算CRC字段之前的部分
            crc32.update(messageBytes, 12, bodyOffset - 12); // 计算CRC字段之后到消息体之前的部分
        }

        // 计算消息体校验和
        crc32.update(messageBytes, bodyOffset, messageBytes.length - bodyOffset);

        return (int) crc32.getValue();
    }

    /**
     * 批量校验多个数据块
     *
     * @param dataBlocks 数据块数组
     * @return 综合校验和
     */
    public static int calculateCombinedCRC32(byte[]... dataBlocks) {
        CRC32 crc32 = new CRC32();

        for (byte[] data : dataBlocks) {
            if (data != null && data.length > 0) {
                crc32.update(data);
            }
        }

        return (int) crc32.getValue();
    }

    /**
     * 使用不同的校验算法进行校验
     */
    public enum ChecksumAlgorithm {
        CRC32("CRC32"),
        ADLER32("Adler32");

        private final String name;

        ChecksumAlgorithm(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    /**
     * 通用校验和计算方法
     *
     * @param data 数据
     * @param algorithm 校验算法
     * @return 校验和值
     */
    public static long calculateChecksum(byte[] data, ChecksumAlgorithm algorithm) {
        if (data == null || data.length == 0) {
            return 0;
        }

        Checksum checksum;
        switch (algorithm) {
            case CRC32:
                checksum = new CRC32();
                break;
            case ADLER32:
                checksum = new java.util.zip.Adler32();
                break;
            default:
                throw new IllegalArgumentException("Unsupported checksum algorithm: " + algorithm);
        }

        checksum.update(data);
        return checksum.getValue();
    }

    /**
     * 校验结果类
     */
    public static class ChecksumResult {
        private final boolean valid;
        private final int expectedChecksum;
        private final int actualChecksum;
        private final String errorMessage;

        public ChecksumResult(boolean valid, int expectedChecksum, int actualChecksum, String errorMessage) {
            this.valid = valid;
            this.expectedChecksum = expectedChecksum;
            this.actualChecksum = actualChecksum;
            this.errorMessage = errorMessage;
        }

        public static ChecksumResult success(int checksum) {
            return new ChecksumResult(true, checksum, checksum, null);
        }

        public static ChecksumResult failure(int expected, int actual, String message) {
            return new ChecksumResult(false, expected, actual, message);
        }

        public boolean isValid() { return valid; }
        public int getExpectedChecksum() { return expectedChecksum; }
        public int getActualChecksum() { return actualChecksum; }
        public String getErrorMessage() { return errorMessage; }

        @Override
        public String toString() {
            if (valid) {
                return String.format("ChecksumResult{valid=true, checksum=%d}", expectedChecksum);
            } else {
                return String.format("ChecksumResult{valid=false, expected=%d, actual=%d, error='%s'}",
                                   expectedChecksum, actualChecksum, errorMessage);
            }
        }
    }
}