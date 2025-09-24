package com.zephyr.storage.compression;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 消息压缩工具类
 * 支持LZ4和Snappy压缩算法
 */
public class MessageCompressor {

    private static final Logger logger = LoggerFactory.getLogger(MessageCompressor.class);

    // 压缩算法类型
    public enum CompressionType {
        NONE((byte) 0),
        LZ4((byte) 1),
        SNAPPY((byte) 2);

        private final byte code;

        CompressionType(byte code) {
            this.code = code;
        }

        public byte getCode() {
            return code;
        }

        public static CompressionType fromCode(byte code) {
            for (CompressionType type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            return NONE;
        }
    }

    private static final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
    private static final LZ4Compressor lz4Compressor = lz4Factory.fastCompressor();
    private static final LZ4FastDecompressor lz4Decompressor = lz4Factory.fastDecompressor();

    // 压缩阈值，小于此大小的消息不进行压缩
    private static final int COMPRESSION_THRESHOLD = 1024; // 1KB

    /**
     * 压缩消息数据
     * @param data 原始数据
     * @param compressionType 压缩类型
     * @return 压缩后的数据（包含压缩类型标识和原始长度）
     */
    public static byte[] compress(byte[] data, CompressionType compressionType) {
        if (data == null || data.length == 0) {
            return data;
        }

        // 小于阈值的消息不压缩
        if (data.length < COMPRESSION_THRESHOLD || compressionType == CompressionType.NONE) {
            return wrapUncompressed(data);
        }

        try {
            switch (compressionType) {
                case LZ4:
                    return compressWithLZ4(data);
                case SNAPPY:
                    return compressWithSnappy(data);
                default:
                    return wrapUncompressed(data);
            }
        } catch (Exception e) {
            logger.warn("Failed to compress message with {}, fallback to uncompressed", compressionType, e);
            return wrapUncompressed(data);
        }
    }

    /**
     * 解压缩消息数据
     * @param compressedData 压缩后的数据
     * @return 原始数据
     */
    public static byte[] decompress(byte[] compressedData) {
        if (compressedData == null || compressedData.length < 5) {
            return compressedData;
        }

        ByteBuffer buffer = ByteBuffer.wrap(compressedData);
        byte compressionTypeCode = buffer.get();
        int originalLength = buffer.getInt();

        CompressionType compressionType = CompressionType.fromCode(compressionTypeCode);

        byte[] compressedPayload = new byte[buffer.remaining()];
        buffer.get(compressedPayload);

        try {
            switch (compressionType) {
                case NONE:
                    return compressedPayload;
                case LZ4:
                    return decompressWithLZ4(compressedPayload, originalLength);
                case SNAPPY:
                    return decompressWithSnappy(compressedPayload);
                default:
                    logger.warn("Unknown compression type: {}", compressionTypeCode);
                    return compressedPayload;
            }
        } catch (Exception e) {
            logger.error("Failed to decompress message with type {}", compressionType, e);
            throw new RuntimeException("Message decompression failed", e);
        }
    }

    /**
     * 检查数据是否被压缩
     */
    public static boolean isCompressed(byte[] data) {
        if (data == null || data.length < 5) {
            return false;
        }
        byte compressionTypeCode = data[0];
        CompressionType type = CompressionType.fromCode(compressionTypeCode);
        return type != CompressionType.NONE;
    }

    /**
     * 获取压缩类型
     */
    public static CompressionType getCompressionType(byte[] data) {
        if (data == null || data.length < 1) {
            return CompressionType.NONE;
        }
        return CompressionType.fromCode(data[0]);
    }

    /**
     * 计算压缩率
     */
    public static double getCompressionRatio(int originalSize, int compressedSize) {
        if (originalSize == 0) {
            return 0.0;
        }
        return 1.0 - ((double) compressedSize / originalSize);
    }

    private static byte[] wrapUncompressed(byte[] data) {
        ByteBuffer buffer = ByteBuffer.allocate(5 + data.length);
        buffer.put(CompressionType.NONE.getCode());
        buffer.putInt(data.length);
        buffer.put(data);
        return buffer.array();
    }

    private static byte[] compressWithLZ4(byte[] data) {
        int maxCompressedLength = lz4Compressor.maxCompressedLength(data.length);
        byte[] compressed = new byte[maxCompressedLength];
        int compressedLength = lz4Compressor.compress(data, 0, data.length, compressed, 0);

        // 如果压缩后的大小没有明显减少，则不使用压缩
        if (compressedLength >= data.length * 0.9) {
            return wrapUncompressed(data);
        }

        ByteBuffer buffer = ByteBuffer.allocate(5 + compressedLength);
        buffer.put(CompressionType.LZ4.getCode());
        buffer.putInt(data.length);
        buffer.put(compressed, 0, compressedLength);

        logger.debug("LZ4 compressed {} bytes to {} bytes, ratio: {:.2f}%",
                data.length, compressedLength, getCompressionRatio(data.length, compressedLength) * 100);

        return buffer.array();
    }

    private static byte[] compressWithSnappy(byte[] data) throws IOException {
        byte[] compressed = Snappy.compress(data);

        // 如果压缩后的大小没有明显减少，则不使用压缩
        if (compressed.length >= data.length * 0.9) {
            return wrapUncompressed(data);
        }

        ByteBuffer buffer = ByteBuffer.allocate(5 + compressed.length);
        buffer.put(CompressionType.SNAPPY.getCode());
        buffer.putInt(data.length);
        buffer.put(compressed);

        logger.debug("Snappy compressed {} bytes to {} bytes, ratio: {:.2f}%",
                data.length, compressed.length, getCompressionRatio(data.length, compressed.length) * 100);

        return buffer.array();
    }

    private static byte[] decompressWithLZ4(byte[] compressedData, int originalLength) {
        byte[] decompressed = new byte[originalLength];
        lz4Decompressor.decompress(compressedData, 0, decompressed, 0, originalLength);
        return decompressed;
    }

    private static byte[] decompressWithSnappy(byte[] compressedData) throws IOException {
        return Snappy.uncompress(compressedData);
    }
}