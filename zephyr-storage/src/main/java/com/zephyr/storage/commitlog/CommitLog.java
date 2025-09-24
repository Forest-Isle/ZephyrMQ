package com.zephyr.storage.commitlog;

import com.zephyr.common.config.BrokerConfig;
import com.zephyr.protocol.message.MessageExt;
import com.zephyr.storage.compression.MessageCompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * CommitLog - 消息存储的核心实现
 * 基于文件映射实现高性能的消息存储
 */
public class CommitLog {

    private static final Logger logger = LoggerFactory.getLogger(CommitLog.class);

    // 消息魔数，用于验证消息格式
    public static final int MESSAGE_MAGIC_CODE = 0xDAA320A7;
    // 空白魔数，用于标识文件末尾
    public static final int BLANK_MAGIC_CODE = 0xBB10F7B4;

    private final MappedFileQueue mappedFileQueue;
    private final BrokerConfig brokerConfig;
    private final AtomicLong confirmOffset = new AtomicLong(-1);

    public CommitLog(final BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
        this.mappedFileQueue = new MappedFileQueue(brokerConfig.getStorePathCommitLog(),
                brokerConfig.getMapedFileSizeCommitLog());
    }

    public void load() {
        this.mappedFileQueue.load();
    }

    public void start() {
        logger.info("CommitLog service started");
    }

    public void shutdown() {
        this.mappedFileQueue.shutdown(1000 * 3);
        logger.info("CommitLog service shutdown");
    }

    /**
     * 存储消息到CommitLog
     */
    public PutMessageResult putMessage(final MessageExt msg) {
        // 设置存储时间
        msg.setStoreTimestamp(System.currentTimeMillis());

        // 序列化消息
        byte[] messageBytes = encodeMessage(msg);
        if (messageBytes == null) {
            logger.error("Encode message failed, msgId: {}", msg.getMsgId());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        // 写入文件
        MappedFile.AppendMessageResult result = this.mappedFileQueue.appendMessage(messageBytes);

        switch (result.getStatus()) {
            case PUT_OK:
                break;
            case END_OF_FILE:
                // 文件已满，创建新文件重试
                MappedFile.AppendMessageResult retryResult = this.mappedFileQueue.appendMessage(messageBytes);
                if (retryResult.getStatus() != MappedFile.AppendMessageStatus.PUT_OK) {
                    logger.error("Failed to append message after retry, msgId: {}", msg.getMsgId());
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
                }
                result = retryResult;
                break;
            default:
                logger.error("Failed to append message to commitlog, msgId: {}, status: {}",
                        msg.getMsgId(), result.getStatus());
                return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // 更新统计信息
        if (brokerConfig.isTransactionEnable()) {
            // TODO: 处理事务消息
        }

        return putMessageResult;
    }

    /**
     * 根据物理偏移量获取消息
     */
    public MessageExt lookMessageByOffset(long commitLogOffset) {
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(commitLogOffset);
        if (mappedFile != null) {
            int pos = (int) (commitLogOffset % this.brokerConfig.getMapedFileSizeCommitLog());
            return lookMessageByOffset(mappedFile, pos);
        }

        return null;
    }

    /**
     * 从指定文件位置读取消息
     */
    private MessageExt lookMessageByOffset(MappedFile mappedFile, int pos) {
        ByteBuffer buffer = mappedFile.selectMappedBuffer(pos, 4);
        if (buffer == null) {
            return null;
        }

        // 读取消息长度
        int totalSize = buffer.getInt();
        if (totalSize <= 0 || totalSize > brokerConfig.getMaxMessageSize()) {
            logger.warn("Invalid message size: {}, pos: {}", totalSize, pos);
            return null;
        }

        // 读取完整消息
        ByteBuffer messageBuffer = mappedFile.selectMappedBuffer(pos, totalSize);
        if (messageBuffer == null) {
            return null;
        }

        return decodeMessage(messageBuffer);
    }

    /**
     * 序列化消息
     */
    private byte[] encodeMessage(MessageExt messageExt) {
        try {
            // 计算消息体长度
            byte[] topicBytes = messageExt.getTopic().getBytes("UTF-8");
            byte[] tagsBytes = messageExt.getTags() != null ? messageExt.getTags().getBytes("UTF-8") : new byte[0];
            byte[] keysBytes = messageExt.getKeys() != null ? messageExt.getKeys().getBytes("UTF-8") : new byte[0];
            byte[] bodyBytes = messageExt.getBody();

            // 应用压缩（如果启用）
            byte[] finalBodyBytes = bodyBytes;
            boolean isCompressed = false;
            if (brokerConfig.isCompressionEnable() && bodyBytes.length >= brokerConfig.getCompressionThreshold()) {
                MessageCompressor.CompressionType compressionType = getCompressionType();
                byte[] compressedBody = MessageCompressor.compress(bodyBytes, compressionType);

                // 检查压缩效果
                double compressionRatio = MessageCompressor.getCompressionRatio(bodyBytes.length, compressedBody.length);
                if (compressionRatio >= brokerConfig.getCompressionRatioThreshold()) {
                    finalBodyBytes = compressedBody;
                    isCompressed = true;
                    logger.debug("Message compressed: original={} bytes, compressed={} bytes, ratio={:.2f}%",
                            bodyBytes.length, compressedBody.length, compressionRatio * 100);
                }
            }

            // 计算总长度
            int totalSize = 4 + // totalSize
                    4 + // magicCode
                    4 + // bodyCRC
                    4 + // queueId
                    4 + // flag
                    8 + // queueOffset
                    8 + // physicalOffset
                    4 + // sysFlag
                    8 + // bornTimestamp
                    8 + // bornHost (simplified to 8 bytes)
                    8 + // storeTimestamp
                    8 + // storeHost (simplified to 8 bytes)
                    4 + // reconsumeTimes
                    8 + // preparedTransactionOffset
                    1 + // compressionFlag
                    4 + topicBytes.length + // topic
                    1 + tagsBytes.length + // tags
                    2 + keysBytes.length + // keys
                    4 + finalBodyBytes.length; // body

            ByteBuffer buffer = ByteBuffer.allocate(totalSize);

            // 写入消息头
            buffer.putInt(totalSize);
            buffer.putInt(MESSAGE_MAGIC_CODE);
            buffer.putInt(0); // bodyCRC
            buffer.putInt(messageExt.getQueueId());
            buffer.putInt(messageExt.getFlag());
            buffer.putLong(messageExt.getQueueOffset());
            buffer.putLong(messageExt.getCommitLogOffset());
            buffer.putInt(messageExt.getSysFlag());
            buffer.putLong(messageExt.getBornTimestamp());
            buffer.putLong(0); // bornHost simplified
            buffer.putLong(messageExt.getStoreTimestamp());
            buffer.putLong(0); // storeHost simplified
            buffer.putInt(messageExt.getReconsumeTimes());
            buffer.putLong(messageExt.getPreparedTransactionOffset());

            // 写入压缩标志
            buffer.put(isCompressed ? (byte) 1 : (byte) 0);

            // 写入topic
            buffer.putInt(topicBytes.length);
            buffer.put(topicBytes);

            // 写入tags
            buffer.put((byte) tagsBytes.length);
            buffer.put(tagsBytes);

            // 写入keys
            buffer.putShort((short) keysBytes.length);
            buffer.put(keysBytes);

            // 写入body
            buffer.putInt(finalBodyBytes.length);
            buffer.put(finalBodyBytes);

            return buffer.array();

        } catch (Exception e) {
            logger.error("Failed to encode message", e);
            return null;
        }
    }

    /**
     * 反序列化消息
     */
    private MessageExt decodeMessage(ByteBuffer buffer) {
        try {
            MessageExt messageExt = new MessageExt();

            // 读取消息头
            int totalSize = buffer.getInt();
            int magicCode = buffer.getInt();

            if (magicCode != MESSAGE_MAGIC_CODE) {
                logger.warn("Invalid magic code: {}", magicCode);
                return null;
            }

            int bodyCRC = buffer.getInt();
            int queueId = buffer.getInt();
            int flag = buffer.getInt();
            long queueOffset = buffer.getLong();
            long physicalOffset = buffer.getLong();
            int sysFlag = buffer.getInt();
            long bornTimestamp = buffer.getLong();
            long bornHost = buffer.getLong();
            long storeTimestamp = buffer.getLong();
            long storeHost = buffer.getLong();
            int reconsumeTimes = buffer.getInt();
            long preparedTransactionOffset = buffer.getLong();

            // 读取压缩标志
            boolean isCompressed = buffer.get() == 1;

            // 读取topic
            int topicLen = buffer.getInt();
            byte[] topicBytes = new byte[topicLen];
            buffer.get(topicBytes);
            String topic = new String(topicBytes, "UTF-8");

            // 读取tags
            int tagsLen = buffer.get() & 0xFF;
            byte[] tagsBytes = new byte[tagsLen];
            buffer.get(tagsBytes);
            String tags = tagsLen > 0 ? new String(tagsBytes, "UTF-8") : null;

            // 读取keys
            int keysLen = buffer.getShort() & 0xFFFF;
            byte[] keysBytes = new byte[keysLen];
            buffer.get(keysBytes);
            String keys = keysLen > 0 ? new String(keysBytes, "UTF-8") : null;

            // 读取body
            int bodyLen = buffer.getInt();
            byte[] bodyBytes = new byte[bodyLen];
            buffer.get(bodyBytes);

            // 解压缩body（如果需要）
            byte[] finalBodyBytes = bodyBytes;
            if (isCompressed) {
                try {
                    finalBodyBytes = MessageCompressor.decompress(bodyBytes);
                    logger.debug("Message decompressed: compressed={} bytes, original={} bytes",
                            bodyBytes.length, finalBodyBytes.length);
                } catch (Exception e) {
                    logger.error("Failed to decompress message body", e);
                    throw new RuntimeException("Message decompression failed", e);
                }
            }

            // 设置消息属性
            messageExt.setTopic(topic);
            messageExt.setTags(tags);
            messageExt.setKeys(keys);
            messageExt.setBody(finalBodyBytes);
            messageExt.setQueueId(queueId);
            messageExt.setFlag(flag);
            messageExt.setQueueOffset(queueOffset);
            messageExt.setCommitLogOffset(physicalOffset);
            messageExt.setSysFlag(sysFlag);
            messageExt.setBornTimestamp(bornTimestamp);
            messageExt.setStoreTimestamp(storeTimestamp);
            messageExt.setReconsumeTimes(reconsumeTimes);
            messageExt.setPreparedTransactionOffset(preparedTransactionOffset);
            messageExt.setBodyCRC(bodyCRC);

            return messageExt;

        } catch (Exception e) {
            logger.error("Failed to decode message", e);
            return null;
        }
    }

    /**
     * 刷盘操作
     */
    public void flush() {
        this.mappedFileQueue.flush(0);
    }

    /**
     * 获取最大物理偏移量
     */
    public long getMaxOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    /**
     * 获取最小物理偏移量
     */
    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset();
        }
        return -1;
    }

    /**
     * 获取压缩类型
     */
    private MessageCompressor.CompressionType getCompressionType() {
        String compressionTypeStr = brokerConfig.getCompressionType().toUpperCase();
        try {
            return MessageCompressor.CompressionType.valueOf(compressionTypeStr);
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid compression type: {}, fallback to NONE", compressionTypeStr);
            return MessageCompressor.CompressionType.NONE;
        }
    }

    // 消息存储结果
    public static class PutMessageResult {
        private final PutMessageStatus putMessageStatus;
        private final MappedFile.AppendMessageResult appendMessageResult;

        public PutMessageResult(PutMessageStatus putMessageStatus, MappedFile.AppendMessageResult appendMessageResult) {
            this.putMessageStatus = putMessageStatus;
            this.appendMessageResult = appendMessageResult;
        }

        public PutMessageStatus getPutMessageStatus() {
            return putMessageStatus;
        }

        public MappedFile.AppendMessageResult getAppendMessageResult() {
            return appendMessageResult;
        }
    }

    // 消息存储状态
    public enum PutMessageStatus {
        PUT_OK,
        FLUSH_DISK_TIMEOUT,
        FLUSH_SLAVE_TIMEOUT,
        SLAVE_NOT_AVAILABLE,
        SERVICE_NOT_AVAILABLE,
        CREATE_MAPEDFILE_FAILED,
        MESSAGE_ILLEGAL,
        PROPERTIES_SIZE_EXCEEDED,
        UNKNOWN_ERROR
    }
}