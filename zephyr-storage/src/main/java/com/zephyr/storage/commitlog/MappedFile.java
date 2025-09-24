package com.zephyr.storage.commitlog;

import com.zephyr.common.config.BrokerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 内存映射文件实现
 * 基于MMap技术实现高性能的文件读写
 */
public class MappedFile {

    private static final Logger logger = LoggerFactory.getLogger(MappedFile.class);

    // 操作系统页面大小，通常为4K
    public static final int OS_PAGE_SIZE = 1024 * 4;

    private final String fileName;
    private final long fileFromOffset;
    private final int fileSize;
    private final File file;
    private final AtomicInteger wrotePosition = new AtomicInteger(0);
    private final AtomicInteger committedPosition = new AtomicInteger(0);
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    private final AtomicLong storeTimestamp = new AtomicLong(0);

    private MappedByteBuffer mappedByteBuffer;
    private FileChannel fileChannel;
    private volatile boolean available = true;

    public MappedFile(String fileName, int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());

        // 确保父目录存在
        File parent = this.file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }

        // 初始化文件和内存映射
        init();
    }

    private void init() throws IOException {
        boolean ok = false;
        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            ok = true;
            logger.info("Mapped file created: {}, size: {}", fileName, fileSize);
        } catch (IOException e) {
            logger.error("Failed to create mapped file: " + fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    /**
     * 追加数据到文件
     */
    public AppendMessageResult appendMessage(byte[] data) {
        int currentPos = this.wrotePosition.get();

        // 检查空间是否足够
        if (currentPos + data.length >= this.fileSize) {
            logger.warn("Mapped file is full: {}, currentPos: {}, dataLength: {}, fileSize: {}",
                    fileName, currentPos, data.length, fileSize);
            return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, 0, 0, null, System.currentTimeMillis(), 0);
        }

        try {
            // 写入数据
            ByteBuffer buffer = this.mappedByteBuffer.slice();
            buffer.position(currentPos);
            buffer.put(data);

            // 更新写入位置
            this.wrotePosition.addAndGet(data.length);
            this.storeTimestamp.set(System.currentTimeMillis());

            return new AppendMessageResult(AppendMessageStatus.PUT_OK, currentPos, data.length,
                    null, System.currentTimeMillis(), currentPos + data.length);

        } catch (Exception e) {
            logger.error("Error appending message to mapped file: " + fileName, e);
            return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR, 0, 0, null, 0, 0);
        }
    }

    /**
     * 从指定位置读取数据
     */
    public ByteBuffer selectMappedBuffer(int pos, int size) {
        if (pos >= 0 && pos + size <= this.wrotePosition.get()) {
            ByteBuffer buffer = this.mappedByteBuffer.slice();
            buffer.position(pos);
            ByteBuffer result = buffer.slice();
            result.limit(size);
            return result;
        }
        return null;
    }

    /**
     * 强制刷盘
     */
    public int flush(int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.mappedByteBuffer != null) {
                this.mappedByteBuffer.force();
            }

            int value = this.wrotePosition.get();
            this.flushedPosition.set(value);
            return value;
        }

        return this.getFlushedPosition();
    }

    /**
     * 提交数据
     */
    public int commit(int commitLeastPages) {
        if (this.isAbleToCommit(commitLeastPages)) {
            int value = this.wrotePosition.get();
            this.committedPosition.set(value);
            return value;
        }

        return this.getCommittedPosition();
    }

    private boolean isAbleToFlush(int flushLeastPages) {
        int flush = this.flushedPosition.get();
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    private boolean isAbleToCommit(int commitLeastPages) {
        int commit = this.committedPosition.get();
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (commit / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > commit;
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    public boolean isAvailable() {
        return available;
    }

    public void setAvailable(boolean available) {
        this.available = available;
    }

    /**
     * 销毁映射文件
     */
    public boolean destroy(long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                if (this.fileChannel != null) {
                    this.fileChannel.close();
                }

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                logger.info("Delete mapped file: {}, result: {}, time: {}ms",
                        fileName, result, System.currentTimeMillis() - beginTime);
                return result;
            } catch (Exception e) {
                logger.warn("Failed to delete mapped file: " + fileName, e);
            }
        }

        return false;
    }

    public void shutdown(long intervalForcibly) {
        if (this.available) {
            this.available = false;
        }
    }

    public boolean isCleanupOver() {
        return !this.available;
    }

    // Getters
    public String getFileName() {
        return fileName;
    }

    public long getFileFromOffset() {
        return fileFromOffset;
    }

    public int getFileSize() {
        return fileSize;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public int getCommittedPosition() {
        return committedPosition.get();
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public long getStoreTimestamp() {
        return storeTimestamp.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    // 消息追加结果
    public static class AppendMessageResult {
        private final AppendMessageStatus status;
        private final long wroteOffset;
        private final int wroteBytes;
        private final String msgId;
        private final long storeTimestamp;
        private final long logicsOffset;

        public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes,
                                   String msgId, long storeTimestamp, long logicsOffset) {
            this.status = status;
            this.wroteOffset = wroteOffset;
            this.wroteBytes = wroteBytes;
            this.msgId = msgId;
            this.storeTimestamp = storeTimestamp;
            this.logicsOffset = logicsOffset;
        }

        // Getters
        public AppendMessageStatus getStatus() { return status; }
        public long getWroteOffset() { return wroteOffset; }
        public int getWroteBytes() { return wroteBytes; }
        public String getMsgId() { return msgId; }
        public long getStoreTimestamp() { return storeTimestamp; }
        public long getLogicsOffset() { return logicsOffset; }
    }

    // 追加状态枚举
    public enum AppendMessageStatus {
        PUT_OK,
        END_OF_FILE,
        MESSAGE_SIZE_EXCEEDED,
        PROPERTIES_SIZE_EXCEEDED,
        UNKNOWN_ERROR
    }
}