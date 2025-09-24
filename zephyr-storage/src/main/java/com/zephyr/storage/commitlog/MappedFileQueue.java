package com.zephyr.storage.commitlog;

import com.zephyr.common.config.BrokerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * CommitLog文件队列管理
 * 管理多个CommitLog文件，实现文件滚动和清理
 */
public class MappedFileQueue {

    private static final Logger logger = LoggerFactory.getLogger(MappedFileQueue.class);

    private final String storePath;
    private final int mappedFileSize;
    private final BrokerConfig brokerConfig;
    private final FileRollingService fileRollingService;
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<>();
    private final AtomicLong flushedWhere = new AtomicLong(0);
    private final AtomicLong committedWhere = new AtomicLong(0);
    private volatile long storeTimestamp = 0;

    public MappedFileQueue(String storePath, int mappedFileSize) {
        this(storePath, mappedFileSize, new BrokerConfig());
    }

    public MappedFileQueue(String storePath, int mappedFileSize, BrokerConfig brokerConfig) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.brokerConfig = brokerConfig;
        this.fileRollingService = new FileRollingService(brokerConfig);
    }

    /**
     * 启动并加载现有文件
     */
    public void load() {
        File dir = new File(this.storePath);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        File[] files = dir.listFiles();
        if (files != null) {
            // 按文件名排序
            Arrays.sort(files);
            for (File file : files) {
                if (file.length() != this.mappedFileSize) {
                    logger.warn("File size mismatch: {}, expected: {}, actual: {}",
                            file.getName(), mappedFileSize, file.length());
                    continue;
                }

                try {
                    MappedFile mappedFile = new MappedFile(file.getAbsolutePath(), mappedFileSize);

                    // 恢复写入位置
                    mappedFile.setWrotePosition(mappedFileSize);
                    mappedFile.setFlushedPosition(mappedFileSize);
                    mappedFile.setCommittedPosition(mappedFileSize);

                    this.mappedFiles.add(mappedFile);
                } catch (IOException e) {
                    logger.error("Failed to load mapped file: " + file.getAbsolutePath(), e);
                }
            }
        }

        // 启动文件滚动服务
        fileRollingService.start();

        logger.info("Loaded {} mapped files from {}", mappedFiles.size(), storePath);
    }

    /**
     * 追加数据
     */
    public MappedFile.AppendMessageResult appendMessage(byte[] data) {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile == null) {
            mappedFile = getLastMappedFile(0);
        }

        // 检查文件是否需要滚动
        if (mappedFile != null && fileRollingService.shouldRollFile(mappedFile)) {
            FileRollingService.RollingTrigger trigger = fileRollingService.getRollingTrigger(mappedFile);
            logger.info("Rolling file {} due to {}", mappedFile.getFileName(), trigger);

            // 创建新文件
            long nextOffset = mappedFile.getFileFromOffset() + this.mappedFileSize;
            mappedFile = tryCreateMappedFile(nextOffset);
        }

        if (mappedFile != null) {
            MappedFile.AppendMessageResult result = mappedFile.appendMessage(data);

            // 异步清理过期文件
            if (brokerConfig.isAutoCleanExpiredFiles()) {
                cleanExpiredFilesAsync();
            }

            return result;
        }

        logger.error("Failed to get mapped file for append");
        return new MappedFile.AppendMessageResult(MappedFile.AppendMessageStatus.UNKNOWN_ERROR, 0, 0, null, 0, 0);
    }

    /**
     * 获取最后一个映射文件
     */
    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
            } catch (IndexOutOfBoundsException e) {
                // ignore
            }
        }

        return mappedFileLast;
    }

    /**
     * 获取最后一个映射文件，如果不存在或者已满则创建新的
     */
    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast == null) {
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }

        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        if (createOffset != -1 && needCreate) {
            return tryCreateMappedFile(createOffset);
        }

        return mappedFileLast;
    }

    /**
     * 尝试创建新的映射文件
     */
    private MappedFile tryCreateMappedFile(long createOffset) {
        String nextFilePath = this.storePath + File.separator + String.format("%020d", createOffset);
        String nextNextFilePath = this.storePath + File.separator + String.format("%020d", createOffset + this.mappedFileSize);

        MappedFile mappedFile = null;

        try {
            mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
        } catch (IOException e) {
            logger.error("Failed to create mapped file: " + nextFilePath, e);
            return null;
        }

        if (this.mappedFiles.size() == 0) {
            mappedFile.setAvailable(true);
        }

        this.mappedFiles.add(mappedFile);

        logger.info("Created new mapped file: {}", nextFilePath);
        return mappedFile;
    }

    /**
     * 根据偏移量查找映射文件
     */
    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            MappedFile firstMappedFile = this.getFirstMappedFile();
            MappedFile lastMappedFile = this.getLastMappedFile();
            if (firstMappedFile != null && lastMappedFile != null) {
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    logger.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}",
                            offset, firstMappedFile.getFileFromOffset(), lastMappedFile.getFileFromOffset());
                } else {
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }

                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
                            && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }

                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset()
                                && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }

                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            logger.error("Error finding mapped file by offset: " + offset, e);
        }

        return null;
    }

    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                // ignore
            }
        }

        return mappedFileFirst;
    }

    /**
     * 提交数据
     */
    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere.get());
        if (mappedFile != null) {
            int offset = mappedFile.commit(commitLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.committedWhere.get();
            this.committedWhere.set(where);
        }

        return result;
    }

    /**
     * 刷盘操作
     */
    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere.get());
        if (mappedFile != null) {
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            int offset = mappedFile.flush(flushLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.flushedWhere.get();
            this.flushedWhere.set(where);
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

    /**
     * 清理过期文件
     */
    public int deleteExpiredFileByTime(final long expiredTime, final int deleteFilesInterval,
                                       final long intervalForcibly, final boolean cleanImmediately) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return 0;

        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MappedFile> files = new ArrayList<>();
        if (null != mfs) {
            for (int i = 0; i < mfsLength; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];
                long liveMaxTimestamp = mappedFile.getStoreTimestamp() + expiredTime;
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    if (mappedFile.destroy(intervalForcibly)) {
                        files.add(mappedFile);
                        deleteCount++;

                        if (files.size() >= deleteFilesInterval) {
                            break;
                        }

                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    // avoid deleting files in the middle
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    private void deleteExpiredFile(List<MappedFile> files) {
        if (!files.isEmpty()) {
            try {
                for (MappedFile file : files) {
                    logger.info("Delete expired file: {}", file.getFileName());
                    boolean result = this.mappedFiles.remove(file);
                    logger.info("Delete expired file result: {}", result);
                }
            } catch (Exception e) {
                logger.error("deleteExpiredFile has exception.", e);
            }
        }
    }

    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;

        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    /**
     * 关闭队列
     */
    public void shutdown(final long intervalForcibly) {
        // 关闭文件滚动服务
        fileRollingService.shutdown();

        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    /**
     * 销毁队列
     */
    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere.set(0);

        // Delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    // Getters
    public long getFlushedWhere() {
        return flushedWhere.get();
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere.set(flushedWhere);
    }

    public long getCommittedWhere() {
        return committedWhere.get();
    }

    public void setCommittedWhere(long committedWhere) {
        this.committedWhere.set(committedWhere);
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    /**
     * 异步清理过期文件
     */
    private void cleanExpiredFilesAsync() {
        // 使用简单的线程池或者单独线程异步执行
        Thread cleanupThread = new Thread(() -> {
            try {
                fileRollingService.cleanExpiredFiles(this);
            } catch (Exception e) {
                logger.error("Error during async cleanup", e);
            }
        }, "FileCleanup");
        cleanupThread.setDaemon(true);
        cleanupThread.start();
    }

    /**
     * 手动清理过期文件
     */
    public int cleanExpiredFiles() {
        return fileRollingService.cleanExpiredFiles(this);
    }

    /**
     * 获取文件滚动统计信息
     */
    public FileRollingService.FileRollingStats getFileRollingStats() {
        return fileRollingService.getFileRollingStats(this);
    }

    /**
     * 获取文件滚动服务
     */
    public FileRollingService getFileRollingService() {
        return fileRollingService;
    }
}