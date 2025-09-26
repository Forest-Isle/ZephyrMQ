package com.zephyr.common.config;

public class BrokerConfig {

    private String brokerName = "DefaultBroker";
    private String brokerClusterName = "DefaultCluster";
    private long brokerId = 0;
    private int listenPort = 9876;
    private String namesrvAddr = "127.0.0.1:9877";

    // Storage configuration
    private String storePathRootDir = System.getProperty("user.home") + "/zephyr-store";
    private String storePathCommitLog = "";
    private String storePathConsumeQueue = "";
    private String storePathIndex = "";

    // Message store configuration
    private int mapedFileSizeCommitLog = 1024 * 1024 * 1024; // 1GB
    private int mapedFileSizeConsumeQueue = 300000 * 20; // 300K entries
    private boolean enableConsumeQueueExt = false;
    private int mapedFileSizeConsumeQueueExt = 48 * 1024 * 1024; // 48MB
    private int bitMapLengthConsumeQueueExt = 64;

    // Flush configuration
    private int flushIntervalCommitLog = 500; // 500ms
    private int flushCommitLogLeastPages = 4;
    private int flushConsumeQueueLeastPages = 2;
    private int flushCommitLogThoroughInterval = 10000; // 10s
    private int flushConsumeQueueThoroughInterval = 60000; // 60s

    // Disk space configuration
    private int diskMaxUsedSpaceRatio = 88; // 88%
    private boolean cleanFileForciblyEnable = true;
    private int cleanResourceInterval = 10000; // 10s
    private int deleteCommitLogFilesInterval = 100; // 100ms

    // Message configuration
    private int maxMessageSize = 1024 * 1024 * 4; // 4MB
    private boolean messageIndexEnable = true;
    private int maxHashSlotNum = 5000000;
    private int maxIndexNum = 5000000 * 4;

    // Transaction configuration
    private boolean transactionEnable = false;
    private int transactionTimeOut = 6000; // 6s
    private int transactionCheckMax = 15;
    private int transactionCheckInterval = 60000; // 60s

    // Compression configuration
    private boolean compressionEnable = true;
    private String compressionType = "LZ4"; // LZ4, SNAPPY, NONE
    private int compressionThreshold = 1024; // 1KB, messages smaller than this won't be compressed
    private double compressionRatioThreshold = 0.1; // 10%, minimum compression ratio to apply compression

    // Checksum configuration
    private boolean checksumVerificationEnable = true; // Enable CRC32 checksum verification
    private boolean checksumCalculationEnable = true; // Enable CRC32 checksum calculation during encoding
    private String checksumAlgorithm = "CRC32"; // CRC32, ADLER32

    // Integrity check configuration
    private boolean integrityCheckEnable = true; // Enable comprehensive integrity check on message storage
    private boolean integrityCheckOnRead = false; // Enable integrity check when reading messages (performance impact)

    // Sparse index configuration
    private boolean sparseIndexEnable = true; // Enable sparse index for query performance improvement
    private int sparseIndexInterval = 100; // Index interval: create one index entry per N messages
    private int maxSparseIndexEntries = 100000; // Maximum number of index entries to keep in memory

    // Async flush configuration
    private boolean asyncFlushEnable = true; // Enable async flush mechanism
    private String flushStrategy = "ASYNC_HYBRID"; // SYNC, ASYNC_BATCH, ASYNC_TIMER, ASYNC_HYBRID
    private int flushBatchSize = 4; // Flush batch size in pages (4KB each)
    private long flushIntervalMs = 500; // Flush interval in milliseconds
    private int flushThreadPoolSize = 2; // Flush thread pool size

    // File rolling configuration
    private boolean fileRollingEnable = true;
    private String fileRollingStrategy = "SIZE_TIME"; // SIZE, TIME, SIZE_TIME
    private long fileMaxSizeBytes = 1024 * 1024 * 1024; // 1GB, max file size before rolling
    private long fileMaxAgeMillis = 24 * 60 * 60 * 1000; // 24 hours, max file age before rolling
    private long fileRetentionHours = 72; // 72 hours, keep files for 3 days
    private int maxFileCount = 100; // Maximum number of files to keep
    private boolean autoCleanExpiredFiles = true; // Automatically clean expired files
    private int cleanExpiredFilesInterval = 10 * 60 * 1000; // 10 minutes, interval for cleaning expired files

    public BrokerConfig() {
        // Initialize derived paths if not explicitly set
        if (storePathCommitLog.isEmpty()) {
            storePathCommitLog = storePathRootDir + "/commitlog";
        }
        if (storePathConsumeQueue.isEmpty()) {
            storePathConsumeQueue = storePathRootDir + "/consumequeue";
        }
        if (storePathIndex.isEmpty()) {
            storePathIndex = storePathRootDir + "/index";
        }
    }

    // Getters and Setters
    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerClusterName() {
        return brokerClusterName;
    }

    public void setBrokerClusterName(String brokerClusterName) {
        this.brokerClusterName = brokerClusterName;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public String getNameServerAddr() {
        return namesrvAddr;
    }

    public String getStorePathRootDir() {
        return storePathRootDir;
    }

    public void setStorePathRootDir(String storePathRootDir) {
        this.storePathRootDir = storePathRootDir;
        // Update derived paths
        if (storePathCommitLog.isEmpty() || storePathCommitLog.equals(this.storePathRootDir + "/commitlog")) {
            storePathCommitLog = storePathRootDir + "/commitlog";
        }
        if (storePathConsumeQueue.isEmpty() || storePathConsumeQueue.equals(this.storePathRootDir + "/consumequeue")) {
            storePathConsumeQueue = storePathRootDir + "/consumequeue";
        }
        if (storePathIndex.isEmpty() || storePathIndex.equals(this.storePathRootDir + "/index")) {
            storePathIndex = storePathRootDir + "/index";
        }
    }

    public String getStorePathCommitLog() {
        return storePathCommitLog;
    }

    public void setStorePathCommitLog(String storePathCommitLog) {
        this.storePathCommitLog = storePathCommitLog;
    }

    public String getStorePathConsumeQueue() {
        return storePathConsumeQueue;
    }

    public void setStorePathConsumeQueue(String storePathConsumeQueue) {
        this.storePathConsumeQueue = storePathConsumeQueue;
    }

    public String getStorePathIndex() {
        return storePathIndex;
    }

    public void setStorePathIndex(String storePathIndex) {
        this.storePathIndex = storePathIndex;
    }

    public int getMapedFileSizeCommitLog() {
        return mapedFileSizeCommitLog;
    }

    public void setMapedFileSizeCommitLog(int mapedFileSizeCommitLog) {
        this.mapedFileSizeCommitLog = mapedFileSizeCommitLog;
    }

    public int getMapedFileSizeConsumeQueue() {
        return mapedFileSizeConsumeQueue;
    }

    public void setMapedFileSizeConsumeQueue(int mapedFileSizeConsumeQueue) {
        this.mapedFileSizeConsumeQueue = mapedFileSizeConsumeQueue;
    }

    public boolean isEnableConsumeQueueExt() {
        return enableConsumeQueueExt;
    }

    public void setEnableConsumeQueueExt(boolean enableConsumeQueueExt) {
        this.enableConsumeQueueExt = enableConsumeQueueExt;
    }

    public int getMapedFileSizeConsumeQueueExt() {
        return mapedFileSizeConsumeQueueExt;
    }

    public void setMapedFileSizeConsumeQueueExt(int mapedFileSizeConsumeQueueExt) {
        this.mapedFileSizeConsumeQueueExt = mapedFileSizeConsumeQueueExt;
    }

    public int getBitMapLengthConsumeQueueExt() {
        return bitMapLengthConsumeQueueExt;
    }

    public void setBitMapLengthConsumeQueueExt(int bitMapLengthConsumeQueueExt) {
        this.bitMapLengthConsumeQueueExt = bitMapLengthConsumeQueueExt;
    }

    public int getFlushIntervalCommitLog() {
        return flushIntervalCommitLog;
    }

    public void setFlushIntervalCommitLog(int flushIntervalCommitLog) {
        this.flushIntervalCommitLog = flushIntervalCommitLog;
    }

    public int getFlushCommitLogLeastPages() {
        return flushCommitLogLeastPages;
    }

    public void setFlushCommitLogLeastPages(int flushCommitLogLeastPages) {
        this.flushCommitLogLeastPages = flushCommitLogLeastPages;
    }

    public int getFlushConsumeQueueLeastPages() {
        return flushConsumeQueueLeastPages;
    }

    public void setFlushConsumeQueueLeastPages(int flushConsumeQueueLeastPages) {
        this.flushConsumeQueueLeastPages = flushConsumeQueueLeastPages;
    }

    public int getFlushCommitLogThoroughInterval() {
        return flushCommitLogThoroughInterval;
    }

    public void setFlushCommitLogThoroughInterval(int flushCommitLogThoroughInterval) {
        this.flushCommitLogThoroughInterval = flushCommitLogThoroughInterval;
    }

    public int getFlushConsumeQueueThoroughInterval() {
        return flushConsumeQueueThoroughInterval;
    }

    public void setFlushConsumeQueueThoroughInterval(int flushConsumeQueueThoroughInterval) {
        this.flushConsumeQueueThoroughInterval = flushConsumeQueueThoroughInterval;
    }

    public int getDiskMaxUsedSpaceRatio() {
        return diskMaxUsedSpaceRatio;
    }

    public void setDiskMaxUsedSpaceRatio(int diskMaxUsedSpaceRatio) {
        this.diskMaxUsedSpaceRatio = diskMaxUsedSpaceRatio;
    }

    public boolean isCleanFileForciblyEnable() {
        return cleanFileForciblyEnable;
    }

    public void setCleanFileForciblyEnable(boolean cleanFileForciblyEnable) {
        this.cleanFileForciblyEnable = cleanFileForciblyEnable;
    }

    public int getCleanResourceInterval() {
        return cleanResourceInterval;
    }

    public void setCleanResourceInterval(int cleanResourceInterval) {
        this.cleanResourceInterval = cleanResourceInterval;
    }

    public int getDeleteCommitLogFilesInterval() {
        return deleteCommitLogFilesInterval;
    }

    public void setDeleteCommitLogFilesInterval(int deleteCommitLogFilesInterval) {
        this.deleteCommitLogFilesInterval = deleteCommitLogFilesInterval;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public boolean isMessageIndexEnable() {
        return messageIndexEnable;
    }

    public void setMessageIndexEnable(boolean messageIndexEnable) {
        this.messageIndexEnable = messageIndexEnable;
    }

    public int getMaxHashSlotNum() {
        return maxHashSlotNum;
    }

    public void setMaxHashSlotNum(int maxHashSlotNum) {
        this.maxHashSlotNum = maxHashSlotNum;
    }

    public int getMaxIndexNum() {
        return maxIndexNum;
    }

    public void setMaxIndexNum(int maxIndexNum) {
        this.maxIndexNum = maxIndexNum;
    }

    public boolean isTransactionEnable() {
        return transactionEnable;
    }

    public void setTransactionEnable(boolean transactionEnable) {
        this.transactionEnable = transactionEnable;
    }

    public int getTransactionTimeOut() {
        return transactionTimeOut;
    }

    public void setTransactionTimeOut(int transactionTimeOut) {
        this.transactionTimeOut = transactionTimeOut;
    }

    public int getTransactionCheckMax() {
        return transactionCheckMax;
    }

    public void setTransactionCheckMax(int transactionCheckMax) {
        this.transactionCheckMax = transactionCheckMax;
    }

    public int getTransactionCheckInterval() {
        return transactionCheckInterval;
    }

    public void setTransactionCheckInterval(int transactionCheckInterval) {
        this.transactionCheckInterval = transactionCheckInterval;
    }

    public boolean isCompressionEnable() {
        return compressionEnable;
    }

    public void setCompressionEnable(boolean compressionEnable) {
        this.compressionEnable = compressionEnable;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(String compressionType) {
        this.compressionType = compressionType;
    }

    public int getCompressionThreshold() {
        return compressionThreshold;
    }

    public void setCompressionThreshold(int compressionThreshold) {
        this.compressionThreshold = compressionThreshold;
    }

    public double getCompressionRatioThreshold() {
        return compressionRatioThreshold;
    }

    public void setCompressionRatioThreshold(double compressionRatioThreshold) {
        this.compressionRatioThreshold = compressionRatioThreshold;
    }

    public boolean isFileRollingEnable() {
        return fileRollingEnable;
    }

    public void setFileRollingEnable(boolean fileRollingEnable) {
        this.fileRollingEnable = fileRollingEnable;
    }

    public String getFileRollingStrategy() {
        return fileRollingStrategy;
    }

    public void setFileRollingStrategy(String fileRollingStrategy) {
        this.fileRollingStrategy = fileRollingStrategy;
    }

    public long getFileMaxSizeBytes() {
        return fileMaxSizeBytes;
    }

    public void setFileMaxSizeBytes(long fileMaxSizeBytes) {
        this.fileMaxSizeBytes = fileMaxSizeBytes;
    }

    public long getFileMaxAgeMillis() {
        return fileMaxAgeMillis;
    }

    public void setFileMaxAgeMillis(long fileMaxAgeMillis) {
        this.fileMaxAgeMillis = fileMaxAgeMillis;
    }

    public long getFileRetentionHours() {
        return fileRetentionHours;
    }

    public void setFileRetentionHours(long fileRetentionHours) {
        this.fileRetentionHours = fileRetentionHours;
    }

    public int getMaxFileCount() {
        return maxFileCount;
    }

    public void setMaxFileCount(int maxFileCount) {
        this.maxFileCount = maxFileCount;
    }

    public boolean isAutoCleanExpiredFiles() {
        return autoCleanExpiredFiles;
    }

    public void setAutoCleanExpiredFiles(boolean autoCleanExpiredFiles) {
        this.autoCleanExpiredFiles = autoCleanExpiredFiles;
    }

    public int getCleanExpiredFilesInterval() {
        return cleanExpiredFilesInterval;
    }

    public void setCleanExpiredFilesInterval(int cleanExpiredFilesInterval) {
        this.cleanExpiredFilesInterval = cleanExpiredFilesInterval;
    }

    public boolean isChecksumVerificationEnable() {
        return checksumVerificationEnable;
    }

    public void setChecksumVerificationEnable(boolean checksumVerificationEnable) {
        this.checksumVerificationEnable = checksumVerificationEnable;
    }

    public boolean isChecksumCalculationEnable() {
        return checksumCalculationEnable;
    }

    public void setChecksumCalculationEnable(boolean checksumCalculationEnable) {
        this.checksumCalculationEnable = checksumCalculationEnable;
    }

    public String getChecksumAlgorithm() {
        return checksumAlgorithm;
    }

    public void setChecksumAlgorithm(String checksumAlgorithm) {
        this.checksumAlgorithm = checksumAlgorithm;
    }

    public boolean isIntegrityCheckEnable() {
        return integrityCheckEnable;
    }

    public void setIntegrityCheckEnable(boolean integrityCheckEnable) {
        this.integrityCheckEnable = integrityCheckEnable;
    }

    public boolean isIntegrityCheckOnRead() {
        return integrityCheckOnRead;
    }

    public void setIntegrityCheckOnRead(boolean integrityCheckOnRead) {
        this.integrityCheckOnRead = integrityCheckOnRead;
    }

    public boolean isSparseIndexEnable() {
        return sparseIndexEnable;
    }

    public void setSparseIndexEnable(boolean sparseIndexEnable) {
        this.sparseIndexEnable = sparseIndexEnable;
    }

    public int getSparseIndexInterval() {
        return sparseIndexInterval;
    }

    public void setSparseIndexInterval(int sparseIndexInterval) {
        this.sparseIndexInterval = sparseIndexInterval;
    }

    public int getMaxSparseIndexEntries() {
        return maxSparseIndexEntries;
    }

    public void setMaxSparseIndexEntries(int maxSparseIndexEntries) {
        this.maxSparseIndexEntries = maxSparseIndexEntries;
    }

    public boolean isAsyncFlushEnable() {
        return asyncFlushEnable;
    }

    public void setAsyncFlushEnable(boolean asyncFlushEnable) {
        this.asyncFlushEnable = asyncFlushEnable;
    }

    public String getFlushStrategy() {
        return flushStrategy;
    }

    public void setFlushStrategy(String flushStrategy) {
        this.flushStrategy = flushStrategy;
    }

    public int getFlushBatchSize() {
        return flushBatchSize;
    }

    public void setFlushBatchSize(int flushBatchSize) {
        this.flushBatchSize = flushBatchSize;
    }

    public long getFlushIntervalMs() {
        return flushIntervalMs;
    }

    public void setFlushIntervalMs(long flushIntervalMs) {
        this.flushIntervalMs = flushIntervalMs;
    }

    public int getFlushThreadPoolSize() {
        return flushThreadPoolSize;
    }

    public void setFlushThreadPoolSize(int flushThreadPoolSize) {
        this.flushThreadPoolSize = flushThreadPoolSize;
    }
}