package com.zephyr.broker.store;

import com.zephyr.protocol.message.MessageExt;

import java.util.List;

public interface MessageStore {

    void start() throws Exception;

    void shutdown();

    boolean putMessage(MessageExt messageExt);

    List<MessageExt> getMessage(String topic, int queueId, long offset, int maxMsgNums);

    MessageExt lookMessageByOffset(long commitLogOffset);

    MessageExt lookMessageByOffset(long commitLogOffset, int size);

    long getMaxPhyOffset();

    long getMinPhyOffset();

    long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset);

    long getOffsetInQueueByTime(String topic, int queueId, long timestamp);

    long getMaxOffsetInQueue(String topic, int queueId);

    long getMinOffsetInQueue(String topic, int queueId);

    long getEarliestMessageTime(String topic, int queueId);

    long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset);

    long getMessageTotalInQueue(String topic, int queueId);

    byte[] findMessageByOffset(long commitLogOffset, int size);

    boolean cleanExpiredConsumerQueue();

    boolean cleanExpiredCommitLog();

    void executeDeleteFilesManualy();

    void warmMappedFile(String type, String fileName);

    long getDispatchBehindBytes();

    long getTotalSize();

    boolean isOSPageCacheBusy();

    long getLockTimeMills();

    boolean isTransientStorePoolDeficient();

    double getDispatchMaxBuffer();

    boolean isSpaceRequired();

    long getCommitLogCommitLogSize();

    boolean isSyncDiskFlush();
}