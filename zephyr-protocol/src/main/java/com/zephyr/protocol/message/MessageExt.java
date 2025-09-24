package com.zephyr.protocol.message;

import java.nio.ByteBuffer;

public class MessageExt extends Message {

    private static final long serialVersionUID = 1L;

    private String brokerName;
    private int queueId;
    private long storeSize;
    private long queueOffset;
    private int sysFlag;
    private long bornTimestamp;
    private ByteBuffer bornHost;
    private long storeTimestamp;
    private ByteBuffer storeHost;
    private String msgId;
    private long commitLogOffset;
    private int bodyCRC;
    private int reconsumeTimes;
    private long preparedTransactionOffset;
    private MessageQueue messageQueue;

    public MessageExt() {
        super();
    }

    public MessageExt(int queueId, long bornTimestamp, ByteBuffer bornHost, long storeTimestamp, ByteBuffer storeHost, String msgId) {
        this.queueId = queueId;
        this.bornTimestamp = bornTimestamp;
        this.bornHost = bornHost;
        this.storeTimestamp = storeTimestamp;
        this.storeHost = storeHost;
        this.msgId = msgId;
    }

    public static ByteBuffer socketAddress2ByteBuffer(String addr) {
        String[] s = addr.split(":");
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        String[] ip = s[0].split("\\.");
        byteBuffer.put((byte) Integer.parseInt(ip[0]));
        byteBuffer.put((byte) Integer.parseInt(ip[1]));
        byteBuffer.put((byte) Integer.parseInt(ip[2]));
        byteBuffer.put((byte) Integer.parseInt(ip[3]));
        byteBuffer.putInt(Integer.parseInt(s[1]));
        return byteBuffer;
    }

    public static String parseChannelRemoteAddr(ByteBuffer byteBuffer) {
        if (byteBuffer.capacity() < 8) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            int value = byteBuffer.get(i) & 0xFF;
            sb.append(value);
            if (i < 3) {
                sb.append(".");
            }
        }
        sb.append(":");
        sb.append(byteBuffer.getInt(4));
        return sb.toString();
    }

    // Getters and Setters
    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public long getStoreSize() {
        return storeSize;
    }

    public void setStoreSize(long storeSize) {
        this.storeSize = storeSize;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public int getSysFlag() {
        return sysFlag;
    }

    public void setSysFlag(int sysFlag) {
        this.sysFlag = sysFlag;
    }

    public long getBornTimestamp() {
        return bornTimestamp;
    }

    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    public ByteBuffer getBornHost() {
        return bornHost;
    }

    public void setBornHost(ByteBuffer bornHost) {
        this.bornHost = bornHost;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public ByteBuffer getStoreHost() {
        return storeHost;
    }

    public void setStoreHost(ByteBuffer storeHost) {
        this.storeHost = storeHost;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public void setCommitLogOffset(long commitLogOffset) {
        this.commitLogOffset = commitLogOffset;
    }

    public int getBodyCRC() {
        return bodyCRC;
    }

    public void setBodyCRC(int bodyCRC) {
        this.bodyCRC = bodyCRC;
    }

    public int getReconsumeTimes() {
        return reconsumeTimes;
    }

    public void setReconsumeTimes(int reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
    }

    public long getPreparedTransactionOffset() {
        return preparedTransactionOffset;
    }

    public void setPreparedTransactionOffset(long preparedTransactionOffset) {
        this.preparedTransactionOffset = preparedTransactionOffset;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    @Override
    public String toString() {
        return "MessageExt{" +
                "brokerName='" + brokerName + '\'' +
                ", queueId=" + queueId +
                ", storeSize=" + storeSize +
                ", queueOffset=" + queueOffset +
                ", sysFlag=" + sysFlag +
                ", bornTimestamp=" + bornTimestamp +
                ", storeTimestamp=" + storeTimestamp +
                ", msgId='" + msgId + '\'' +
                ", commitLogOffset=" + commitLogOffset +
                ", bodyCRC=" + bodyCRC +
                ", reconsumeTimes=" + reconsumeTimes +
                ", preparedTransactionOffset=" + preparedTransactionOffset +
                "} " + super.toString();
    }
}