package com.zephyr.protocol.message;

public class SendResult {

    private SendStatus sendStatus;
    private String msgId;
    private MessageQueue messageQueue;
    private long queueOffset;
    private String transactionId;
    private String offsetMsgId;
    private String regionId;
    private boolean traceOn = true;

    public SendResult() {
    }

    public SendResult(SendStatus sendStatus, String msgId, MessageQueue messageQueue, long queueOffset) {
        this.sendStatus = sendStatus;
        this.msgId = msgId;
        this.messageQueue = messageQueue;
        this.queueOffset = queueOffset;
    }

    public SendStatus getSendStatus() {
        return sendStatus;
    }

    public void setSendStatus(SendStatus sendStatus) {
        this.sendStatus = sendStatus;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getOffsetMsgId() {
        return offsetMsgId;
    }

    public void setOffsetMsgId(String offsetMsgId) {
        this.offsetMsgId = offsetMsgId;
    }

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public boolean isTraceOn() {
        return traceOn;
    }

    public void setTraceOn(boolean traceOn) {
        this.traceOn = traceOn;
    }

    @Override
    public String toString() {
        return "SendResult{" +
                "sendStatus=" + sendStatus +
                ", msgId='" + msgId + '\'' +
                ", messageQueue=" + messageQueue +
                ", queueOffset=" + queueOffset +
                ", transactionId='" + transactionId + '\'' +
                ", offsetMsgId='" + offsetMsgId + '\'' +
                ", regionId='" + regionId + '\'' +
                ", traceOn=" + traceOn +
                '}';
    }
}