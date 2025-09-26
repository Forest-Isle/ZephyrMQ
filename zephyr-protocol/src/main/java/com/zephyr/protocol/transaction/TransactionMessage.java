package com.zephyr.protocol.transaction;

import java.io.Serializable;

/**
 * Transaction message information
 */
public class TransactionMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private String transactionId;
    private String msgId;
    private String topic;
    private String producerGroup;
    private String producerId;
    private TransactionState state;
    private long prepareTime;
    private long commitTime;
    private long timeoutTime;
    private int checkTimes = 0;
    private String checkResult;

    public TransactionMessage() {
    }

    public TransactionMessage(String transactionId, String msgId, String topic,
                            String producerGroup, String producerId) {
        this.transactionId = transactionId;
        this.msgId = msgId;
        this.topic = topic;
        this.producerGroup = producerGroup;
        this.producerId = producerId;
        this.state = TransactionState.PREPARE;
        this.prepareTime = System.currentTimeMillis();
        this.timeoutTime = prepareTime + 60000; // Default 1 minute timeout
    }

    // Getters and setters
    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    public String getMsgId() { return msgId; }
    public void setMsgId(String msgId) { this.msgId = msgId; }
    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public String getProducerGroup() { return producerGroup; }
    public void setProducerGroup(String producerGroup) { this.producerGroup = producerGroup; }
    public String getProducerId() { return producerId; }
    public void setProducerId(String producerId) { this.producerId = producerId; }
    public TransactionState getState() { return state; }
    public void setState(TransactionState state) { this.state = state; }
    public long getPrepareTime() { return prepareTime; }
    public void setPrepareTime(long prepareTime) { this.prepareTime = prepareTime; }
    public long getCommitTime() { return commitTime; }
    public void setCommitTime(long commitTime) { this.commitTime = commitTime; }
    public long getTimeoutTime() { return timeoutTime; }
    public void setTimeoutTime(long timeoutTime) { this.timeoutTime = timeoutTime; }
    public int getCheckTimes() { return checkTimes; }
    public void setCheckTimes(int checkTimes) { this.checkTimes = checkTimes; }
    public String getCheckResult() { return checkResult; }
    public void setCheckResult(String checkResult) { this.checkResult = checkResult; }

    /**
     * Check if transaction is expired
     *
     * @return true if expired
     */
    public boolean isExpired() {
        return System.currentTimeMillis() > timeoutTime;
    }

    /**
     * Increment check times
     */
    public void incrementCheckTimes() {
        this.checkTimes++;
    }

    @Override
    public String toString() {
        return "TransactionMessage{" +
                "transactionId='" + transactionId + '\'' +
                ", msgId='" + msgId + '\'' +
                ", topic='" + topic + '\'' +
                ", producerGroup='" + producerGroup + '\'' +
                ", producerId='" + producerId + '\'' +
                ", state=" + state +
                ", prepareTime=" + prepareTime +
                ", commitTime=" + commitTime +
                ", timeoutTime=" + timeoutTime +
                ", checkTimes=" + checkTimes +
                '}';
    }
}