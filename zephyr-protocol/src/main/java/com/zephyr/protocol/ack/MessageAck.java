package com.zephyr.protocol.ack;

import java.io.Serializable;

/**
 * Message acknowledgment information
 */
public class MessageAck implements Serializable {

    private static final long serialVersionUID = 1L;

    private String msgId;
    private String topic;
    private int queueId;
    private long queueOffset;
    private String consumerGroup;
    private String consumerId;
    private long ackTime;
    private AckStatus ackStatus;

    public MessageAck() {
        this.ackTime = System.currentTimeMillis();
    }

    public MessageAck(String msgId, String topic, int queueId, long queueOffset,
                     String consumerGroup, String consumerId, AckStatus ackStatus) {
        this.msgId = msgId;
        this.topic = topic;
        this.queueId = queueId;
        this.queueOffset = queueOffset;
        this.consumerGroup = consumerGroup;
        this.consumerId = consumerId;
        this.ackStatus = ackStatus;
        this.ackTime = System.currentTimeMillis();
    }

    // Getters and setters
    public String getMsgId() { return msgId; }
    public void setMsgId(String msgId) { this.msgId = msgId; }
    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public int getQueueId() { return queueId; }
    public void setQueueId(int queueId) { this.queueId = queueId; }
    public long getQueueOffset() { return queueOffset; }
    public void setQueueOffset(long queueOffset) { this.queueOffset = queueOffset; }
    public String getConsumerGroup() { return consumerGroup; }
    public void setConsumerGroup(String consumerGroup) { this.consumerGroup = consumerGroup; }
    public String getConsumerId() { return consumerId; }
    public void setConsumerId(String consumerId) { this.consumerId = consumerId; }
    public long getAckTime() { return ackTime; }
    public void setAckTime(long ackTime) { this.ackTime = ackTime; }
    public AckStatus getAckStatus() { return ackStatus; }
    public void setAckStatus(AckStatus ackStatus) { this.ackStatus = ackStatus; }

    public enum AckStatus {
        /**
         * Message consumed successfully
         */
        SUCCESS,

        /**
         * Message consumption failed, retry later
         */
        RETRY_LATER,

        /**
         * Message consumption failed, send to dead letter queue
         */
        DEAD_LETTER
    }

    @Override
    public String toString() {
        return "MessageAck{" +
                "msgId='" + msgId + '\'' +
                ", topic='" + topic + '\'' +
                ", queueId=" + queueId +
                ", queueOffset=" + queueOffset +
                ", consumerGroup='" + consumerGroup + '\'' +
                ", consumerId='" + consumerId + '\'' +
                ", ackTime=" + ackTime +
                ", ackStatus=" + ackStatus +
                '}';
    }
}