package com.zephyr.protocol.message;

public class MessageQueue {

    private String topic;
    private String brokerName;
    private int queueId;

    public MessageQueue() {
    }

    public MessageQueue(String topic, String brokerName, int queueId) {
        this.topic = topic;
        this.brokerName = brokerName;
        this.queueId = queueId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MessageQueue that = (MessageQueue) o;

        if (queueId != that.queueId) return false;
        if (topic != null ? !topic.equals(that.topic) : that.topic != null) return false;
        return brokerName != null ? brokerName.equals(that.brokerName) : that.brokerName == null;
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + (brokerName != null ? brokerName.hashCode() : 0);
        result = 31 * result + queueId;
        return result;
    }

    @Override
    public String toString() {
        return "MessageQueue{" +
                "topic='" + topic + '\'' +
                ", brokerName='" + brokerName + '\'' +
                ", queueId=" + queueId +
                '}';
    }
}