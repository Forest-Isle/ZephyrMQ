package com.zephyr.protocol.message;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Message implements Serializable {

    private static final long serialVersionUID = 1L;

    private String topic;
    private int flag;
    private String tags;
    private String keys;
    private int delayTimeLevel;
    private boolean waitStoreMsgOK = true;
    private byte[] body;
    private Map<String, String> properties;

    public Message() {
    }

    public Message(String topic, byte[] body) {
        this(topic, "", "", 0, body, true);
    }

    public Message(String topic, String tags, byte[] body) {
        this(topic, tags, "", 0, body, true);
    }

    public Message(String topic, String tags, String keys, byte[] body) {
        this(topic, tags, keys, 0, body, true);
    }

    public Message(String topic, String tags, String keys, int flag, byte[] body, boolean waitStoreMsgOK) {
        this.topic = topic;
        this.flag = flag;
        this.tags = tags;
        this.keys = keys;
        this.body = body;
        this.waitStoreMsgOK = waitStoreMsgOK;
    }

    public void putProperty(String name, String value) {
        if (null == this.properties) {
            this.properties = new HashMap<>();
        }
        this.properties.put(name, value);
    }

    public String getProperty(String name) {
        if (null == this.properties) {
            return null;
        }
        return this.properties.get(name);
    }

    public void clearProperty(String name) {
        if (null != this.properties) {
            this.properties.remove(name);
        }
    }

    public String getPropertiesString() {
        if (null == this.properties || this.properties.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : this.properties.entrySet()) {
            if (sb.length() > 0) {
                sb.append("&");
            }
            sb.append(entry.getKey()).append("=").append(entry.getValue());
        }
        return sb.toString();
    }

    // Getters and Setters
    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public String getKeys() {
        return keys;
    }

    public void setKeys(String keys) {
        this.keys = keys;
    }

    public int getDelayTimeLevel() {
        return delayTimeLevel;
    }

    public void setDelayTimeLevel(int delayTimeLevel) {
        this.delayTimeLevel = delayTimeLevel;
    }

    public boolean isWaitStoreMsgOK() {
        return waitStoreMsgOK;
    }

    public void setWaitStoreMsgOK(boolean waitStoreMsgOK) {
        this.waitStoreMsgOK = waitStoreMsgOK;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "Message{" +
                "topic='" + topic + '\'' +
                ", flag=" + flag +
                ", tags='" + tags + '\'' +
                ", keys='" + keys + '\'' +
                ", delayTimeLevel=" + delayTimeLevel +
                ", waitStoreMsgOK=" + waitStoreMsgOK +
                ", properties=" + properties +
                '}';
    }
}