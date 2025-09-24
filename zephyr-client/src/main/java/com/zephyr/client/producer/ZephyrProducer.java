package com.zephyr.client.producer;

import com.zephyr.protocol.message.Message;
import com.zephyr.protocol.message.MessageQueue;
import com.zephyr.protocol.message.SendResult;

import java.util.List;

public interface ZephyrProducer {

    void start() throws Exception;

    void shutdown();

    SendResult send(Message msg) throws Exception;

    SendResult send(Message msg, long timeout) throws Exception;

    SendResult send(Message msg, MessageQueue mq) throws Exception;

    SendResult send(Message msg, MessageQueue mq, long timeout) throws Exception;

    void sendAsync(Message msg, SendCallback sendCallback) throws Exception;

    void sendAsync(Message msg, SendCallback sendCallback, long timeout) throws Exception;

    void sendAsync(Message msg, MessageQueue mq, SendCallback sendCallback) throws Exception;

    void sendAsync(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout) throws Exception;

    void sendOneway(Message msg) throws Exception;

    void sendOneway(Message msg, MessageQueue mq) throws Exception;

    List<MessageQueue> fetchPublishMessageQueues(String topic) throws Exception;
}