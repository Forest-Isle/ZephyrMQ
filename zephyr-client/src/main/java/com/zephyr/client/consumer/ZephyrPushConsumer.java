package com.zephyr.client.consumer;

import com.zephyr.protocol.message.MessageQueue;

import java.util.List;

public interface ZephyrPushConsumer {

    void start() throws Exception;

    void shutdown();

    void registerMessageListener(MessageListener messageListener);

    void subscribe(String topic, String subExpression) throws Exception;

    void unsubscribe(String topic);

    void updateCorePoolSize(int corePoolSize);

    void suspend();

    void resume();
}