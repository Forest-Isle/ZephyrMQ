package com.zephyr.client.producer;

import com.zephyr.protocol.message.SendResult;

public interface SendCallback {

    void onSuccess(SendResult sendResult);

    void onException(Throwable e);
}