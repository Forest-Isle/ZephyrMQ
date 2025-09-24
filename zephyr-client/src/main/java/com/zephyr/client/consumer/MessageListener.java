package com.zephyr.client.consumer;

import com.zephyr.protocol.message.MessageExt;

import java.util.List;

public interface MessageListener {

    ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context);
}