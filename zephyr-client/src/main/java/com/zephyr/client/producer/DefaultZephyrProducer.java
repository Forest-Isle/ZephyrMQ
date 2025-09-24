package com.zephyr.client.producer;

import com.zephyr.protocol.message.Message;
import com.zephyr.protocol.message.MessageQueue;
import com.zephyr.protocol.message.SendResult;
import com.zephyr.protocol.message.SendStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.zephyr.common.constant.ZephyrConstants.*;

public class DefaultZephyrProducer implements ZephyrProducer {

    private static final Logger logger = LoggerFactory.getLogger(DefaultZephyrProducer.class);

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final ExecutorService asyncSenderExecutor;

    private String producerGroup;
    private String nameserverAddresses;
    private int sendMsgTimeout = DEFAULT_SEND_TIMEOUT_MILLIS;
    private int maxMessageSize = MAX_MESSAGE_SIZE;

    public DefaultZephyrProducer() {
        this("DEFAULT_PRODUCER");
    }

    public DefaultZephyrProducer(String producerGroup) {
        this.producerGroup = producerGroup;
        this.asyncSenderExecutor = Executors.newFixedThreadPool(4);
    }

    @Override
    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            logger.info("DefaultZephyrProducer[{}] start", producerGroup);
            // TODO: Initialize network client and connect to nameserver
        }
    }

    @Override
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            logger.info("DefaultZephyrProducer[{}] shutdown", producerGroup);
            asyncSenderExecutor.shutdown();
            // TODO: Cleanup network connections
        }
    }

    @Override
    public SendResult send(Message msg) throws Exception {
        return send(msg, sendMsgTimeout);
    }

    @Override
    public SendResult send(Message msg, long timeout) throws Exception {
        checkMessage(msg);

        // TODO: Implement actual send logic
        // For now, return a mock result
        MessageQueue mq = selectOneMessageQueue(msg.getTopic());
        return createMockSendResult(msg, mq);
    }

    @Override
    public SendResult send(Message msg, MessageQueue mq) throws Exception {
        return send(msg, mq, sendMsgTimeout);
    }

    @Override
    public SendResult send(Message msg, MessageQueue mq, long timeout) throws Exception {
        checkMessage(msg);
        checkMessageQueue(mq);

        // TODO: Implement actual send logic to specific queue
        return createMockSendResult(msg, mq);
    }

    @Override
    public void sendAsync(Message msg, SendCallback sendCallback) throws Exception {
        sendAsync(msg, sendCallback, sendMsgTimeout);
    }

    @Override
    public void sendAsync(Message msg, SendCallback sendCallback, long timeout) throws Exception {
        checkMessage(msg);

        MessageQueue mq = selectOneMessageQueue(msg.getTopic());
        sendAsync(msg, mq, sendCallback, timeout);
    }

    @Override
    public void sendAsync(Message msg, MessageQueue mq, SendCallback sendCallback) throws Exception {
        sendAsync(msg, mq, sendCallback, sendMsgTimeout);
    }

    @Override
    public void sendAsync(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout) throws Exception {
        checkMessage(msg);
        checkMessageQueue(mq);

        CompletableFuture.supplyAsync(() -> {
            try {
                // TODO: Implement actual async send logic
                return createMockSendResult(msg, mq);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, asyncSenderExecutor).whenComplete((result, throwable) -> {
            if (throwable != null) {
                sendCallback.onException(throwable);
            } else {
                sendCallback.onSuccess(result);
            }
        });
    }

    @Override
    public void sendOneway(Message msg) throws Exception {
        checkMessage(msg);

        MessageQueue mq = selectOneMessageQueue(msg.getTopic());
        sendOneway(msg, mq);
    }

    @Override
    public void sendOneway(Message msg, MessageQueue mq) throws Exception {
        checkMessage(msg);
        checkMessageQueue(mq);

        // TODO: Implement actual oneway send logic
        logger.info("Send oneway message to queue: {}", mq);
    }

    @Override
    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws Exception {
        // TODO: Fetch from nameserver
        // For now, return mock queues
        List<MessageQueue> queues = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            queues.add(new MessageQueue(topic, "broker-a", i));
        }
        return queues;
    }

    private void checkMessage(Message msg) throws Exception {
        if (msg == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        if (msg.getTopic() == null || msg.getTopic().trim().isEmpty()) {
            throw new IllegalArgumentException("Message topic cannot be null or empty");
        }
        if (msg.getBody() == null || msg.getBody().length == 0) {
            throw new IllegalArgumentException("Message body cannot be null or empty");
        }
        if (msg.getBody().length > maxMessageSize) {
            throw new IllegalArgumentException("Message body size exceeds limit: " + maxMessageSize);
        }
    }

    private void checkMessageQueue(MessageQueue mq) throws Exception {
        if (mq == null) {
            throw new IllegalArgumentException("MessageQueue cannot be null");
        }
        if (mq.getTopic() == null || mq.getTopic().trim().isEmpty()) {
            throw new IllegalArgumentException("MessageQueue topic cannot be null or empty");
        }
        if (mq.getBrokerName() == null || mq.getBrokerName().trim().isEmpty()) {
            throw new IllegalArgumentException("MessageQueue brokerName cannot be null or empty");
        }
    }

    private MessageQueue selectOneMessageQueue(String topic) throws Exception {
        List<MessageQueue> queues = fetchPublishMessageQueues(topic);
        if (queues.isEmpty()) {
            throw new Exception("No available message queue for topic: " + topic);
        }
        // Simple round-robin selection
        return queues.get((int) (System.currentTimeMillis() % queues.size()));
    }

    private SendResult createMockSendResult(Message msg, MessageQueue mq) {
        SendResult result = new SendResult();
        result.setSendStatus(SendStatus.SEND_OK);
        result.setMsgId(UUID.randomUUID().toString());
        result.setMessageQueue(mq);
        result.setQueueOffset(System.currentTimeMillis());
        return result;
    }

    // Getters and Setters
    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getNameserverAddresses() {
        return nameserverAddresses;
    }

    public void setNameserverAddresses(String nameserverAddresses) {
        this.nameserverAddresses = nameserverAddresses;
    }

    public int getSendMsgTimeout() {
        return sendMsgTimeout;
    }

    public void setSendMsgTimeout(int sendMsgTimeout) {
        this.sendMsgTimeout = sendMsgTimeout;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }
}