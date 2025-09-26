package com.zephyr.client.producer;

import com.zephyr.client.route.TopicRouteInfoManager;
import com.zephyr.client.partition.MessageQueueSelector;
import com.zephyr.client.partition.SelectMessageQueueByRoundRobin;
import com.zephyr.protocol.message.Message;
import com.zephyr.protocol.message.MessageQueue;
import com.zephyr.protocol.message.SendResult;
import com.zephyr.protocol.message.SendStatus;
import com.zephyr.protocol.network.NettyRemotingClient;
import com.zephyr.protocol.network.RemotingCommand;
import com.zephyr.protocol.network.RequestCode;
import com.zephyr.protocol.network.ResponseCode;
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
    private final TopicRouteInfoManager routeInfoManager;
    private NettyRemotingClient remotingClient;

    private String producerGroup;
    private String nameserverAddresses;
    private int sendMsgTimeout = DEFAULT_SEND_TIMEOUT_MILLIS;
    private int maxMessageSize = MAX_MESSAGE_SIZE;

    // Default message queue selector
    private MessageQueueSelector defaultMessageQueueSelector = new SelectMessageQueueByRoundRobin();

    public DefaultZephyrProducer() {
        this("DEFAULT_PRODUCER");
    }

    public DefaultZephyrProducer(String producerGroup) {
        this.producerGroup = producerGroup;
        this.asyncSenderExecutor = Executors.newFixedThreadPool(4);
        this.routeInfoManager = new TopicRouteInfoManager();
        this.remotingClient = new NettyRemotingClient();
    }

    @Override
    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            logger.info("DefaultZephyrProducer[{}] start", producerGroup);
            remotingClient.start();
            if (nameserverAddresses != null && !nameserverAddresses.trim().isEmpty()) {
                routeInfoManager.updateNameServerAddressList(nameserverAddresses);
            }
            logger.info("Producer started successfully with nameserver: {}", nameserverAddresses);
        }
    }

    @Override
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            logger.info("DefaultZephyrProducer[{}] shutdown", producerGroup);
            asyncSenderExecutor.shutdown();
            if (remotingClient != null) {
                remotingClient.shutdown();
            }
            logger.info("Producer shutdown successfully");
        }
    }

    @Override
    public SendResult send(Message msg) throws Exception {
        return send(msg, sendMsgTimeout);
    }

    @Override
    public SendResult send(Message msg, long timeout) throws Exception {
        checkMessage(msg);
        checkProducerStarted();

        MessageQueue mq = selectOneMessageQueue(msg.getTopic());
        return sendMessage(msg, mq, timeout);
    }

    @Override
    public SendResult send(Message msg, MessageQueue mq) throws Exception {
        return send(msg, mq, sendMsgTimeout);
    }

    @Override
    public SendResult send(Message msg, MessageQueue mq, long timeout) throws Exception {
        checkMessage(msg);
        checkMessageQueue(mq);
        checkProducerStarted();

        return sendMessage(msg, mq, timeout);
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
                return sendMessage(msg, mq, timeout);
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
        checkProducerStarted();

        String brokerAddr = getBrokerAddress(mq.getBrokerName());
        if (brokerAddr == null) {
            throw new Exception("Cannot find broker address for: " + mq.getBrokerName());
        }

        RemotingCommand request = createSendMessageRequest(msg, mq);
        remotingClient.invokeOneway(brokerAddr, request);
        logger.debug("Send oneway message to queue: {}", mq);
    }

    @Override
    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws Exception {
        return routeInfoManager.getPublishMessageQueues(topic);
    }

    /**
     * Send message with custom queue selector
     *
     * @param msg message to send
     * @param selector message queue selector
     * @param arg selector argument
     * @return send result
     * @throws Exception if send fails
     */
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg) throws Exception {
        return send(msg, selector, arg, sendMsgTimeout);
    }

    /**
     * Send message with custom queue selector and timeout
     *
     * @param msg message to send
     * @param selector message queue selector
     * @param arg selector argument
     * @param timeout send timeout
     * @return send result
     * @throws Exception if send fails
     */
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout) throws Exception {
        checkMessage(msg);

        List<MessageQueue> messageQueues = fetchPublishMessageQueues(msg.getTopic());
        if (messageQueues.isEmpty()) {
            throw new Exception("No available message queue for topic: " + msg.getTopic());
        }

        MessageQueue selectedQueue = selector != null ?
            selector.select(messageQueues, msg, arg) :
            defaultMessageQueueSelector.select(messageQueues, msg, arg);

        if (selectedQueue == null) {
            throw new Exception("Failed to select message queue for topic: " + msg.getTopic());
        }

        return send(msg, selectedQueue, timeout);
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
        List<MessageQueue> messageQueues = fetchPublishMessageQueues(topic);
        if (messageQueues.isEmpty()) {
            throw new Exception("No available message queue for topic: " + topic);
        }

        return defaultMessageQueueSelector.select(messageQueues, null, null);
    }

    private SendResult sendMessage(Message msg, MessageQueue mq, long timeout) throws Exception {
        String brokerAddr = getBrokerAddress(mq.getBrokerName());
        if (brokerAddr == null) {
            throw new Exception("Cannot find broker address for: " + mq.getBrokerName());
        }

        RemotingCommand request = createSendMessageRequest(msg, mq);
        RemotingCommand response = remotingClient.invokeSync(brokerAddr, request, timeout);

        if (response.getCode() == ResponseCode.SUCCESS) {
            return parseSendResult(response, msg, mq);
        } else {
            throw new Exception("Send message failed: " + response.getRemark());
        }
    }

    private RemotingCommand createSendMessageRequest(Message msg, MessageQueue mq) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE);

        SendMessageRequestHeader header = new SendMessageRequestHeader();
        header.producerGroup = this.producerGroup;
        header.topic = msg.getTopic();
        header.defaultTopic = "TBW102";
        header.defaultTopicQueueNums = 4;
        header.queueId = mq.getQueueId();
        header.sysFlag = 0;
        header.bornTimestamp = System.currentTimeMillis();
        header.flag = msg.getFlag();
        header.properties = msg.getPropertiesString();
        header.reconsumeTimes = 0;

        request.setCustomHeader(header);
        request.setBody(msg.getBody());

        return request;
    }

    private SendResult parseSendResult(RemotingCommand response, Message msg, MessageQueue mq) {
        SendResult result = new SendResult();
        result.setSendStatus(SendStatus.SEND_OK);
        result.setMsgId(response.getExtFields() != null ? response.getExtFields().get("msgId") : UUID.randomUUID().toString());
        result.setMessageQueue(mq);

        String queueOffset = response.getExtFields() != null ? response.getExtFields().get("queueOffset") : null;
        result.setQueueOffset(queueOffset != null ? Long.parseLong(queueOffset) : System.currentTimeMillis());

        return result;
    }

    private String getBrokerAddress(String brokerName) {
        return routeInfoManager.findBrokerAddressInPublish(brokerName);
    }

    private void checkProducerStarted() throws Exception {
        if (!started.get()) {
            throw new Exception("Producer is not started");
        }
    }

    public static class SendMessageRequestHeader {
        public String producerGroup;
        public String topic;
        public String defaultTopic;
        public Integer defaultTopicQueueNums;
        public Integer queueId;
        public Integer sysFlag;
        public Long bornTimestamp;
        public Integer flag;
        public String properties;
        public Integer reconsumeTimes;
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

    public MessageQueueSelector getDefaultMessageQueueSelector() {
        return defaultMessageQueueSelector;
    }

    public void setDefaultMessageQueueSelector(MessageQueueSelector defaultMessageQueueSelector) {
        this.defaultMessageQueueSelector = defaultMessageQueueSelector;
    }

    public TopicRouteInfoManager getRouteInfoManager() {
        return routeInfoManager;
    }
}