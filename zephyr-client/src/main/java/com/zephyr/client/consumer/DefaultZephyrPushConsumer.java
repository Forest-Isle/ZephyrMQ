package com.zephyr.client.consumer;

import com.zephyr.protocol.message.MessageExt;
import com.zephyr.protocol.message.MessageQueue;
import com.zephyr.protocol.network.NettyRemotingClient;
import com.zephyr.protocol.network.RemotingCommand;
import com.zephyr.protocol.network.RequestCode;
import com.zephyr.protocol.network.ResponseCode;
import com.zephyr.client.route.TopicRouteInfoManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.zephyr.common.constant.ZephyrConstants.*;

public class DefaultZephyrPushConsumer implements ZephyrPushConsumer {

    private static final Logger logger = LoggerFactory.getLogger(DefaultZephyrPushConsumer.class);

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduledExecutorService;
    private final ExecutorService consumeExecutor;
    private NettyRemotingClient remotingClient;
    private TopicRouteInfoManager routeInfoManager;
    private final ConcurrentMap<MessageQueue, Long> processQueueTable = new ConcurrentHashMap<>();

    private String consumerGroup;
    private String nameserverAddresses;
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    private int consumeThreadMin = 20;
    private int consumeThreadMax = 64;
    private long consumeTimeout = DEFAULT_CONSUME_TIMEOUT_MILLIS;

    private MessageListener messageListener;
    private final ConcurrentMap<String, String> subscription = new ConcurrentHashMap<>();

    public DefaultZephyrPushConsumer() {
        this(DEFAULT_CONSUMER_GROUP);
    }

    public DefaultZephyrPushConsumer(String consumerGroup) {
        this.consumerGroup = consumerGroup;
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        this.consumeExecutor = Executors.newFixedThreadPool(consumeThreadMax);
        this.remotingClient = new NettyRemotingClient();
        this.routeInfoManager = new TopicRouteInfoManager();
    }

    @Override
    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            logger.info("DefaultZephyrPushConsumer[{}] start", consumerGroup);

            if (messageListener == null) {
                throw new IllegalArgumentException("MessageListener cannot be null");
            }

            remotingClient.start();
            if (nameserverAddresses != null && !nameserverAddresses.trim().isEmpty()) {
                routeInfoManager.updateNameServerAddressList(nameserverAddresses);
            }

            // Start periodic rebalance
            scheduledExecutorService.scheduleAtFixedRate(this::doRebalance, 10, 20, TimeUnit.SECONDS);

            // Start periodic message pulling
            scheduledExecutorService.scheduleAtFixedRate(this::pullMessages, 5, 5, TimeUnit.SECONDS);

            logger.info("DefaultZephyrPushConsumer[{}] started successfully", consumerGroup);
        }
    }

    @Override
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            logger.info("DefaultZephyrPushConsumer[{}] shutdown", consumerGroup);

            scheduledExecutorService.shutdown();
            consumeExecutor.shutdown();
            if (remotingClient != null) {
                remotingClient.shutdown();
            }

            logger.info("DefaultZephyrPushConsumer[{}] shutdown successfully", consumerGroup);
        }
    }

    @Override
    public void registerMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
    }

    @Override
    public void subscribe(String topic, String subExpression) throws Exception {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic cannot be null or empty");
        }

        subscription.put(topic, subExpression == null ? "*" : subExpression);
        logger.info("Subscribe topic: {}, subExpression: {}", topic, subExpression);

        // Update route info for the subscribed topic
        if (started.get()) {
            routeInfoManager.updateTopicRouteInfoFromNameServer(topic);
        }
    }

    @Override
    public void unsubscribe(String topic) {
        subscription.remove(topic);
        logger.info("Unsubscribe topic: {}", topic);

        // TODO: Send unsubscription request to broker
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        this.consumeThreadMin = corePoolSize;
        logger.info("Updated consume thread min to: {}", corePoolSize);
    }

    @Override
    public void suspend() {
        logger.info("Suspend consumer: {}", consumerGroup);
        processQueueTable.clear();
    }

    @Override
    public void resume() {
        logger.info("Resume consumer: {}", consumerGroup);
        // Resume will be handled by the next rebalance
    }

    private void doRebalance() {
        try {
            logger.debug("Do rebalance for consumer group: {}", consumerGroup);

            for (String topic : subscription.keySet()) {
                List<MessageQueue> mqAll = routeInfoManager.fetchSubscribeMessageQueues(topic);
                if (mqAll != null && !mqAll.isEmpty()) {
                    // Simple allocation - assign all queues to this consumer for now
                    for (MessageQueue mq : mqAll) {
                        if (!processQueueTable.containsKey(mq)) {
                            processQueueTable.put(mq, 0L);
                            logger.info("New message queue allocated: {}", mq);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Rebalance failed", e);
        }
    }

    private void pullMessages() {
        for (MessageQueue mq : processQueueTable.keySet()) {
            pullMessage(mq);
        }
    }

    private void pullMessage(MessageQueue messageQueue) {
        try {
            String brokerAddr = routeInfoManager.findBrokerAddressInSubscribe(
                messageQueue.getBrokerName(), 0L, false);
            if (brokerAddr == null) {
                logger.warn("Cannot find broker address for queue: {}", messageQueue);
                return;
            }

            RemotingCommand request = createPullMessageRequest(messageQueue);
            remotingClient.invokeAsync(brokerAddr, request, new NettyRemotingClient.InvokeCallback() {
                @Override
                public void operationComplete(RemotingCommand response) {
                    if (response.getCode() == ResponseCode.SUCCESS) {
                        List<MessageExt> messages = decodeMessages(response);
                        if (!messages.isEmpty()) {
                            consumeExecutor.submit(() -> consumeMessage(messages, messageQueue));
                        }
                    } else {
                        logger.debug("Pull message response: {}", response.getRemark());
                    }
                }

                @Override
                public void operationFailed(Throwable throwable) {
                    logger.error("Pull message failed for queue: {}", messageQueue, throwable);
                }
            });

            logger.debug("Pull message from queue: {}", messageQueue);
        } catch (Exception e) {
            logger.error("Pull message failed", e);
        }
    }

    private RemotingCommand createPullMessageRequest(MessageQueue mq) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE);

        PullMessageRequestHeader header = new PullMessageRequestHeader();
        header.consumerGroup = this.consumerGroup;
        header.topic = mq.getTopic();
        header.queueId = mq.getQueueId();
        header.queueOffset = processQueueTable.getOrDefault(mq, 0L);
        header.maxMsgNums = 32; // Pull up to 32 messages at a time
        header.sysFlag = 0;
        header.commitOffset = 0L;
        header.suspendTimeoutMillis = 15000L; // 15 seconds
        header.subscription = subscription.get(mq.getTopic());

        request.setCustomHeader(header);
        return request;
    }

    private List<MessageExt> decodeMessages(RemotingCommand response) {
        List<MessageExt> messages = new ArrayList<>();
        try {
            // Simple decoding - in practice, this would be more complex
            if (response.getBody() != null && response.getBody().length > 0) {
                // Mock message creation for now
                MessageExt msg = new MessageExt();
                msg.setBody(response.getBody());
                msg.setTopic("test"); // Would be extracted from response
                messages.add(msg);
            }
        } catch (Exception e) {
            logger.error("Failed to decode messages", e);
        }
        return messages;
    }

    private void consumeMessage(List<MessageExt> msgs, MessageQueue messageQueue) {
        try {
            ConsumeOrderlyContext context = new ConsumeOrderlyContext();
            context.setMessageQueue(messageQueue);

            ConsumeOrderlyStatus status = messageListener.consumeMessage(msgs, context);

            if (status == ConsumeOrderlyStatus.SUCCESS) {
                // Update queue offset
                long maxOffset = processQueueTable.get(messageQueue) + msgs.size();
                processQueueTable.put(messageQueue, maxOffset);

                // Send ACK to broker
                sendMessageAck(msgs, messageQueue);
                logger.debug("Consume message success, queue: {}, msgCount: {}", messageQueue, msgs.size());
            } else {
                logger.warn("Consume message failed, queue: {}, msgCount: {}", messageQueue, msgs.size());
                // Handle consume failure - could implement retry logic here
            }
        } catch (Exception e) {
            logger.error("Consume message exception", e);
        }
    }

    private void sendMessageAck(List<MessageExt> msgs, MessageQueue messageQueue) {
        try {
            String brokerAddr = routeInfoManager.findBrokerAddressInSubscribe(
                messageQueue.getBrokerName(), 0L, false);
            if (brokerAddr != null) {
                RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK);
                // Set ACK information in request
                remotingClient.invokeOneway(brokerAddr, request);
            }
        } catch (Exception e) {
            logger.error("Send message ACK failed", e);
        }
    }

    public static class PullMessageRequestHeader {
        public String consumerGroup;
        public String topic;
        public Integer queueId;
        public Long queueOffset;
        public Integer maxMsgNums;
        public Integer sysFlag;
        public Long commitOffset;
        public Long suspendTimeoutMillis;
        public String subscription;
    }

    // Getters and Setters
    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getNameserverAddresses() {
        return nameserverAddresses;
    }

    public void setNameserverAddresses(String nameserverAddresses) {
        this.nameserverAddresses = nameserverAddresses;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public int getConsumeThreadMin() {
        return consumeThreadMin;
    }

    public void setConsumeThreadMin(int consumeThreadMin) {
        this.consumeThreadMin = consumeThreadMin;
    }

    public int getConsumeThreadMax() {
        return consumeThreadMax;
    }

    public void setConsumeThreadMax(int consumeThreadMax) {
        this.consumeThreadMax = consumeThreadMax;
    }

    public long getConsumeTimeout() {
        return consumeTimeout;
    }

    public void setConsumeTimeout(long consumeTimeout) {
        this.consumeTimeout = consumeTimeout;
    }
}