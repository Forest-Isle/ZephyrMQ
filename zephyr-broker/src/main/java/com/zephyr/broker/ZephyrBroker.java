package com.zephyr.broker;

import com.zephyr.common.config.BrokerConfig;
import com.zephyr.protocol.network.NettyRemotingServer;
import com.zephyr.broker.processor.SendMessageProcessor;
import com.zephyr.broker.processor.PullMessageProcessor;
import com.zephyr.broker.processor.QueryMessageProcessor;
import com.zephyr.broker.processor.HeartbeatProcessor;
import com.zephyr.broker.store.MessageStore;
import com.zephyr.broker.store.DefaultMessageStore;
import com.zephyr.broker.topic.TopicConfigManager;
import com.zephyr.broker.subscription.SubscriptionGroupManager;
import com.zephyr.broker.offset.ConsumerOffsetManager;
import com.zephyr.common.constant.ZephyrConstants;
import com.zephyr.protocol.network.RequestCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class ZephyrBroker {

    private static final Logger logger = LoggerFactory.getLogger(ZephyrBroker.class);

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final BrokerConfig brokerConfig;
    private NettyRemotingServer remotingServer;
    private MessageStore messageStore;
    private TopicConfigManager topicConfigManager;
    private SubscriptionGroupManager subscriptionGroupManager;
    private ConsumerOffsetManager consumerOffsetManager;

    public ZephyrBroker(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
    }

    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            logger.info("ZephyrBroker[{}] starting...", brokerConfig.getBrokerName());

            // Initialize message store
            this.messageStore = new DefaultMessageStore(brokerConfig);
            this.messageStore.start();

            // Initialize topic config manager
            this.topicConfigManager = new TopicConfigManager(brokerConfig);
            this.topicConfigManager.start();

            // Initialize subscription group manager
            this.subscriptionGroupManager = new SubscriptionGroupManager(brokerConfig);
            this.subscriptionGroupManager.start();

            // Initialize consumer offset manager
            this.consumerOffsetManager = new ConsumerOffsetManager(brokerConfig);
            this.consumerOffsetManager.start();

            // Initialize and start remoting server
            this.remotingServer = new NettyRemotingServer(brokerConfig.getListenPort());
            registerProcessors();
            this.remotingServer.start();

            logger.info("ZephyrBroker[{}] started successfully on port {}",
                    brokerConfig.getBrokerName(), brokerConfig.getListenPort());
        }
    }

    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            logger.info("ZephyrBroker[{}] shutting down...", brokerConfig.getBrokerName());

            if (remotingServer != null) {
                remotingServer.shutdown();
            }

            if (consumerOffsetManager != null) {
                consumerOffsetManager.shutdown();
            }

            if (subscriptionGroupManager != null) {
                subscriptionGroupManager.shutdown();
            }

            if (topicConfigManager != null) {
                topicConfigManager.shutdown();
            }

            if (messageStore != null) {
                messageStore.shutdown();
            }

            logger.info("ZephyrBroker[{}] shut down successfully", brokerConfig.getBrokerName());
        }
    }

    private void registerProcessors() {
        // Register message processors
        SendMessageProcessor sendMessageProcessor = new SendMessageProcessor(this);
        PullMessageProcessor pullMessageProcessor = new PullMessageProcessor(this);
        QueryMessageProcessor queryMessageProcessor = new QueryMessageProcessor(this);
        HeartbeatProcessor heartbeatProcessor = new HeartbeatProcessor(this);

        // Register processors to handle different request types
        remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendMessageProcessor);
        remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendMessageProcessor);
        remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendMessageProcessor);

        remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, pullMessageProcessor);
        remotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryMessageProcessor);
        remotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryMessageProcessor);

        remotingServer.registerProcessor(RequestCode.HEART_BEAT, heartbeatProcessor);
        remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, heartbeatProcessor);
        remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, heartbeatProcessor);
        remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, heartbeatProcessor);
    }

    public static void main(String[] args) {
        try {
            BrokerConfig brokerConfig = new BrokerConfig();
            brokerConfig.setBrokerName("DefaultBroker");
            brokerConfig.setListenPort(ZephyrConstants.DEFAULT_BROKER_PORT);
            brokerConfig.setStorePathRootDir(System.getProperty("user.home") + "/zephyr-store");

            ZephyrBroker broker = new ZephyrBroker(brokerConfig);
            broker.start();

            Runtime.getRuntime().addShutdownHook(new Thread(broker::shutdown));

            logger.info("ZephyrBroker started successfully, press Ctrl+C to exit");
            Thread.currentThread().join();

        } catch (Exception e) {
            logger.error("ZephyrBroker startup failed", e);
            System.exit(1);
        }
    }

    // Getters
    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }

    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return subscriptionGroupManager;
    }

    public ConsumerOffsetManager getConsumerOffsetManager() {
        return consumerOffsetManager;
    }

    public NettyRemotingServer getRemotingServer() {
        return remotingServer;
    }
}