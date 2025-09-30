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
import com.zephyr.broker.out.BrokerOuterAPI;
import com.zephyr.common.constant.ZephyrConstants;
import com.zephyr.protocol.network.RequestCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
    private BrokerOuterAPI brokerOuterAPI;
    private ScheduledExecutorService scheduledExecutorService;

    public ZephyrBroker(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
    }

    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            logger.info("ZephyrBroker[{}] starting...", brokerConfig.getBrokerName());

            // Initialize scheduled executor service
            this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

            // Initialize broker outer API
            this.brokerOuterAPI = new BrokerOuterAPI(brokerConfig);
            this.brokerOuterAPI.start();

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

            // Register broker to nameserver
            this.registerBrokerAll();

            // Start scheduled tasks
            this.startScheduledTasks();

            logger.info("ZephyrBroker[{}] started successfully on port {}",
                    brokerConfig.getBrokerName(), brokerConfig.getListenPort());
        }
    }

    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            logger.info("ZephyrBroker[{}] shutting down...", brokerConfig.getBrokerName());

            // Unregister broker from nameserver
            this.unregisterBrokerAll();

            // Shutdown scheduled tasks
            if (scheduledExecutorService != null) {
                scheduledExecutorService.shutdown();
            }

            if (remotingServer != null) {
                remotingServer.shutdown();
            }

            if (brokerOuterAPI != null) {
                brokerOuterAPI.shutdown();
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

    private void registerBrokerAll() throws Exception {
        if (brokerOuterAPI != null && brokerConfig.getNamesrvAddr() != null) {
            brokerOuterAPI.registerBrokerAll(
                    brokerConfig.getBrokerClusterName(),
                    "127.0.0.1:" + brokerConfig.getListenPort(),
                    brokerConfig.getBrokerName(),
                    brokerConfig.getBrokerId(),
                    null, // haServerAddr
                    null  // topicConfigWrapper
            );
            logger.info("Register broker to name server success");
        } else {
            throw new Exception("Cannot register broker: brokerOuterAPI or namesrvAddr is null");
        }
    }

    private void unregisterBrokerAll() {
        try {
            if (brokerOuterAPI != null && brokerConfig.getNamesrvAddr() != null) {
                brokerOuterAPI.unregisterBrokerAll(
                    brokerConfig.getBrokerClusterName(),
                    "127.0.0.1:" + brokerConfig.getListenPort(),
                    brokerConfig.getBrokerName(),
                    brokerConfig.getBrokerId()
                );
                logger.info("Unregister broker from name server success");
            }
        } catch (Exception e) {
            logger.error("Unregister broker from name server failed", e);
        }
    }

    private void startScheduledTasks() {
        // Placeholder for scheduled tasks like:
        // - Periodic broker registration
        // - Statistics reporting
        // - Cleanup tasks
        logger.info("Scheduled tasks started");
    }
}