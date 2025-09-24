package com.zephyr.nameserver;

import com.zephyr.common.constant.ZephyrConstants;
import com.zephyr.nameserver.processor.DefaultRequestProcessor;
import com.zephyr.nameserver.routeinfo.RouteInfoManager;
import com.zephyr.protocol.network.NettyRemotingServer;
import com.zephyr.protocol.network.RequestCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class ZephyrNameServer {

    private static final Logger logger = LoggerFactory.getLogger(ZephyrNameServer.class);

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final NameServerConfig nameServerConfig;
    private NettyRemotingServer remotingServer;
    private RouteInfoManager routeInfoManager;

    public ZephyrNameServer(NameServerConfig nameServerConfig) {
        this.nameServerConfig = nameServerConfig;
    }

    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            logger.info("ZephyrNameServer starting on port {}...", nameServerConfig.getListenPort());

            // Initialize route info manager
            this.routeInfoManager = new RouteInfoManager();
            this.routeInfoManager.start();

            // Initialize and start remoting server
            this.remotingServer = new NettyRemotingServer(nameServerConfig.getListenPort());
            registerProcessors();
            this.remotingServer.start();

            logger.info("ZephyrNameServer started successfully on port {}", nameServerConfig.getListenPort());
        }
    }

    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            logger.info("ZephyrNameServer shutting down...");

            if (remotingServer != null) {
                remotingServer.shutdown();
            }

            if (routeInfoManager != null) {
                routeInfoManager.shutdown();
            }

            logger.info("ZephyrNameServer shut down successfully");
        }
    }

    private void registerProcessors() {
        DefaultRequestProcessor requestProcessor = new DefaultRequestProcessor(this);

        // Register all request processors
        remotingServer.registerProcessor(RequestCode.REGISTER_BROKER, requestProcessor);
        remotingServer.registerProcessor(RequestCode.UNREGISTER_BROKER, requestProcessor);
        remotingServer.registerProcessor(RequestCode.GET_ROUTEINTO_BY_TOPIC, requestProcessor);
        remotingServer.registerProcessor(RequestCode.GET_BROKER_CLUSTER_INFO, requestProcessor);
        remotingServer.registerProcessor(RequestCode.UPDATE_AND_CREATE_TOPIC, requestProcessor);
        remotingServer.registerProcessor(RequestCode.DELETE_TOPIC_IN_NAMESRV, requestProcessor);
        remotingServer.registerProcessor(RequestCode.GET_KVLIST_BY_NAMESPACE, requestProcessor);
        remotingServer.registerProcessor(RequestCode.GET_TOPICS_BY_CLUSTER, requestProcessor);
        remotingServer.registerProcessor(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS, requestProcessor);
        remotingServer.registerProcessor(RequestCode.GET_UNIT_TOPIC_LIST, requestProcessor);
        remotingServer.registerProcessor(RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST, requestProcessor);
        remotingServer.registerProcessor(RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST, requestProcessor);
        remotingServer.registerProcessor(RequestCode.UPDATE_NAMESRV_CONFIG, requestProcessor);
        remotingServer.registerProcessor(RequestCode.GET_NAMESRV_CONFIG, requestProcessor);
    }

    public static void main(String[] args) {
        try {
            NameServerConfig nameServerConfig = new NameServerConfig();
            nameServerConfig.setListenPort(ZephyrConstants.DEFAULT_NAMESERVER_PORT);

            ZephyrNameServer nameServer = new ZephyrNameServer(nameServerConfig);
            nameServer.start();

            Runtime.getRuntime().addShutdownHook(new Thread(nameServer::shutdown));

            logger.info("ZephyrNameServer started successfully, press Ctrl+C to exit");
            Thread.currentThread().join();

        } catch (Exception e) {
            logger.error("ZephyrNameServer startup failed", e);
            System.exit(1);
        }
    }

    // Getters
    public NameServerConfig getNameServerConfig() {
        return nameServerConfig;
    }

    public RouteInfoManager getRouteInfoManager() {
        return routeInfoManager;
    }

    public NettyRemotingServer getRemotingServer() {
        return remotingServer;
    }
}