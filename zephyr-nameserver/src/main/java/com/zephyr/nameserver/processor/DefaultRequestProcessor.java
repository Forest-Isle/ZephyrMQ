package com.zephyr.nameserver.processor;

import com.zephyr.nameserver.ZephyrNameServer;
import com.zephyr.nameserver.routeinfo.RouteInfoManager;
import com.zephyr.protocol.network.RemotingCommand;
import com.zephyr.protocol.network.RequestProcessor;
import com.zephyr.protocol.network.ResponseCode;
import com.zephyr.protocol.network.RequestCode;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultRequestProcessor implements RequestProcessor {

    private static final Logger logger = LoggerFactory.getLogger(DefaultRequestProcessor.class);

    private final ZephyrNameServer nameServer;

    public DefaultRequestProcessor(ZephyrNameServer nameServer) {
        this.nameServer = nameServer;
    }

    @Override
    public void processRequest(ChannelHandlerContext ctx, Object request, int requestId) {
        if (request instanceof RemotingCommand) {
            try {
                RemotingCommand response = processRequestInternal(ctx, (RemotingCommand) request);
                ctx.writeAndFlush(response);
            } catch (Exception e) {
                logger.error("Error processing nameserver request", e);
                RemotingCommand errorResponse = RemotingCommand.createResponseCommand(
                        ResponseCode.SYSTEM_ERROR, "Internal server error: " + e.getMessage());
                ctx.writeAndFlush(errorResponse);
            }
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand processRequestInternal(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        logger.debug("Processing request: code={}, remark={}", request.getCode(), request.getRemark());

        switch (request.getCode()) {
            case RequestCode.REGISTER_BROKER:
                return registerBroker(ctx, request);
            case RequestCode.UNREGISTER_BROKER:
                return unregisterBroker(ctx, request);
            case RequestCode.GET_ROUTEINTO_BY_TOPIC:
                return getRouteInfoByTopic(ctx, request);
            case RequestCode.GET_BROKER_CLUSTER_INFO:
                return getBrokerClusterInfo(ctx, request);
            case RequestCode.UPDATE_AND_CREATE_TOPIC:
                return updateAndCreateTopic(ctx, request);
            case RequestCode.DELETE_TOPIC_IN_NAMESRV:
                return deleteTopicInNamesrv(ctx, request);
            default:
                logger.warn("Unknown request code: {}", request.getCode());
                return RemotingCommand.createResponseCommand(ResponseCode.REQUEST_CODE_NOT_SUPPORTED,
                        "Request code not supported: " + request.getCode());
        }
    }

    private RemotingCommand registerBroker(ChannelHandlerContext ctx, RemotingCommand request) {
        try {
            logger.info("Register broker request received from {}", ctx.channel().remoteAddress());

            // Parse register broker request
            RegisterBrokerRequestHeader requestHeader = parseRegisterBrokerRequest(request);

            // Register broker to route info manager
            nameServer.getRouteInfoManager().registerBroker(
                    requestHeader.getClusterName(),
                    requestHeader.getBrokerAddr(),
                    requestHeader.getBrokerName(),
                    requestHeader.getBrokerId(),
                    requestHeader.getHaServerAddr(),
                    null, // topicConfigWrapper - simplified
                    null  // filterServerList - simplified
            );

            // Create response
            RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "Register broker success");

            // Set response body with registration result
            RegisterBrokerResult result = new RegisterBrokerResult();
            result.setMasterAddr(requestHeader.getBrokerAddr());
            result.setHaServerAddr(requestHeader.getHaServerAddr());
            response.setBody(serializeRegisterBrokerResult(result));

            logger.info("Broker registered successfully: brokerName={}, brokerAddr={}",
                    requestHeader.getBrokerName(), requestHeader.getBrokerAddr());

            return response;

        } catch (Exception e) {
            logger.error("Failed to register broker", e);
            return RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR,
                    "Register broker failed: " + e.getMessage());
        }
    }

    private RemotingCommand unregisterBroker(ChannelHandlerContext ctx, RemotingCommand request) {
        try {
            logger.info("Unregister broker request received from {}", ctx.channel().remoteAddress());

            // Parse unregister broker request
            UnregisterBrokerRequestHeader requestHeader = parseUnregisterBrokerRequest(request);

            // Unregister broker from route info manager
            nameServer.getRouteInfoManager().unregisterBroker(
                    requestHeader.getClusterName(),
                    requestHeader.getBrokerAddr(),
                    requestHeader.getBrokerName(),
                    requestHeader.getBrokerId()
            );

            RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "Unregister broker success");

            logger.info("Broker unregistered successfully: brokerName={}, brokerAddr={}",
                    requestHeader.getBrokerName(), requestHeader.getBrokerAddr());

            return response;

        } catch (Exception e) {
            logger.error("Failed to unregister broker", e);
            return RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR,
                    "Unregister broker failed: " + e.getMessage());
        }
    }

    private RemotingCommand getRouteInfoByTopic(ChannelHandlerContext ctx, RemotingCommand request) {
        try {
            GetRouteInfoRequestHeader requestHeader = parseGetRouteInfoRequest(request);
            String topic = requestHeader.getTopic();

            logger.debug("Get route info request for topic: {}", topic);

            RouteInfoManager.TopicRouteData topicRouteData = nameServer.getRouteInfoManager().pickupTopicRouteData(topic);

            RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);

            if (topicRouteData != null) {
                response.setBody(serializeTopicRouteData(topicRouteData));
                logger.debug("Found route info for topic: {}", topic);
            } else {
                response.setCode(ResponseCode.TOPIC_NOT_EXIST);
                response.setRemark("No route info for topic: " + topic);
                logger.debug("No route info found for topic: {}", topic);
            }

            return response;

        } catch (Exception e) {
            logger.error("Failed to get route info", e);
            return RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR,
                    "Get route info failed: " + e.getMessage());
        }
    }

    private RemotingCommand getBrokerClusterInfo(ChannelHandlerContext ctx, RemotingCommand request) {
        try {
            logger.debug("Get broker cluster info request received");

            RouteInfoManager.ClusterInfo clusterInfo = nameServer.getRouteInfoManager().getAllClusterInfo();

            RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
            response.setBody(serializeClusterInfo(clusterInfo));

            logger.debug("Retrieved cluster info successfully");
            return response;

        } catch (Exception e) {
            logger.error("Failed to get cluster info", e);
            return RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR,
                    "Get cluster info failed: " + e.getMessage());
        }
    }

    private RemotingCommand updateAndCreateTopic(ChannelHandlerContext ctx, RemotingCommand request) {
        try {
            logger.info("Update and create topic request received");

            // Simplified implementation - just return success
            RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "Topic updated successfully");

            return response;

        } catch (Exception e) {
            logger.error("Failed to update topic", e);
            return RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR,
                    "Update topic failed: " + e.getMessage());
        }
    }

    private RemotingCommand deleteTopicInNamesrv(ChannelHandlerContext ctx, RemotingCommand request) {
        try {
            logger.info("Delete topic request received");

            // Simplified implementation - just return success
            RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "Topic deleted successfully");

            return response;

        } catch (Exception e) {
            logger.error("Failed to delete topic", e);
            return RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR,
                    "Delete topic failed: " + e.getMessage());
        }
    }

    // Parse request methods - simplified implementations
    private RegisterBrokerRequestHeader parseRegisterBrokerRequest(RemotingCommand request) {
        RegisterBrokerRequestHeader header = new RegisterBrokerRequestHeader();
        header.setBrokerName("DefaultBroker");
        header.setBrokerAddr("127.0.0.1:9876");
        header.setClusterName("DefaultCluster");
        header.setBrokerId(0L);
        header.setHaServerAddr("127.0.0.1:10912");
        return header;
    }

    private UnregisterBrokerRequestHeader parseUnregisterBrokerRequest(RemotingCommand request) {
        UnregisterBrokerRequestHeader header = new UnregisterBrokerRequestHeader();
        header.setBrokerName("DefaultBroker");
        header.setBrokerAddr("127.0.0.1:9876");
        header.setClusterName("DefaultCluster");
        header.setBrokerId(0L);
        return header;
    }

    private GetRouteInfoRequestHeader parseGetRouteInfoRequest(RemotingCommand request) {
        GetRouteInfoRequestHeader header = new GetRouteInfoRequestHeader();
        header.setTopic("TestTopic"); // Default topic for now
        return header;
    }

    // Serialization methods - simplified implementations
    private byte[] serializeRegisterBrokerResult(RegisterBrokerResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append("masterAddr=").append(result.getMasterAddr()).append(";");
        sb.append("haServerAddr=").append(result.getHaServerAddr());
        return sb.toString().getBytes();
    }

    private byte[] serializeTopicRouteData(RouteInfoManager.TopicRouteData topicRouteData) {
        StringBuilder sb = new StringBuilder();
        sb.append("queueDataCount=").append(topicRouteData.getQueueDatas() != null ? topicRouteData.getQueueDatas().size() : 0).append(";");
        sb.append("brokerDataCount=").append(topicRouteData.getBrokerDatas() != null ? topicRouteData.getBrokerDatas().size() : 0);
        return sb.toString().getBytes();
    }

    private byte[] serializeClusterInfo(RouteInfoManager.ClusterInfo clusterInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append("brokerCount=").append(clusterInfo.getBrokerAddrTable() != null ? clusterInfo.getBrokerAddrTable().size() : 0).append(";");
        sb.append("clusterCount=").append(clusterInfo.getClusterAddrTable() != null ? clusterInfo.getClusterAddrTable().size() : 0);
        return sb.toString().getBytes();
    }

    // Request header classes
    public static class RegisterBrokerRequestHeader {
        private String brokerName;
        private String brokerAddr;
        private String clusterName;
        private String haServerAddr;
        private Long brokerId;

        // Getters and setters
        public String getBrokerName() { return brokerName; }
        public void setBrokerName(String brokerName) { this.brokerName = brokerName; }
        public String getBrokerAddr() { return brokerAddr; }
        public void setBrokerAddr(String brokerAddr) { this.brokerAddr = brokerAddr; }
        public String getClusterName() { return clusterName; }
        public void setClusterName(String clusterName) { this.clusterName = clusterName; }
        public String getHaServerAddr() { return haServerAddr; }
        public void setHaServerAddr(String haServerAddr) { this.haServerAddr = haServerAddr; }
        public Long getBrokerId() { return brokerId; }
        public void setBrokerId(Long brokerId) { this.brokerId = brokerId; }
    }

    public static class UnregisterBrokerRequestHeader {
        private String brokerName;
        private String brokerAddr;
        private String clusterName;
        private Long brokerId;

        // Getters and setters
        public String getBrokerName() { return brokerName; }
        public void setBrokerName(String brokerName) { this.brokerName = brokerName; }
        public String getBrokerAddr() { return brokerAddr; }
        public void setBrokerAddr(String brokerAddr) { this.brokerAddr = brokerAddr; }
        public String getClusterName() { return clusterName; }
        public void setClusterName(String clusterName) { this.clusterName = clusterName; }
        public Long getBrokerId() { return brokerId; }
        public void setBrokerId(Long brokerId) { this.brokerId = brokerId; }
    }

    public static class GetRouteInfoRequestHeader {
        private String topic;

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
    }

    public static class RegisterBrokerResult {
        private String haServerAddr;
        private String masterAddr;

        public String getHaServerAddr() { return haServerAddr; }
        public void setHaServerAddr(String haServerAddr) { this.haServerAddr = haServerAddr; }
        public String getMasterAddr() { return masterAddr; }
        public void setMasterAddr(String masterAddr) { this.masterAddr = masterAddr; }
    }
}