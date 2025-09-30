package com.zephyr.broker.out;

import com.zephyr.common.config.BrokerConfig;
import com.zephyr.protocol.network.NettyRemotingClient;
import com.zephyr.protocol.network.RemotingCommand;
import com.zephyr.protocol.network.RequestCode;
import com.zephyr.protocol.network.ResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class BrokerOuterAPI {

    private static final Logger logger = LoggerFactory.getLogger(BrokerOuterAPI.class);

    private final NettyRemotingClient remotingClient;
    private final BrokerConfig brokerConfig;
    private final ConcurrentMap<String, String> nameServerAddressMap = new ConcurrentHashMap<>();

    public BrokerOuterAPI(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
        this.remotingClient = new NettyRemotingClient();
    }

    public void start() {
        remotingClient.start();
        logger.info("BrokerOuterAPI started");
    }

    public void shutdown() {
        if (remotingClient != null) {
            remotingClient.shutdown();
        }
        logger.info("BrokerOuterAPI shut down");
    }

    public RegisterBrokerResult registerBrokerAll(
            String clusterName,
            String brokerAddr,
            String brokerName,
            long brokerId,
            String haServerAddr,
            TopicConfigSerializeWrapper topicConfigWrapper) {

        RegisterBrokerResult registerBrokerResult = null;

        // Get nameserver addresses (for now use default)
        String nameServerAddr = getNameServerAddress();
        if (nameServerAddr == null) {
            logger.warn("No nameserver address configured, using default");
            nameServerAddr = "127.0.0.1:9876";
        }

        try {
            RegisterBrokerRequestHeader requestHeader = new RegisterBrokerRequestHeader();
            requestHeader.setBrokerName(brokerName);
            requestHeader.setBrokerAddr(brokerAddr);
            requestHeader.setClusterName(clusterName);
            requestHeader.setBrokerId(brokerId);
            requestHeader.setHaServerAddr(haServerAddr);

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_BROKER);
            request.setCustomHeader(requestHeader);

            // Set body if topicConfigWrapper is provided
            if (topicConfigWrapper != null) {
                request.setBody(serializeTopicConfigWrapper(topicConfigWrapper));
            }

            RemotingCommand response = remotingClient.invokeSync(nameServerAddr, request, 3000);

            if (response.getCode() == ResponseCode.SUCCESS) {
                registerBrokerResult = deserializeRegisterBrokerResult(response.getBody());
                logger.info("Register broker success: brokerName={}, brokerAddr={}", brokerName, brokerAddr);
            } else {
                logger.warn("Register broker failed: code={}, remark={}", response.getCode(), response.getRemark());
            }

        } catch (Exception e) {
            throw new RuntimeException("Register broker exception: brokerName=" + brokerName, e);
        }

        return registerBrokerResult;
    }

    public void unregisterBrokerAll(
            String clusterName,
            String brokerAddr,
            String brokerName,
            long brokerId) {

        String nameServerAddr = getNameServerAddress();
        if (nameServerAddr == null) {
            nameServerAddr = "127.0.0.1:9876";
        }

        try {
            UnregisterBrokerRequestHeader requestHeader = new UnregisterBrokerRequestHeader();
            requestHeader.setBrokerName(brokerName);
            requestHeader.setBrokerAddr(brokerAddr);
            requestHeader.setClusterName(clusterName);
            requestHeader.setBrokerId(brokerId);

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_BROKER);
            request.setCustomHeader(requestHeader);

            RemotingCommand response = remotingClient.invokeSync(nameServerAddr, request, 3000);

            if (response.getCode() == ResponseCode.SUCCESS) {
                logger.info("Unregister broker success: brokerName={}, brokerAddr={}", brokerName, brokerAddr);
            } else {
                logger.warn("Unregister broker failed: code={}, remark={}", response.getCode(), response.getRemark());
            }

        } catch (Exception e) {
            logger.error("Unregister broker exception: brokerName={}", brokerName, e);
        }
    }

    public TopicRouteData getTopicRouteInfoFromNameServer(String topic, long timeoutMillis) {
        String nameServerAddr = getNameServerAddress();
        if (nameServerAddr == null) {
            nameServerAddr = "127.0.0.1:9876";
        }

        try {
            GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
            requestHeader.setTopic(topic);

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINTO_BY_TOPIC);
            request.setCustomHeader(requestHeader);

            RemotingCommand response = remotingClient.invokeSync(nameServerAddr, request, timeoutMillis);

            if (response.getCode() == ResponseCode.SUCCESS) {
                return deserializeTopicRouteData(response.getBody());
            } else {
                logger.warn("Get topic route info failed: topic={}, code={}, remark={}",
                    topic, response.getCode(), response.getRemark());
            }

        } catch (Exception e) {
            logger.error("Get topic route info exception: topic={}", topic, e);
        }

        return null;
    }

    private String getNameServerAddress() {
        // For now return default, later can be configured
        return brokerConfig.getNameServerAddr();
    }

    private byte[] serializeTopicConfigWrapper(TopicConfigSerializeWrapper wrapper) {
        // Simplified serialization
        StringBuilder sb = new StringBuilder();
        if (wrapper.getTopicConfigTable() != null) {
            for (String topic : wrapper.getTopicConfigTable().keySet()) {
                sb.append(topic).append(";");
            }
        }
        return sb.toString().getBytes();
    }

    private RegisterBrokerResult deserializeRegisterBrokerResult(byte[] data) {
        RegisterBrokerResult result = new RegisterBrokerResult();
        if (data != null) {
            String content = new String(data);
            String[] parts = content.split(";");
            for (String part : parts) {
                if (part.startsWith("masterAddr=")) {
                    result.setMasterAddr(part.substring("masterAddr=".length()));
                } else if (part.startsWith("haServerAddr=")) {
                    result.setHaServerAddr(part.substring("haServerAddr=".length()));
                }
            }
        }
        return result;
    }

    private TopicRouteData deserializeTopicRouteData(byte[] data) {
        // Simplified deserialization - in real implementation would use proper serialization
        TopicRouteData routeData = new TopicRouteData();
        // TODO: Implement proper deserialization
        return routeData;
    }

    // Request header classes
    public static class RegisterBrokerRequestHeader {
        private String brokerName;
        private String brokerAddr;
        private String clusterName;
        private String haServerAddr;
        private long brokerId;

        // Getters and setters
        public String getBrokerName() { return brokerName; }
        public void setBrokerName(String brokerName) { this.brokerName = brokerName; }
        public String getBrokerAddr() { return brokerAddr; }
        public void setBrokerAddr(String brokerAddr) { this.brokerAddr = brokerAddr; }
        public String getClusterName() { return clusterName; }
        public void setClusterName(String clusterName) { this.clusterName = clusterName; }
        public String getHaServerAddr() { return haServerAddr; }
        public void setHaServerAddr(String haServerAddr) { this.haServerAddr = haServerAddr; }
        public long getBrokerId() { return brokerId; }
        public void setBrokerId(long brokerId) { this.brokerId = brokerId; }
    }

    public static class UnregisterBrokerRequestHeader {
        private String brokerName;
        private String brokerAddr;
        private String clusterName;
        private long brokerId;

        // Getters and setters
        public String getBrokerName() { return brokerName; }
        public void setBrokerName(String brokerName) { this.brokerName = brokerName; }
        public String getBrokerAddr() { return brokerAddr; }
        public void setBrokerAddr(String brokerAddr) { this.brokerAddr = brokerAddr; }
        public String getClusterName() { return clusterName; }
        public void setClusterName(String clusterName) { this.clusterName = clusterName; }
        public long getBrokerId() { return brokerId; }
        public void setBrokerId(long brokerId) { this.brokerId = brokerId; }
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

    public static class TopicConfigSerializeWrapper {
        private ConcurrentMap<String, TopicConfig> topicConfigTable;

        public ConcurrentMap<String, TopicConfig> getTopicConfigTable() { return topicConfigTable; }
        public void setTopicConfigTable(ConcurrentMap<String, TopicConfig> topicConfigTable) { this.topicConfigTable = topicConfigTable; }
    }

    public static class TopicConfig {
        private String topicName;
        private int readQueueNums = 4;
        private int writeQueueNums = 4;
        private int perm = 6;
        private int topicSysFlag = 0;

        public String getTopicName() { return topicName; }
        public void setTopicName(String topicName) { this.topicName = topicName; }
        public int getReadQueueNums() { return readQueueNums; }
        public void setReadQueueNums(int readQueueNums) { this.readQueueNums = readQueueNums; }
        public int getWriteQueueNums() { return writeQueueNums; }
        public void setWriteQueueNums(int writeQueueNums) { this.writeQueueNums = writeQueueNums; }
        public int getPerm() { return perm; }
        public void setPerm(int perm) { this.perm = perm; }
        public int getTopicSysFlag() { return topicSysFlag; }
        public void setTopicSysFlag(int topicSysFlag) { this.topicSysFlag = topicSysFlag; }
    }

    public static class TopicRouteData {
        private String orderTopicConf;
        // TODO: Add proper fields for route data

        public String getOrderTopicConf() { return orderTopicConf; }
        public void setOrderTopicConf(String orderTopicConf) { this.orderTopicConf = orderTopicConf; }
    }
}