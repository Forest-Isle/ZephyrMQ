package com.zephyr.nameserver.routeinfo;

import com.zephyr.protocol.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RouteInfoManager {

    private static final Logger logger = LoggerFactory.getLogger(RouteInfoManager.class);

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduledExecutorService;

    // Topic routing info: topic -> QueueData list
    private final ConcurrentMap<String, List<QueueData>> topicQueueTable = new ConcurrentHashMap<>();

    // Broker address info: brokerName -> BrokerData
    private final ConcurrentMap<String, BrokerData> brokerAddrTable = new ConcurrentHashMap<>();

    // Cluster info: clusterName -> Set<brokerName>
    private final ConcurrentMap<String, Set<String>> clusterAddrTable = new ConcurrentHashMap<>();

    // Broker live info: brokerAddr -> BrokerLiveInfo
    private final ConcurrentMap<String, BrokerLiveInfo> brokerLiveTable = new ConcurrentHashMap<>();

    // Filter server info: brokerAddr -> List<filterServerAddr>
    private final ConcurrentMap<String, List<String>> filterServerTable = new ConcurrentHashMap<>();

    public RouteInfoManager() {
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            logger.info("RouteInfoManager starting...");

            // Start periodic cleanup task
            scheduledExecutorService.scheduleAtFixedRate(this::scanNotActiveBroker, 5, 10, TimeUnit.SECONDS);

            logger.info("RouteInfoManager started successfully");
        }
    }

    public void shutdown() {
        logger.info("RouteInfoManager shutting down...");

        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdown();
        }

        logger.info("RouteInfoManager shut down successfully");
    }

    public void registerBroker(String clusterName, String brokerAddr, String brokerName, long brokerId,
                               String haServerAddr, TopicConfigSerializeWrapper topicConfigWrapper,
                               List<String> filterServerList) {

        try {
            logger.info("Register broker: cluster={}, brokerAddr={}, brokerName={}, brokerId={}",
                    clusterName, brokerAddr, brokerName, brokerId);

            // Update cluster info
            Set<String> brokerNames = clusterAddrTable.computeIfAbsent(clusterName, k -> new HashSet<>());
            brokerNames.add(brokerName);

            // Update broker address info
            BrokerData brokerData = brokerAddrTable.get(brokerName);
            if (brokerData == null) {
                brokerData = new BrokerData();
                brokerData.setBrokerName(brokerName);
                brokerData.setCluster(clusterName);
                brokerAddrTable.put(brokerName, brokerData);
            }
            brokerData.getBrokerAddrs().put(brokerId, brokerAddr);

            // Update broker live info
            BrokerLiveInfo brokerLiveInfo = new BrokerLiveInfo();
            brokerLiveInfo.setLastUpdateTimestamp(System.currentTimeMillis());
            brokerLiveInfo.setDataVersion(1L);
            brokerLiveInfo.setHaServerAddr(haServerAddr);
            brokerLiveTable.put(brokerAddr, brokerLiveInfo);

            // Update filter server info
            if (filterServerList != null && !filterServerList.isEmpty()) {
                filterServerTable.put(brokerAddr, filterServerList);
            }

            // Update topic config
            if (topicConfigWrapper != null && topicConfigWrapper.getTopicConfigTable() != null) {
                updateTopicConfigFromBroker(brokerName, topicConfigWrapper.getTopicConfigTable());
            }

            logger.info("Broker registered successfully: {}", brokerName);

        } catch (Exception e) {
            logger.error("Failed to register broker: " + brokerName, e);
        }
    }

    public void unregisterBroker(String clusterName, String brokerAddr, String brokerName, long brokerId) {
        logger.info("Unregister broker: cluster={}, brokerAddr={}, brokerName={}, brokerId={}",
                clusterName, brokerAddr, brokerName, brokerId);

        // Remove from broker live table
        brokerLiveTable.remove(brokerAddr);

        // Remove from filter server table
        filterServerTable.remove(brokerAddr);

        // Remove broker address
        BrokerData brokerData = brokerAddrTable.get(brokerName);
        if (brokerData != null) {
            brokerData.getBrokerAddrs().remove(brokerId);
            if (brokerData.getBrokerAddrs().isEmpty()) {
                brokerAddrTable.remove(brokerName);

                // Remove from cluster
                Set<String> brokerNames = clusterAddrTable.get(clusterName);
                if (brokerNames != null) {
                    brokerNames.remove(brokerName);
                    if (brokerNames.isEmpty()) {
                        clusterAddrTable.remove(clusterName);
                    }
                }

                // Remove related topic queue data
                removeTopicByBrokerName(brokerName);
            }
        }

        logger.info("Broker unregistered successfully: {}", brokerName);
    }

    public TopicRouteData pickupTopicRouteData(String topic) {
        TopicRouteData topicRouteData = new TopicRouteData();
        boolean foundQueueData = false;
        boolean foundBrokerData = false;

        List<QueueData> queueDataList = topicQueueTable.get(topic);
        if (queueDataList != null && !queueDataList.isEmpty()) {
            topicRouteData.setQueueDatas(new ArrayList<>(queueDataList));
            foundQueueData = true;

            Set<String> brokerNameSet = new HashSet<>();
            for (QueueData queueData : queueDataList) {
                brokerNameSet.add(queueData.getBrokerName());
            }

            List<BrokerData> brokerDataList = new ArrayList<>();
            for (String brokerName : brokerNameSet) {
                BrokerData brokerData = brokerAddrTable.get(brokerName);
                if (brokerData != null) {
                    brokerDataList.add(brokerData.clone());
                    foundBrokerData = true;
                }
            }
            topicRouteData.setBrokerDatas(brokerDataList);
        }

        if (foundQueueData && foundBrokerData) {
            return topicRouteData;
        }

        return null;
    }

    public ClusterInfo getAllClusterInfo() {
        ClusterInfo clusterInfoSerializeWrapper = new ClusterInfo();
        clusterInfoSerializeWrapper.setBrokerAddrTable(new HashMap<>(brokerAddrTable));
        clusterInfoSerializeWrapper.setClusterAddrTable(new HashMap<>(clusterAddrTable));
        return clusterInfoSerializeWrapper;
    }

    private void updateTopicConfigFromBroker(String brokerName, Map<String, TopicConfig> topicConfigTable) {
        for (Map.Entry<String, TopicConfig> entry : topicConfigTable.entrySet()) {
            String topic = entry.getKey();
            TopicConfig topicConfig = entry.getValue();

            QueueData queueData = new QueueData();
            queueData.setBrokerName(brokerName);
            queueData.setReadQueueNums(topicConfig.getReadQueueNums());
            queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
            queueData.setPerm(topicConfig.getPerm());
            queueData.setTopicSynFlag(topicConfig.getTopicSysFlag());

            List<QueueData> queueDataList = this.topicQueueTable.computeIfAbsent(topic, k -> new ArrayList<>());

            // Remove old queue data for this broker
            queueDataList.removeIf(qd -> qd.getBrokerName().equals(brokerName));

            // Add new queue data
            queueDataList.add(queueData);

            logger.debug("Updated topic config: topic={}, brokerName={}", topic, brokerName);
        }
    }

    private void removeTopicByBrokerName(String brokerName) {
        Iterator<Map.Entry<String, List<QueueData>>> itMap = topicQueueTable.entrySet().iterator();
        while (itMap.hasNext()) {
            Map.Entry<String, List<QueueData>> entry = itMap.next();
            List<QueueData> queueDataList = entry.getValue();
            Iterator<QueueData> it = queueDataList.iterator();
            while (it.hasNext()) {
                QueueData queueData = it.next();
                if (queueData.getBrokerName().equals(brokerName)) {
                    it.remove();
                    logger.info("Remove topic queue data: topic={}, brokerName={}", entry.getKey(), brokerName);
                }
            }

            if (queueDataList.isEmpty()) {
                itMap.remove();
                logger.info("Remove topic completely: topic={}", entry.getKey());
            }
        }
    }

    private void scanNotActiveBroker() {
        Iterator<Map.Entry<String, BrokerLiveInfo>> it = brokerLiveTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, BrokerLiveInfo> next = it.next();
            long last = next.getValue().getLastUpdateTimestamp();
            if (System.currentTimeMillis() - last > 120000) { // 2 minutes timeout
                it.remove();
                logger.warn("The broker channel expired, {} {}ms", next.getKey(), System.currentTimeMillis() - last);
                onChannelDestroy(next.getKey());
            }
        }
    }

    private void onChannelDestroy(String brokerAddr) {
        BrokerLiveInfo brokerLiveInfo = brokerLiveTable.remove(brokerAddr);
        if (brokerLiveInfo != null) {
            logger.info("The broker's channel destroyed, {}, clean it's route info", brokerAddr);
            // Clean up related route info
        }
    }

    // Inner classes
    public static class QueueData implements Cloneable {
        private String brokerName;
        private int readQueueNums;
        private int writeQueueNums;
        private int perm;
        private int topicSynFlag;

        // Getters and setters
        public String getBrokerName() { return brokerName; }
        public void setBrokerName(String brokerName) { this.brokerName = brokerName; }
        public int getReadQueueNums() { return readQueueNums; }
        public void setReadQueueNums(int readQueueNums) { this.readQueueNums = readQueueNums; }
        public int getWriteQueueNums() { return writeQueueNums; }
        public void setWriteQueueNums(int writeQueueNums) { this.writeQueueNums = writeQueueNums; }
        public int getPerm() { return perm; }
        public void setPerm(int perm) { this.perm = perm; }
        public int getTopicSynFlag() { return topicSynFlag; }
        public void setTopicSynFlag(int topicSynFlag) { this.topicSynFlag = topicSynFlag; }

        @Override
        public QueueData clone() {
            try {
                return (QueueData) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class BrokerData implements Cloneable {
        private String cluster;
        private String brokerName;
        private Map<Long, String> brokerAddrs = new HashMap<>();

        public String getCluster() { return cluster; }
        public void setCluster(String cluster) { this.cluster = cluster; }
        public String getBrokerName() { return brokerName; }
        public void setBrokerName(String brokerName) { this.brokerName = brokerName; }
        public Map<Long, String> getBrokerAddrs() { return brokerAddrs; }
        public void setBrokerAddrs(Map<Long, String> brokerAddrs) { this.brokerAddrs = brokerAddrs; }

        @Override
        public BrokerData clone() {
            try {
                BrokerData clone = (BrokerData) super.clone();
                clone.brokerAddrs = new HashMap<>(this.brokerAddrs);
                return clone;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class BrokerLiveInfo {
        private long lastUpdateTimestamp;
        private long dataVersion;
        private String haServerAddr;

        public long getLastUpdateTimestamp() { return lastUpdateTimestamp; }
        public void setLastUpdateTimestamp(long lastUpdateTimestamp) { this.lastUpdateTimestamp = lastUpdateTimestamp; }
        public long getDataVersion() { return dataVersion; }
        public void setDataVersion(long dataVersion) { this.dataVersion = dataVersion; }
        public String getHaServerAddr() { return haServerAddr; }
        public void setHaServerAddr(String haServerAddr) { this.haServerAddr = haServerAddr; }
    }

    public static class TopicRouteData {
        private String orderTopicConf;
        private List<QueueData> queueDatas;
        private List<BrokerData> brokerDatas;
        private HashMap<String, List<String>> filterServerTable;

        public String getOrderTopicConf() { return orderTopicConf; }
        public void setOrderTopicConf(String orderTopicConf) { this.orderTopicConf = orderTopicConf; }
        public List<QueueData> getQueueDatas() { return queueDatas; }
        public void setQueueDatas(List<QueueData> queueDatas) { this.queueDatas = queueDatas; }
        public List<BrokerData> getBrokerDatas() { return brokerDatas; }
        public void setBrokerDatas(List<BrokerData> brokerDatas) { this.brokerDatas = brokerDatas; }
        public HashMap<String, List<String>> getFilterServerTable() { return filterServerTable; }
        public void setFilterServerTable(HashMap<String, List<String>> filterServerTable) { this.filterServerTable = filterServerTable; }
    }

    public static class ClusterInfo {
        private HashMap<String, BrokerData> brokerAddrTable;
        private HashMap<String, Set<String>> clusterAddrTable;

        public HashMap<String, BrokerData> getBrokerAddrTable() { return brokerAddrTable; }
        public void setBrokerAddrTable(HashMap<String, BrokerData> brokerAddrTable) { this.brokerAddrTable = brokerAddrTable; }
        public HashMap<String, Set<String>> getClusterAddrTable() { return clusterAddrTable; }
        public void setClusterAddrTable(HashMap<String, Set<String>> clusterAddrTable) { this.clusterAddrTable = clusterAddrTable; }
    }

    public static class TopicConfigSerializeWrapper {
        private Map<String, TopicConfig> topicConfigTable;

        public Map<String, TopicConfig> getTopicConfigTable() { return topicConfigTable; }
        public void setTopicConfigTable(Map<String, TopicConfig> topicConfigTable) { this.topicConfigTable = topicConfigTable; }
    }

    public static class TopicConfig {
        private String topicName;
        private int readQueueNums;
        private int writeQueueNums;
        private int perm;
        private int topicSysFlag;

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
}