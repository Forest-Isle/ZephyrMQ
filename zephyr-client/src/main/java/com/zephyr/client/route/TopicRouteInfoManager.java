package com.zephyr.client.route;

import com.zephyr.protocol.message.MessageQueue;
import com.zephyr.protocol.network.NettyRemotingClient;
import com.zephyr.protocol.network.RemotingCommand;
import com.zephyr.protocol.network.RequestCode;
import com.zephyr.protocol.network.ResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Client-side topic route information manager
 * Caches topic routing information and provides queue selection
 * Communicates with nameserver to fetch and maintain route information
 */
public class TopicRouteInfoManager {

    public TopicRouteInfoManager() {
        this.remotingClient = new NettyRemotingClient();
        this.scheduledExecutorService = Executors.newScheduledThreadPool(1,
            r -> new Thread(r, "TopicRouteInfoManager-UpdateThread"));
    }

    private static final Logger logger = LoggerFactory.getLogger(TopicRouteInfoManager.class);

    // Cache for topic route information
    private final ConcurrentMap<String, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();

    // Name server addresses
    private volatile List<String> nameServerAddresses = new ArrayList<>();

    // Broker address cache: brokerName -> Map<brokerId, brokerAddr>
    private final ConcurrentMap<String, Map<Long, String>> brokerAddrTable = new ConcurrentHashMap<>();

    // Read-write lock for route data update
    private final ReadWriteLock routeDataLock = new ReentrantReadWriteLock();

    // Netty client for communication with nameserver
    private final NettyRemotingClient remotingClient;

    // Scheduled executor for periodic route update
    private final ScheduledExecutorService scheduledExecutorService;

    // Started flag
    private final AtomicBoolean started = new AtomicBoolean(false);

    // Default broker and queue configuration
    private String defaultBrokerName = "defaultBroker";
    private int defaultQueueCount = 4;
    private long routeInfoUpdateInterval = 30000; // 30 seconds

    /**
     * Start the route info manager
     */
    public void start() {
        if (started.compareAndSet(false, true)) {
            remotingClient.start();

            // Start periodic route update task
            scheduledExecutorService.scheduleAtFixedRate(
                this::updateAllTopicRouteInfo,
                routeInfoUpdateInterval,
                routeInfoUpdateInterval,
                TimeUnit.MILLISECONDS
            );

            logger.info("TopicRouteInfoManager started");
        }
    }

    /**
     * Shutdown the route info manager
     */
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            if (scheduledExecutorService != null && !scheduledExecutorService.isShutdown()) {
                scheduledExecutorService.shutdown();
            }

            if (remotingClient != null) {
                remotingClient.shutdown();
            }

            logger.info("TopicRouteInfoManager shutdown");
        }
    }

    /**
     * Update name server address list
     *
     * @param nameServerAddresses name server address list, separated by semicolon
     */
    public void updateNameServerAddressList(String nameServerAddresses) {
        if (nameServerAddresses != null && !nameServerAddresses.trim().isEmpty()) {
            List<String> newAddresses = Arrays.asList(nameServerAddresses.split(";"));
            this.nameServerAddresses = newAddresses;
            logger.info("Updated name server addresses: {}", this.nameServerAddresses);
        }
    }

    /**
     * Update topic route information from name server
     *
     * @param topic topic name
     * @return true if successfully updated
     */
    public boolean updateTopicRouteInfoFromNameServer(String topic) {
        return updateTopicRouteInfoFromNameServer(topic, false);
    }

    /**
     * Update topic route information from name server
     *
     * @param topic topic name
     * @param isDefault whether this is the default topic
     * @return true if successfully updated
     */
    public boolean updateTopicRouteInfoFromNameServer(String topic, boolean isDefault) {
        try {
            routeDataLock.writeLock().lock();

            TopicRouteData newRouteData = getTopicRouteDataFromNameServer(topic);
            if (newRouteData != null) {
                TopicRouteData oldRouteData = topicRouteTable.get(topic);
                boolean changed = topicRouteDataChanged(oldRouteData, newRouteData);

                if (changed || oldRouteData == null) {
                    // Update broker address table
                    updateBrokerAddrTable(newRouteData);

                    // Update route data
                    topicRouteTable.put(topic, newRouteData);

                    logger.info("Update topic route info success: topic={}, changed={}", topic, changed);
                    return true;
                }
            } else {
                // If failed to get from nameserver, create default if allowed
                if (!isDefault && topicRouteTable.get(topic) == null) {
                    TopicRouteData defaultRouteData = createDefaultRouteData(topic);
                    topicRouteTable.put(topic, defaultRouteData);
                    logger.info("Created default route data for topic: {}", topic);
                    return true;
                }
            }
        } catch (Exception e) {
            logger.error("Update topic route info from nameserver failed, topic: {}", topic, e);
        } finally {
            routeDataLock.writeLock().unlock();
        }

        return false;
    }

    /**
     * Fetch subscribe message queues for topic
     *
     * @param topic topic name
     * @return list of message queues for subscribing
     */
    public List<MessageQueue> fetchSubscribeMessageQueues(String topic) {
        updateTopicRouteInfoFromNameServer(topic);
        return getSubscribeMessageQueues(topic);
    }

    /**
     * Find broker address for subscribe operation
     *
     * @param brokerName broker name
     * @param brokerId broker id
     * @param onlyThisBroker only return this broker
     * @return broker address
     */
    public String findBrokerAddressInSubscribe(String brokerName, long brokerId, boolean onlyThisBroker) {
        routeDataLock.readLock().lock();
        try {
            Map<Long, String> brokerMap = brokerAddrTable.get(brokerName);
            if (brokerMap != null && !brokerMap.isEmpty()) {
                // Try to find the specific broker id first
                String brokerAddr = brokerMap.get(brokerId);
                if (brokerAddr != null) {
                    return brokerAddr;
                }

                // If onlyThisBroker is false, try to find slave broker
                if (!onlyThisBroker) {
                    // Try to find any available broker (prefer master, then slave)
                    brokerAddr = brokerMap.get(0L); // Master broker
                    if (brokerAddr != null) {
                        return brokerAddr;
                    }

                    // Return any available broker
                    for (String addr : brokerMap.values()) {
                        if (addr != null) {
                            return addr;
                        }
                    }
                }
            }

            // If not found, use default address
            logger.warn("Cannot find broker address for brokerName: {}, brokerId: {}, using default",
                brokerName, brokerId);
            return "127.0.0.1:10911";

        } finally {
            routeDataLock.readLock().unlock();
        }
    }

    /**
     * Find broker address for publish operation
     *
     * @param brokerName broker name
     * @return broker address
     */
    public String findBrokerAddressInPublish(String brokerName) {
        routeDataLock.readLock().lock();
        try {
            Map<Long, String> brokerMap = brokerAddrTable.get(brokerName);
            if (brokerMap != null && !brokerMap.isEmpty()) {
                // For publish, prefer master broker (brokerId = 0)
                String masterAddr = brokerMap.get(0L);
                if (masterAddr != null) {
                    return masterAddr;
                }

                // If no master, return any available broker
                for (String addr : brokerMap.values()) {
                    if (addr != null) {
                        return addr;
                    }
                }
            }

            // If not found, use default address
            logger.warn("Cannot find broker address for publish brokerName: {}, using default", brokerName);
            return "127.0.0.1:10911";

        } finally {
            routeDataLock.readLock().unlock();
        }
    }

    /**
     * Get topic route data from nameserver
     *
     * @param topic topic name
     * @return topic route data from nameserver
     */
    private TopicRouteData getTopicRouteDataFromNameServer(String topic) {
        if (nameServerAddresses.isEmpty()) {
            logger.warn("No nameserver addresses configured for topic: {}", topic);
            return null;
        }

        for (String nameServerAddr : nameServerAddresses) {
            try {
                GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
                requestHeader.setTopic(topic);

                RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINTO_BY_TOPIC);
                request.setCustomHeader(requestHeader);

                RemotingCommand response = remotingClient.invokeSync(nameServerAddr, request, 3000);

                if (response.getCode() == ResponseCode.SUCCESS) {
                    TopicRouteData routeData = deserializeTopicRouteData(response.getBody());
                    if (routeData != null) {
                        logger.debug("Get topic route info from nameserver success: topic={}, nameserver={}",
                            topic, nameServerAddr);
                        return routeData;
                    }
                } else {
                    logger.warn("Get topic route info from nameserver failed: topic={}, nameserver={}, code={}, remark={}",
                        topic, nameServerAddr, response.getCode(), response.getRemark());
                }

            } catch (Exception e) {
                logger.error("Get topic route info from nameserver exception: topic={}, nameserver={}",
                    topic, nameServerAddr, e);
            }
        }

        return null;
    }

    /**
     * Update broker address table from route data
     *
     * @param routeData topic route data
     */
    private void updateBrokerAddrTable(TopicRouteData routeData) {
        if (routeData.getBrokerDatas() != null) {
            for (BrokerData brokerData : routeData.getBrokerDatas()) {
                brokerAddrTable.put(brokerData.getBrokerName(), brokerData.getBrokerAddrs());
            }
        }
    }

    /**
     * Check if topic route data changed
     *
     * @param oldRouteData old route data
     * @param newRouteData new route data
     * @return true if changed
     */
    private boolean topicRouteDataChanged(TopicRouteData oldRouteData, TopicRouteData newRouteData) {
        if (oldRouteData == null || newRouteData == null) {
            return true;
        }

        if (!Objects.equals(oldRouteData.getOrderTopicConf(), newRouteData.getOrderTopicConf())) {
            return true;
        }

        // Compare queue data
        List<QueueData> oldQueueData = oldRouteData.getQueueDatas();
        List<QueueData> newQueueData = newRouteData.getQueueDatas();
        if (!Objects.equals(oldQueueData, newQueueData)) {
            return true;
        }

        // Compare broker data
        List<BrokerData> oldBrokerData = oldRouteData.getBrokerDatas();
        List<BrokerData> newBrokerData = newRouteData.getBrokerDatas();
        return !Objects.equals(oldBrokerData, newBrokerData);
    }

    /**
     * Update all topic route info periodically
     */
    private void updateAllTopicRouteInfo() {
        if (!started.get()) {
            return;
        }

        try {
            routeDataLock.readLock().lock();
            Set<String> topicSet = new HashSet<>(topicRouteTable.keySet());
            routeDataLock.readLock().unlock();

            for (String topic : topicSet) {
                updateTopicRouteInfoFromNameServer(topic, false);
            }

        } catch (Exception e) {
            logger.error("Update all topic route info exception", e);
        }
    }

    /**
     * Deserialize topic route data from bytes
     *
     * @param data serialized data
     * @return topic route data
     */
    private TopicRouteData deserializeTopicRouteData(byte[] data) {
        // Simplified deserialization - in production should use proper serialization
        if (data == null || data.length == 0) {
            return null;
        }

        try {
            // For now, return a simple TopicRouteData
            // In production, this would properly deserialize the data
            TopicRouteData routeData = new TopicRouteData();
            String content = new String(data);

            // Parse simple format: topic:brokerName:readQueues:writeQueues
            String[] parts = content.split(":");
            if (parts.length >= 4) {
                String brokerName = parts[1];
                int readQueueNums = Integer.parseInt(parts[2]);
                int writeQueueNums = Integer.parseInt(parts[3]);

                // Create queue data
                QueueData queueData = new QueueData();
                queueData.setBrokerName(brokerName);
                queueData.setReadQueueNums(readQueueNums);
                queueData.setWriteQueueNums(writeQueueNums);
                queueData.setPerm(6); // Default permission
                queueData.setTopicSynFlag(0);

                routeData.setQueueDatas(Collections.singletonList(queueData));

                // Create broker data
                BrokerData brokerData = new BrokerData();
                brokerData.setBrokerName(brokerName);
                brokerData.setCluster("defaultCluster");
                Map<Long, String> brokerAddrs = new HashMap<>();
                brokerAddrs.put(0L, "127.0.0.1:10911"); // Default master address
                brokerData.setBrokerAddrs(brokerAddrs);

                routeData.setBrokerDatas(Collections.singletonList(brokerData));
            }

            return routeData;

        } catch (Exception e) {
            logger.error("Deserialize topic route data failed", e);
            return null;
        }
    }

    /**
     * Get or create topic route data
     *
     * @param topic topic name
     * @return topic route data
     */
    public TopicRouteData getTopicRouteData(String topic) {
        return getTopicRouteData(topic, true);
    }

    /**
     * Get topic route data
     *
     * @param topic topic name
     * @param createIfAbsent whether to create if not exists
     * @return topic route data
     */
    public TopicRouteData getTopicRouteData(String topic, boolean createIfAbsent) {
        if (topic == null || topic.isEmpty()) {
            return null;
        }

        TopicRouteData routeData = topicRouteTable.get(topic);
        if (routeData == null && createIfAbsent) {
            routeData = createDefaultRouteData(topic);
            topicRouteTable.put(topic, routeData);
            logger.info("Created default route data for topic: {}", topic);
        }

        return routeData;
    }

    /**
     * Update topic route data from nameserver
     *
     * @param topic topic name
     * @param routeData route data from nameserver
     */
    public void updateTopicRouteData(String topic, TopicRouteData routeData) {
        if (topic != null && routeData != null) {
            routeDataLock.writeLock().lock();
            try {
                topicRouteTable.put(topic, routeData);
                updateBrokerAddrTable(routeData);
                logger.debug("Updated route data for topic: {} with {} queue data entries",
                            topic, routeData.getQueueDatas() != null ? routeData.getQueueDatas().size() : 0);
            } finally {
                routeDataLock.writeLock().unlock();
            }
        }
    }

    /**
     * Get message queues for publish
     *
     * @param topic topic name
     * @return list of message queues for publishing
     */
    public List<MessageQueue> getPublishMessageQueues(String topic) {
        TopicRouteData routeData = getTopicRouteData(topic);
        if (routeData == null) {
            return new ArrayList<>();
        }

        return convertQueueDataToMessageQueues(topic, routeData.getQueueDatas(), true);
    }

    /**
     * Get message queues for subscribe
     *
     * @param topic topic name
     * @return list of message queues for subscribing
     */
    public List<MessageQueue> getSubscribeMessageQueues(String topic) {
        TopicRouteData routeData = getTopicRouteData(topic);
        if (routeData == null) {
            return new ArrayList<>();
        }

        return convertQueueDataToMessageQueues(topic, routeData.getQueueDatas(), false);
    }

    /**
     * Convert QueueData to MessageQueue list
     *
     * @param topic topic name
     * @param queueDatas queue data list
     * @param forWrite true for write queues, false for read queues
     * @return message queue list
     */
    private List<MessageQueue> convertQueueDataToMessageQueues(String topic, List<QueueData> queueDatas, boolean forWrite) {
        List<MessageQueue> messageQueues = new ArrayList<>();

        if (queueDatas != null) {
            for (QueueData queueData : queueDatas) {
                int queueCount = forWrite ? queueData.getWriteQueueNums() : queueData.getReadQueueNums();

                for (int i = 0; i < queueCount; i++) {
                    MessageQueue mq = new MessageQueue(topic, queueData.getBrokerName(), i);
                    mq.setWritable(forWrite && (queueData.getPerm() & 2) != 0); // Write permission
                    mq.setReadable(!forWrite && (queueData.getPerm() & 4) != 0); // Read permission
                    messageQueues.add(mq);
                }
            }
        }

        return messageQueues;
    }

    /**
     * Clear cached route data for topic
     *
     * @param topic topic name
     */
    public void clearTopicRouteData(String topic) {
        topicRouteTable.remove(topic);
        logger.info("Cleared route data for topic: {}", topic);
    }

    /**
     * Clear all cached route data
     */
    public void clearAllRouteData() {
        topicRouteTable.clear();
        logger.info("Cleared all route data");
    }

    /**
     * Get cached topic count
     *
     * @return number of cached topics
     */
    public int getCachedTopicCount() {
        return topicRouteTable.size();
    }

    /**
     * Create default route data for topic
     *
     * @param topic topic name
     * @return default route data
     */
    private TopicRouteData createDefaultRouteData(String topic) {
        TopicRouteData routeData = new TopicRouteData();

        // Create default queue data
        QueueData queueData = new QueueData();
        queueData.setBrokerName(defaultBrokerName);
        queueData.setReadQueueNums(defaultQueueCount);
        queueData.setWriteQueueNums(defaultQueueCount);
        queueData.setPerm(6); // Read and write permission (2 + 4)
        queueData.setTopicSynFlag(0);

        routeData.setQueueDatas(Collections.singletonList(queueData));

        // Create default broker data
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName(defaultBrokerName);
        brokerData.setCluster("defaultCluster");
        Map<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(0L, "127.0.0.1:10911"); // Default master address
        brokerData.setBrokerAddrs(brokerAddrs);

        routeData.setBrokerDatas(Collections.singletonList(brokerData));

        return routeData;
    }

    // Getters and setters
    public String getDefaultBrokerName() {
        return defaultBrokerName;
    }

    public void setDefaultBrokerName(String defaultBrokerName) {
        this.defaultBrokerName = defaultBrokerName;
    }

    public int getDefaultQueueCount() {
        return defaultQueueCount;
    }

    public void setDefaultQueueCount(int defaultQueueCount) {
        this.defaultQueueCount = defaultQueueCount;
    }

    public long getRouteInfoUpdateInterval() {
        return routeInfoUpdateInterval;
    }

    public void setRouteInfoUpdateInterval(long routeInfoUpdateInterval) {
        this.routeInfoUpdateInterval = routeInfoUpdateInterval;
    }

    // Data structures for nameserver communication
    public static class GetRouteInfoRequestHeader {
        private String topic;

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
    }

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
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            QueueData queueData = (QueueData) o;
            return readQueueNums == queueData.readQueueNums &&
                    writeQueueNums == queueData.writeQueueNums &&
                    perm == queueData.perm &&
                    topicSynFlag == queueData.topicSynFlag &&
                    Objects.equals(brokerName, queueData.brokerName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(brokerName, readQueueNums, writeQueueNums, perm, topicSynFlag);
        }

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
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BrokerData that = (BrokerData) o;
            return Objects.equals(cluster, that.cluster) &&
                    Objects.equals(brokerName, that.brokerName) &&
                    Objects.equals(brokerAddrs, that.brokerAddrs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cluster, brokerName, brokerAddrs);
        }

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

    /**
     * Topic route data - full version with nameserver data structures
     */
    public static class TopicRouteData {
        private String orderTopicConf;
        private List<QueueData> queueDatas;
        private List<BrokerData> brokerDatas;
        private Map<String, List<String>> filterServerTable;

        public String getOrderTopicConf() { return orderTopicConf; }
        public void setOrderTopicConf(String orderTopicConf) { this.orderTopicConf = orderTopicConf; }
        public List<QueueData> getQueueDatas() { return queueDatas; }
        public void setQueueDatas(List<QueueData> queueDatas) { this.queueDatas = queueDatas; }
        public List<BrokerData> getBrokerDatas() { return brokerDatas; }
        public void setBrokerDatas(List<BrokerData> brokerDatas) { this.brokerDatas = brokerDatas; }
        public Map<String, List<String>> getFilterServerTable() { return filterServerTable; }
        public void setFilterServerTable(Map<String, List<String>> filterServerTable) { this.filterServerTable = filterServerTable; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TopicRouteData that = (TopicRouteData) o;
            return Objects.equals(orderTopicConf, that.orderTopicConf) &&
                    Objects.equals(queueDatas, that.queueDatas) &&
                    Objects.equals(brokerDatas, that.brokerDatas) &&
                    Objects.equals(filterServerTable, that.filterServerTable);
        }

        @Override
        public int hashCode() {
            return Objects.hash(orderTopicConf, queueDatas, brokerDatas, filterServerTable);
        }
    }
}