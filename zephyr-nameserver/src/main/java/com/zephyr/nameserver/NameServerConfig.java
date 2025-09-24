package com.zephyr.nameserver;

public class NameServerConfig {

    private int listenPort = 9877;
    private String kvConfigPath = System.getProperty("user.home") + "/namesrv/kvConfig.json";
    private String configStorePath = System.getProperty("user.home") + "/namesrv/namesrv.properties";
    private String productEnvName = "center";
    private boolean clusterTest = false;
    private boolean orderMessageEnable = false;

    // Broker timeout settings
    private long brokerChannelExpiredTime = 120000; // 2 minutes
    private long scanNotActiveBrokerInterval = 5000; // 5 seconds

    // Clean resource settings
    private int deleteTopicOnBrokerTimeoutMillis = 60000; // 1 minute

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public String getKvConfigPath() {
        return kvConfigPath;
    }

    public void setKvConfigPath(String kvConfigPath) {
        this.kvConfigPath = kvConfigPath;
    }

    public String getConfigStorePath() {
        return configStorePath;
    }

    public void setConfigStorePath(String configStorePath) {
        this.configStorePath = configStorePath;
    }

    public String getProductEnvName() {
        return productEnvName;
    }

    public void setProductEnvName(String productEnvName) {
        this.productEnvName = productEnvName;
    }

    public boolean isClusterTest() {
        return clusterTest;
    }

    public void setClusterTest(boolean clusterTest) {
        this.clusterTest = clusterTest;
    }

    public boolean isOrderMessageEnable() {
        return orderMessageEnable;
    }

    public void setOrderMessageEnable(boolean orderMessageEnable) {
        this.orderMessageEnable = orderMessageEnable;
    }

    public long getBrokerChannelExpiredTime() {
        return brokerChannelExpiredTime;
    }

    public void setBrokerChannelExpiredTime(long brokerChannelExpiredTime) {
        this.brokerChannelExpiredTime = brokerChannelExpiredTime;
    }

    public long getScanNotActiveBrokerInterval() {
        return scanNotActiveBrokerInterval;
    }

    public void setScanNotActiveBrokerInterval(long scanNotActiveBrokerInterval) {
        this.scanNotActiveBrokerInterval = scanNotActiveBrokerInterval;
    }

    public int getDeleteTopicOnBrokerTimeoutMillis() {
        return deleteTopicOnBrokerTimeoutMillis;
    }

    public void setDeleteTopicOnBrokerTimeoutMillis(int deleteTopicOnBrokerTimeoutMillis) {
        this.deleteTopicOnBrokerTimeoutMillis = deleteTopicOnBrokerTimeoutMillis;
    }
}