package com.zephyr.common.constant;

public final class ZephyrConstants {

    public static final String DEFAULT_CHARSET = "UTF-8";

    public static final int DEFAULT_BROKER_PORT = 9876;
    public static final int DEFAULT_NAMESERVER_PORT = 9877;
    public static final int DEFAULT_ADMIN_PORT = 8080;

    public static final int DEFAULT_SEND_TIMEOUT_MILLIS = 3000;
    public static final int DEFAULT_CONSUME_TIMEOUT_MILLIS = 15000;

    public static final int MAX_MESSAGE_SIZE = 1024 * 1024 * 4; // 4MB
    public static final int DEFAULT_MAX_BATCH_SIZE = 32;

    public static final String DEFAULT_TOPIC = "DefaultTopic";
    public static final String DEFAULT_CONSUMER_GROUP = "DefaultConsumerGroup";

    public static final int NETTY_BOSS_THREADS = 1;
    public static final int NETTY_WORKER_THREADS = Runtime.getRuntime().availableProcessors();
    public static final int NETTY_SO_BACKLOG = 1024;
    public static final boolean NETTY_SO_REUSEADDR = true;
    public static final boolean NETTY_TCP_NODELAY = true;
    public static final boolean NETTY_SO_KEEPALIVE = false;

    private ZephyrConstants() {
        throw new IllegalStateException("Utility class");
    }
}