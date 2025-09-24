package com.zephyr.protocol.network;

public class ResponseCode {

    public static final int SUCCESS = 0;

    public static final int SYSTEM_ERROR = 1;
    public static final int SYSTEM_BUSY = 2;
    public static final int REQUEST_CODE_NOT_SUPPORTED = 3;
    public static final int TRANSACTION_FAILED = 4;

    public static final int FLUSH_DISK_TIMEOUT = 10;
    public static final int SLAVE_NOT_AVAILABLE = 11;
    public static final int FLUSH_SLAVE_TIMEOUT = 12;
    public static final int MESSAGE_ILLEGAL = 13;
    public static final int SERVICE_NOT_AVAILABLE = 14;
    public static final int VERSION_NOT_SUPPORTED = 15;
    public static final int NO_PERMISSION = 16;
    public static final int TOPIC_NOT_EXIST = 17;
    public static final int TOPIC_EXIST_ALREADY = 18;
    public static final int PULL_NOT_FOUND = 19;
    public static final int PULL_RETRY_IMMEDIATELY = 20;
    public static final int PULL_OFFSET_MOVED = 21;

    public static final int CONSUMER_NOT_ONLINE = 206;
    public static final int CONSUME_MSG_TIMEOUT = 207;

    public static final int NO_BUYER_ID = 204;
    public static final int NOT_IN_CURRENT_UNIT = 205;

    public static final int SUBSCRIPTION_PARSE_FAILED = 300;
    public static final int SUBSCRIPTION_NOT_EXIST = 301;
    public static final int SUBSCRIPTION_NOT_LATEST = 302;
    public static final int SUBSCRIPTION_GROUP_NOT_EXIST = 303;
}