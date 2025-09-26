package com.zephyr.broker.ack;

/**
 * Message acknowledgment mode
 */
public enum AckMode {
    /**
     * Automatic acknowledgment - messages are automatically acknowledged when delivered
     */
    AUTO_ACK,

    /**
     * Manual acknowledgment - consumer must explicitly acknowledge messages
     */
    MANUAL_ACK,

    /**
     * Batch acknowledgment - acknowledge multiple messages at once
     */
    BATCH_ACK
}