package com.zephyr.protocol.transaction;

/**
 * Transaction state for transactional messages
 */
public enum TransactionState {
    /**
     * Transaction is preparing - message sent but not committed
     */
    PREPARE,

    /**
     * Transaction committed - message should be delivered
     */
    COMMIT,

    /**
     * Transaction rolled back - message should be discarded
     */
    ROLLBACK,

    /**
     * Transaction state unknown - need to check with producer
     */
    UNKNOWN
}