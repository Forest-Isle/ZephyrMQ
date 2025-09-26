package com.zephyr.broker.transaction;

import com.zephyr.protocol.transaction.TransactionMessage;
import com.zephyr.protocol.transaction.TransactionState;
import com.zephyr.protocol.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.UUID;
import java.util.List;

/**
 * Transaction Manager
 * Handles transactional message lifecycle using simplified 2PC protocol
 */
public class TransactionManager {

    private static final Logger logger = LoggerFactory.getLogger(TransactionManager.class);

    // Active transactions: transactionId -> TransactionMessage
    private final ConcurrentMap<String, TransactionMessage> activeTransactions = new ConcurrentHashMap<>();

    // Message storage for prepared messages: msgId -> Message
    private final ConcurrentMap<String, Message> preparedMessages = new ConcurrentHashMap<>();

    // Configuration
    private final long transactionTimeoutMs;
    private final int maxCheckTimes;
    private final long checkIntervalMs;

    // Executor for background tasks
    private final ScheduledExecutorService executor;

    public TransactionManager() {
        this(60000, 15, 6000); // 1 min timeout, 15 max checks, 6s interval
    }

    public TransactionManager(long transactionTimeoutMs, int maxCheckTimes, long checkIntervalMs) {
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.maxCheckTimes = maxCheckTimes;
        this.checkIntervalMs = checkIntervalMs;
        this.executor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "TransactionManager");
            t.setDaemon(true);
            return t;
        });

        startBackgroundTasks();
    }

    /**
     * Start a new transaction
     *
     * @param msgId message ID
     * @param topic topic name
     * @param producerGroup producer group
     * @param producerId producer ID
     * @return transaction ID
     */
    public String beginTransaction(String msgId, String topic, String producerGroup, String producerId) {
        String transactionId = generateTransactionId();

        TransactionMessage txMsg = new TransactionMessage(transactionId, msgId, topic,
                                                        producerGroup, producerId);
        txMsg.setTimeoutTime(System.currentTimeMillis() + transactionTimeoutMs);

        activeTransactions.put(transactionId, txMsg);

        logger.info("Transaction started: {}", transactionId);
        return transactionId;
    }

    /**
     * Prepare a message for transaction
     *
     * @param transactionId transaction ID
     * @param message message to prepare
     * @return true if prepared successfully
     */
    public boolean prepareMessage(String transactionId, Message message) {
        TransactionMessage txMsg = activeTransactions.get(transactionId);
        if (txMsg == null) {
            logger.warn("Transaction not found: {}", transactionId);
            return false;
        }

        if (txMsg.getState() != TransactionState.PREPARE) {
            logger.warn("Invalid transaction state for prepare: {} state: {}",
                       transactionId, txMsg.getState());
            return false;
        }

        // Store prepared message
        preparedMessages.put(txMsg.getMsgId(), message);

        logger.debug("Message prepared for transaction: {}", transactionId);
        return true;
    }

    /**
     * Commit a transaction
     *
     * @param transactionId transaction ID
     * @return true if committed successfully
     */
    public boolean commitTransaction(String transactionId) {
        TransactionMessage txMsg = activeTransactions.get(transactionId);
        if (txMsg == null) {
            logger.warn("Transaction not found for commit: {}", transactionId);
            return false;
        }

        if (txMsg.getState() != TransactionState.PREPARE) {
            logger.warn("Invalid transaction state for commit: {} state: {}",
                       transactionId, txMsg.getState());
            return false;
        }

        // Update transaction state
        txMsg.setState(TransactionState.COMMIT);
        txMsg.setCommitTime(System.currentTimeMillis());

        // Get prepared message and make it available for consumption
        Message message = preparedMessages.remove(txMsg.getMsgId());
        if (message != null) {
            // TODO: Integrate with message store to make message consumable
            logger.info("Transaction committed: {}, message available for consumption", transactionId);
        } else {
            logger.warn("No prepared message found for transaction: {}", transactionId);
        }

        // Remove from active transactions
        activeTransactions.remove(transactionId);

        return true;
    }

    /**
     * Rollback a transaction
     *
     * @param transactionId transaction ID
     * @return true if rolled back successfully
     */
    public boolean rollbackTransaction(String transactionId) {
        TransactionMessage txMsg = activeTransactions.remove(transactionId);
        if (txMsg == null) {
            logger.warn("Transaction not found for rollback: {}", transactionId);
            return false;
        }

        // Update transaction state
        txMsg.setState(TransactionState.ROLLBACK);

        // Remove prepared message
        Message message = preparedMessages.remove(txMsg.getMsgId());
        if (message != null) {
            logger.info("Transaction rolled back: {}, message discarded", transactionId);
        } else {
            logger.warn("No prepared message found for rollback transaction: {}", transactionId);
        }

        return true;
    }

    /**
     * Check transaction state with producer
     *
     * @param transactionId transaction ID
     * @return current transaction state
     */
    public TransactionState checkTransactionState(String transactionId) {
        TransactionMessage txMsg = activeTransactions.get(transactionId);
        if (txMsg == null) {
            return TransactionState.UNKNOWN;
        }

        // Increment check times
        txMsg.incrementCheckTimes();

        // TODO: Implement actual check with producer
        // For now, return current state
        logger.debug("Checking transaction state: {} (check times: {})",
                    transactionId, txMsg.getCheckTimes());

        return txMsg.getState();
    }

    /**
     * Get transaction information
     *
     * @param transactionId transaction ID
     * @return transaction message or null if not found
     */
    public TransactionMessage getTransaction(String transactionId) {
        return activeTransactions.get(transactionId);
    }

    /**
     * Get count of active transactions
     *
     * @return number of active transactions
     */
    public int getActiveTransactionCount() {
        return activeTransactions.size();
    }

    /**
     * Get count of prepared messages
     *
     * @return number of prepared messages
     */
    public int getPreparedMessageCount() {
        return preparedMessages.size();
    }

    /**
     * Shutdown the transaction manager
     */
    public void shutdown() {
        logger.info("Shutting down TransactionManager...");

        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("TransactionManager shut down successfully");
    }

    private void startBackgroundTasks() {
        // Check for expired transactions
        executor.scheduleAtFixedRate(this::checkExpiredTransactions,
                                   checkIntervalMs, checkIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void checkExpiredTransactions() {
        long currentTime = System.currentTimeMillis();

        List<String> expiredTransactions = activeTransactions.entrySet().stream()
                .filter(entry -> entry.getValue().isExpired())
                .map(entry -> entry.getKey())
                .toList();

        for (String transactionId : expiredTransactions) {
            TransactionMessage txMsg = activeTransactions.get(transactionId);
            if (txMsg != null) {
                if (txMsg.getCheckTimes() < maxCheckTimes) {
                    // Try to check transaction state with producer
                    checkTransactionState(transactionId);
                } else {
                    // Max check times reached, rollback transaction
                    logger.warn("Transaction expired and max check times reached, rolling back: {}",
                               transactionId);
                    rollbackTransaction(transactionId);
                }
            }
        }
    }

    private String generateTransactionId() {
        return "TX_" + System.currentTimeMillis() + "_" + UUID.randomUUID().toString().substring(0, 8);
    }
}