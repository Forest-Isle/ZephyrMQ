package com.zephyr.broker.transaction;

import com.zephyr.protocol.transaction.TransactionMessage;
import com.zephyr.protocol.transaction.TransactionState;
import com.zephyr.protocol.message.Message;
import com.zephyr.protocol.message.MessageExt;
import com.zephyr.broker.store.MessageStore;
import com.zephyr.storage.commitlog.CommitLog.PutMessageResult;
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

    // Message store reference
    private final MessageStore messageStore;

    // Configuration
    private final long transactionTimeoutMs;
    private final int maxCheckTimes;
    private final long checkIntervalMs;

    // Executor for background tasks
    private final ScheduledExecutorService executor;

    public TransactionManager(MessageStore messageStore) {
        this(messageStore, 60000, 15, 6000); // 1 min timeout, 15 max checks, 6s interval
    }

    public TransactionManager(MessageStore messageStore, long transactionTimeoutMs, int maxCheckTimes, long checkIntervalMs) {
        this.messageStore = messageStore;
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
            try {
                // Convert message to MessageExt and store it
                MessageExt messageExt = convertToMessageExt(message, txMsg);
                PutMessageResult result = messageStore.putMessage(messageExt);

                if (result != null && result.isOk()) {
                    logger.info("Transaction committed: {}, message available for consumption", transactionId);
                } else {
                    logger.error("Failed to store committed transaction message: {}, status: {}",
                        transactionId, result != null ? result.getStatus() : "null result");
                    // Re-add to prepared messages for potential retry
                    preparedMessages.put(txMsg.getMsgId(), message);
                    return false;
                }
            } catch (Exception e) {
                logger.error("Failed to commit transaction message: {}", transactionId, e);
                preparedMessages.put(txMsg.getMsgId(), message);
                return false;
            }
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

        // Send transaction check request to producer
        try {
            TransactionCheckResult checkResult = sendTransactionCheckRequest(txMsg);
            if (checkResult != null) {
                // Update state based on producer response
                if (checkResult.shouldCommit()) {
                    txMsg.setState(TransactionState.COMMIT);
                } else if (checkResult.shouldRollback()) {
                    txMsg.setState(TransactionState.ROLLBACK);
                }
                logger.debug("Transaction check completed: {} -> {}", transactionId, txMsg.getState());
            } else {
                logger.warn("Transaction check failed for: {}", transactionId);
            }
        } catch (Exception e) {
            logger.error("Failed to check transaction state: {}", transactionId, e);
        }

        logger.debug("Checking transaction state: {} (check times: {}, state: {})",
                    transactionId, txMsg.getCheckTimes(), txMsg.getState());

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

    private MessageExt convertToMessageExt(Message message, TransactionMessage txMsg) {
        MessageExt messageExt = new MessageExt();
        messageExt.setTopic(message.getTopic());
        messageExt.setBody(message.getBody());
        messageExt.setFlag(message.getFlag());
        messageExt.setTags(message.getTags());
        messageExt.setKeys(message.getKeys());
        messageExt.setProperties(message.getProperties());
        messageExt.setMsgId(txMsg.getMsgId());
        messageExt.setBornTimestamp(txMsg.getCreateTime());
        messageExt.setStoreTimestamp(System.currentTimeMillis());

        // Add transaction metadata
        if (messageExt.getProperties() == null) {
            messageExt.setProperties(new java.util.HashMap<>());
        }
        messageExt.getProperties().put("TRANSACTION_ID", txMsg.getTransactionId());
        messageExt.getProperties().put("TRANSACTION_STATE", txMsg.getState().toString());

        return messageExt;
    }

    private TransactionCheckResult sendTransactionCheckRequest(TransactionMessage txMsg) {
        try {
            // Mock implementation - in real scenario, this would send a network request
            // to the producer to check transaction status
            logger.debug("Sending transaction check request for: {}", txMsg.getTransactionId());

            // Simple logic: after 3 checks, randomly decide commit or rollback
            if (txMsg.getCheckTimes() > 3) {
                boolean shouldCommit = System.currentTimeMillis() % 2 == 0;
                return new TransactionCheckResult(shouldCommit, !shouldCommit);
            }

            return new TransactionCheckResult(false, false); // No decision yet
        } catch (Exception e) {
            logger.error("Failed to send transaction check request: {}", txMsg.getTransactionId(), e);
            return null;
        }
    }

    private static class TransactionCheckResult {
        private final boolean shouldCommit;
        private final boolean shouldRollback;

        public TransactionCheckResult(boolean shouldCommit, boolean shouldRollback) {
            this.shouldCommit = shouldCommit;
            this.shouldRollback = shouldRollback;
        }

        public boolean shouldCommit() {
            return shouldCommit;
        }

        public boolean shouldRollback() {
            return shouldRollback;
        }
    }
}