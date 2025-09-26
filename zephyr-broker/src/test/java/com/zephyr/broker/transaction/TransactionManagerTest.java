package com.zephyr.broker.transaction;

import com.zephyr.protocol.transaction.TransactionState;
import com.zephyr.protocol.message.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionManagerTest {

    private TransactionManager transactionManager;
    private Message testMessage;

    @BeforeEach
    void setUp() {
        transactionManager = new TransactionManager(5000, 3, 1000); // 5s timeout, 3 checks, 1s interval

        testMessage = new Message();
        testMessage.setTopic("testTopic");
        testMessage.setBody("test message".getBytes());
    }

    @AfterEach
    void tearDown() {
        transactionManager.shutdown();
    }

    @Test
    void testBeginTransaction() {
        String txId = transactionManager.beginTransaction("msg1", "testTopic", "producer1", "client1");

        assertNotNull(txId);
        assertTrue(txId.startsWith("TX_"));

        // Verify transaction is tracked
        assertEquals(1, transactionManager.getActiveTransactionCount());
        assertNotNull(transactionManager.getTransaction(txId));
    }

    @Test
    void testPrepareMessage() {
        String txId = transactionManager.beginTransaction("msg1", "testTopic", "producer1", "client1");

        // Prepare message
        assertTrue(transactionManager.prepareMessage(txId, testMessage));

        // Verify prepared message count
        assertEquals(1, transactionManager.getPreparedMessageCount());

        // Try to prepare with invalid transaction
        assertFalse(transactionManager.prepareMessage("invalid_tx", testMessage));
    }

    @Test
    void testCommitTransaction() {
        String txId = transactionManager.beginTransaction("msg1", "testTopic", "producer1", "client1");

        // Prepare and commit
        assertTrue(transactionManager.prepareMessage(txId, testMessage));
        assertTrue(transactionManager.commitTransaction(txId));

        // Verify transaction is removed from active transactions
        assertEquals(0, transactionManager.getActiveTransactionCount());
        assertEquals(0, transactionManager.getPreparedMessageCount());

        // Try to commit invalid transaction
        assertFalse(transactionManager.commitTransaction("invalid_tx"));
    }

    @Test
    void testRollbackTransaction() {
        String txId = transactionManager.beginTransaction("msg1", "testTopic", "producer1", "client1");

        // Prepare and rollback
        assertTrue(transactionManager.prepareMessage(txId, testMessage));
        assertTrue(transactionManager.rollbackTransaction(txId));

        // Verify transaction and message are cleaned up
        assertEquals(0, transactionManager.getActiveTransactionCount());
        assertEquals(0, transactionManager.getPreparedMessageCount());

        // Try to rollback invalid transaction
        assertFalse(transactionManager.rollbackTransaction("invalid_tx"));
    }

    @Test
    void testCommitWithoutPrepare() {
        String txId = transactionManager.beginTransaction("msg1", "testTopic", "producer1", "client1");

        // Try to commit without preparing
        assertTrue(transactionManager.commitTransaction(txId));

        // Should still succeed but no prepared message to commit
        assertEquals(0, transactionManager.getActiveTransactionCount());
    }

    @Test
    void testDoubleCommit() {
        String txId = transactionManager.beginTransaction("msg1", "testTopic", "producer1", "client1");

        // Prepare and commit once
        assertTrue(transactionManager.prepareMessage(txId, testMessage));
        assertTrue(transactionManager.commitTransaction(txId));

        // Try to commit again - should fail
        assertFalse(transactionManager.commitTransaction(txId));
    }

    @Test
    void testCheckTransactionState() {
        String txId = transactionManager.beginTransaction("msg1", "testTopic", "producer1", "client1");

        // Initial state should be PREPARE
        TransactionState state = transactionManager.checkTransactionState(txId);
        assertEquals(TransactionState.PREPARE, state);

        // Check unknown transaction
        TransactionState unknownState = transactionManager.checkTransactionState("invalid_tx");
        assertEquals(TransactionState.UNKNOWN, unknownState);
    }

    @Test
    void testTransactionInfo() {
        String txId = transactionManager.beginTransaction("msg1", "testTopic", "producer1", "client1");

        var txMsg = transactionManager.getTransaction(txId);
        assertNotNull(txMsg);
        assertEquals(txId, txMsg.getTransactionId());
        assertEquals("msg1", txMsg.getMsgId());
        assertEquals("testTopic", txMsg.getTopic());
        assertEquals("producer1", txMsg.getProducerGroup());
        assertEquals("client1", txMsg.getProducerId());
        assertEquals(TransactionState.PREPARE, txMsg.getState());
    }

    @Test
    void testMultipleTransactions() {
        // Create multiple transactions
        String tx1 = transactionManager.beginTransaction("msg1", "topic1", "producer1", "client1");
        String tx2 = transactionManager.beginTransaction("msg2", "topic2", "producer2", "client2");

        assertNotNull(tx1);
        assertNotNull(tx2);
        assertNotEquals(tx1, tx2);

        assertEquals(2, transactionManager.getActiveTransactionCount());

        // Prepare messages
        assertTrue(transactionManager.prepareMessage(tx1, testMessage));

        Message msg2 = new Message();
        msg2.setTopic("topic2");
        msg2.setBody("test message 2".getBytes());
        assertTrue(transactionManager.prepareMessage(tx2, msg2));

        assertEquals(2, transactionManager.getPreparedMessageCount());

        // Commit one, rollback another
        assertTrue(transactionManager.commitTransaction(tx1));
        assertTrue(transactionManager.rollbackTransaction(tx2));

        assertEquals(0, transactionManager.getActiveTransactionCount());
        assertEquals(0, transactionManager.getPreparedMessageCount());
    }
}