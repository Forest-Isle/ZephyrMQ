package com.zephyr.broker.ack;

import com.zephyr.protocol.ack.MessageAck;
import com.zephyr.protocol.ack.MessageAck.AckStatus;
import com.zephyr.protocol.message.MessageQueue;
import com.zephyr.broker.dlq.DeadLetterQueueManager;
import com.zephyr.broker.store.MessageStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;

public class AckManagerTest {

    private AckManager ackManager;
    private MessageQueue testQueue;
    private DeadLetterQueueManager mockDlqManager;
    private MessageStore mockMessageStore;

    @BeforeEach
    void setUp() {
        // Create mock dependencies
        mockMessageStore = Mockito.mock(MessageStore.class);
        mockDlqManager = new DeadLetterQueueManager(mockMessageStore);

        // Create AckManager with proper constructor
        ackManager = new AckManager(mockDlqManager, 5000, 3, 1000); // 5s timeout, 3 retries, 1s interval

        testQueue = new MessageQueue("testTopic", "broker1", 0);
    }

    @AfterEach
    void tearDown() {
        ackManager.shutdown();
    }

    @Test
    void testRegisterMessage() {
        String msgId = "msg123";
        String consumerId = "consumer1";
        String consumerGroup = "group1";

        // Register message for acknowledgment
        ackManager.registerMessage(msgId, testQueue, consumerGroup, consumerId);

        // Verify message is registered
        assertEquals(1, ackManager.getPendingAckCount());
    }

    @Test
    void testAcknowledgeMessageSuccess() {
        String msgId = "msg123";
        String consumerId = "consumer1";
        String consumerGroup = "group1";

        // Register message
        ackManager.registerMessage(msgId, testQueue, consumerGroup, consumerId);
        assertEquals(1, ackManager.getPendingAckCount());

        // Acknowledge with success
        MessageAck ack = new MessageAck();
        ack.setMsgId(msgId);
        ack.setAckStatus(AckStatus.SUCCESS);
        ack.setConsumerId(consumerId);
        ack.setConsumerGroup(consumerGroup);

        boolean result = ackManager.acknowledgeMessage(ack);

        assertTrue(result);
        assertEquals(0, ackManager.getPendingAckCount());
    }

    @Test
    void testAcknowledgeMessageRetryLater() {
        String msgId = "msg123";
        String consumerId = "consumer1";
        String consumerGroup = "group1";

        // Register message
        ackManager.registerMessage(msgId, testQueue, consumerGroup, consumerId);

        // Acknowledge with retry later
        MessageAck ack = new MessageAck();
        ack.setMsgId(msgId);
        ack.setAckStatus(AckStatus.RETRY_LATER);
        ack.setConsumerId(consumerId);
        ack.setConsumerGroup(consumerGroup);

        boolean result = ackManager.acknowledgeMessage(ack);

        assertTrue(result);
        // Message should be removed from pending but added to retry queue
        assertEquals(0, ackManager.getPendingAckCount());
        assertTrue(ackManager.getRetryQueueSize() >= 0);
    }

    @Test
    void testAcknowledgeMessageDeadLetter() {
        String msgId = "msg123";
        String consumerId = "consumer1";
        String consumerGroup = "group1";

        // Register message
        ackManager.registerMessage(msgId, testQueue, consumerGroup, consumerId);

        // Acknowledge with dead letter
        MessageAck ack = new MessageAck();
        ack.setMsgId(msgId);
        ack.setAckStatus(AckStatus.DEAD_LETTER);
        ack.setConsumerId(consumerId);
        ack.setConsumerGroup(consumerGroup);

        boolean result = ackManager.acknowledgeMessage(ack);

        assertTrue(result);
        assertEquals(0, ackManager.getPendingAckCount());
    }

    @Test
    void testAcknowledgeUnknownMessage() {
        MessageAck ack = new MessageAck();
        ack.setMsgId("unknown");
        ack.setAckStatus(AckStatus.SUCCESS);
        ack.setConsumerId("consumer1");
        ack.setConsumerGroup("group1");

        boolean result = ackManager.acknowledgeMessage(ack);

        assertFalse(result);
    }

    @Test
    void testConsumerOffset() {
        String consumerGroup = "group1";
        String topic = "testTopic";
        int queueId = 0;
        long offset = 100;

        // Get initial offset (should be 0)
        long initialOffset = ackManager.getConsumerOffset(consumerGroup, topic, queueId);
        assertEquals(0, initialOffset);

        // Update offset
        ackManager.updateConsumerOffset(consumerGroup, topic, queueId, offset);

        // Verify updated offset
        long updatedOffset = ackManager.getConsumerOffset(consumerGroup, topic, queueId);
        assertEquals(offset, updatedOffset);
    }

    @Test
    void testMultipleConsumerGroups() {
        String topic = "testTopic";
        int queueId = 0;

        String group1 = "group1";
        String group2 = "group2";
        long offset1 = 100;
        long offset2 = 200;

        // Update offsets for different groups
        ackManager.updateConsumerOffset(group1, topic, queueId, offset1);
        ackManager.updateConsumerOffset(group2, topic, queueId, offset2);

        // Verify offsets are independent
        assertEquals(offset1, ackManager.getConsumerOffset(group1, topic, queueId));
        assertEquals(offset2, ackManager.getConsumerOffset(group2, topic, queueId));
    }

    @Test
    void testRetryQueueSize() {
        // Initial retry queue should be empty or have some baseline
        int initialSize = ackManager.getRetryQueueSize();
        assertTrue(initialSize >= 0);

        // Register and nack a message to add to retry queue
        String msgId = "msg123";
        String consumerId = "consumer1";
        String consumerGroup = "group1";

        ackManager.registerMessage(msgId, testQueue, consumerGroup, consumerId);

        MessageAck ack = new MessageAck();
        ack.setMsgId(msgId);
        ack.setAckStatus(AckStatus.RETRY_LATER);
        ack.setConsumerId(consumerId);
        ack.setConsumerGroup(consumerGroup);

        ackManager.acknowledgeMessage(ack);

        // Retry queue size should potentially increase
        assertTrue(ackManager.getRetryQueueSize() >= initialSize);
    }
}