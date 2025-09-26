package com.zephyr.broker.order;

import com.zephyr.protocol.message.Message;
import com.zephyr.protocol.message.MessageQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class OrderMessageManagerTest {

    private OrderMessageManager orderMessageManager;
    private Message testMessage;
    private MessageQueue testQueue;

    @BeforeEach
    void setUp() {
        orderMessageManager = new OrderMessageManager();

        testMessage = new Message();
        testMessage.setTopic("testTopic");
        testMessage.setBody("test message".getBytes());

        testQueue = new MessageQueue("testTopic", "broker1", 0);
    }

    @Test
    void testSendOrderedMessage() {
        String orderKey = "order123";
        long sequenceNumber = orderMessageManager.sendOrderedMessage(testMessage, testQueue, orderKey);

        assertEquals(1, sequenceNumber);

        OrderMessageManager.OrderQueueStatus status = orderMessageManager.getOrderQueueStatus(testQueue, orderKey);
        assertNotNull(status);
        assertEquals(1, status.getPendingMessages());
        assertEquals(1, status.getCurrentSequence());
    }

    @Test
    void testSendMultipleOrderedMessages() {
        String orderKey = "order123";

        long seq1 = orderMessageManager.sendOrderedMessage(testMessage, testQueue, orderKey);
        long seq2 = orderMessageManager.sendOrderedMessage(testMessage, testQueue, orderKey);
        long seq3 = orderMessageManager.sendOrderedMessage(testMessage, testQueue, orderKey);

        assertEquals(1, seq1);
        assertEquals(2, seq2);
        assertEquals(3, seq3);

        OrderMessageManager.OrderQueueStatus status = orderMessageManager.getOrderQueueStatus(testQueue, orderKey);
        assertEquals(3, status.getPendingMessages());
        assertEquals(3, status.getCurrentSequence());
    }

    @Test
    void testConsumeOrderedMessage() {
        String orderKey = "order123";
        String consumerId = "consumer1";

        // Send messages
        orderMessageManager.sendOrderedMessage(testMessage, testQueue, orderKey);
        orderMessageManager.sendOrderedMessage(testMessage, testQueue, orderKey);

        // Consume first message
        OrderMessageManager.OrderedMessage orderedMessage =
            orderMessageManager.consumeOrderedMessage(testQueue, orderKey, consumerId, 1000);

        assertNotNull(orderedMessage);
        assertEquals(1, orderedMessage.getSequenceNumber());
        assertEquals(orderKey, orderedMessage.getOrderKey());
        assertEquals(testQueue, orderedMessage.getMessageQueue());

        // Status should show one less pending message
        OrderMessageManager.OrderQueueStatus status = orderMessageManager.getOrderQueueStatus(testQueue, orderKey);
        assertEquals(1, status.getPendingMessages());
        assertEquals(consumerId, status.getProcessingConsumer());
    }

    @Test
    void testConsumerExclusivity() {
        String orderKey = "order123";
        String consumerId1 = "consumer1";
        String consumerId2 = "consumer2";

        // Send a message
        orderMessageManager.sendOrderedMessage(testMessage, testQueue, orderKey);

        // First consumer gets the message
        OrderMessageManager.OrderedMessage msg1 =
            orderMessageManager.consumeOrderedMessage(testQueue, orderKey, consumerId1, 100);
        assertNotNull(msg1);

        // Second consumer should be blocked
        OrderMessageManager.OrderedMessage msg2 =
            orderMessageManager.consumeOrderedMessage(testQueue, orderKey, consumerId2, 100);
        assertNull(msg2);
    }

    @Test
    void testReleaseOrderQueue() {
        String orderKey = "order123";
        String consumerId = "consumer1";

        // Send and consume a message
        orderMessageManager.sendOrderedMessage(testMessage, testQueue, orderKey);
        OrderMessageManager.OrderedMessage msg =
            orderMessageManager.consumeOrderedMessage(testQueue, orderKey, consumerId, 100);
        assertNotNull(msg);

        // Release the queue
        orderMessageManager.releaseOrderQueue(testQueue, orderKey, consumerId);

        // Status should show no processing consumer
        OrderMessageManager.OrderQueueStatus status = orderMessageManager.getOrderQueueStatus(testQueue, orderKey);
        assertNull(status.getProcessingConsumer());

        // Another consumer should now be able to consume
        String consumerId2 = "consumer2";
        orderMessageManager.sendOrderedMessage(testMessage, testQueue, orderKey);
        OrderMessageManager.OrderedMessage msg2 =
            orderMessageManager.consumeOrderedMessage(testQueue, orderKey, consumerId2, 100);
        assertNotNull(msg2);
    }

    @Test
    void testConsumeFromEmptyQueue() {
        String orderKey = "order123";
        String consumerId = "consumer1";

        OrderMessageManager.OrderedMessage msg =
            orderMessageManager.consumeOrderedMessage(testQueue, orderKey, consumerId, 100);
        assertNull(msg);
    }

    @Test
    void testDifferentOrderKeys() {
        String orderKey1 = "order123";
        String orderKey2 = "order456";
        String consumerId = "consumer1";

        // Send messages with different order keys
        orderMessageManager.sendOrderedMessage(testMessage, testQueue, orderKey1);
        orderMessageManager.sendOrderedMessage(testMessage, testQueue, orderKey2);

        // Should be able to consume from both queues independently
        OrderMessageManager.OrderedMessage msg1 =
            orderMessageManager.consumeOrderedMessage(testQueue, orderKey1, consumerId, 100);
        OrderMessageManager.OrderedMessage msg2 =
            orderMessageManager.consumeOrderedMessage(testQueue, orderKey2, consumerId, 100);

        assertNotNull(msg1);
        assertNotNull(msg2);
        assertEquals(orderKey1, msg1.getOrderKey());
        assertEquals(orderKey2, msg2.getOrderKey());
    }

    @Test
    void testIsOrderedQueue() {
        assertTrue(orderMessageManager.isOrderedQueue(testQueue));
    }

    @Test
    void testOrderQueueCount() {
        String orderKey1 = "order123";
        String orderKey2 = "order456";

        int initialCount = orderMessageManager.getOrderQueueCount();

        orderMessageManager.sendOrderedMessage(testMessage, testQueue, orderKey1);
        assertEquals(initialCount + 1, orderMessageManager.getOrderQueueCount());

        orderMessageManager.sendOrderedMessage(testMessage, testQueue, orderKey2);
        assertEquals(initialCount + 2, orderMessageManager.getOrderQueueCount());
    }

    @Test
    void testCleanup() {
        String orderKey = "order123";
        String consumerId = "consumer1";

        // Send and consume all messages
        orderMessageManager.sendOrderedMessage(testMessage, testQueue, orderKey);
        OrderMessageManager.OrderedMessage msg =
            orderMessageManager.consumeOrderedMessage(testQueue, orderKey, consumerId, 100);
        assertNotNull(msg);

        int beforeCleanup = orderMessageManager.getOrderQueueCount();
        orderMessageManager.cleanup();
        int afterCleanup = orderMessageManager.getOrderQueueCount();

        assertEquals(beforeCleanup - 1, afterCleanup);
    }

    @Test
    void testOrderedMessageProperties() {
        String orderKey = "order123";
        long sequence = orderMessageManager.sendOrderedMessage(testMessage, testQueue, orderKey);

        String consumerId = "consumer1";
        OrderMessageManager.OrderedMessage orderedMessage =
            orderMessageManager.consumeOrderedMessage(testQueue, orderKey, consumerId, 100);

        assertNotNull(orderedMessage);
        assertEquals(testMessage, orderedMessage.getMessage());
        assertEquals(testQueue, orderedMessage.getMessageQueue());
        assertEquals(orderKey, orderedMessage.getOrderKey());
        assertEquals(sequence, orderedMessage.getSequenceNumber());
        assertTrue(orderedMessage.getCreateTime() > 0);
    }
}