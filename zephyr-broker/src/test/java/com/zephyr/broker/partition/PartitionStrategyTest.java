package com.zephyr.broker.partition;

import com.zephyr.protocol.message.Message;
import com.zephyr.protocol.message.MessageQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class PartitionStrategyTest {

    private List<MessageQueue> messageQueues;
    private Message testMessage;

    @BeforeEach
    void setUp() {
        messageQueues = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            messageQueues.add(new MessageQueue("testTopic", "broker-a", i));
        }

        testMessage = new Message();
        testMessage.setTopic("testTopic");
        testMessage.setBody("test message".getBytes());
    }

    @Test
    void testRoundRobinPartitionStrategy() {
        RoundRobinPartitionStrategy strategy = new RoundRobinPartitionStrategy();

        // Test multiple selections should distribute evenly
        List<Integer> selectedQueues = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            MessageQueue selected = strategy.selectOne(testMessage, messageQueues, null);
            assertNotNull(selected);
            selectedQueues.add(selected.getQueueId());
        }

        // Should have selected each queue at least once in 8 attempts
        assertTrue(selectedQueues.contains(0));
        assertTrue(selectedQueues.contains(1));
        assertTrue(selectedQueues.contains(2));
        assertTrue(selectedQueues.contains(3));

        assertEquals("RoundRobin", strategy.getName());
    }

    @Test
    void testHashPartitionStrategy() {
        HashPartitionStrategy strategy = new HashPartitionStrategy();

        // Test with message key
        testMessage.setKeys("testKey");
        MessageQueue selected1 = strategy.selectOne(testMessage, messageQueues, null);
        MessageQueue selected2 = strategy.selectOne(testMessage, messageQueues, null);

        // Same key should always select same queue
        assertEquals(selected1.getQueueId(), selected2.getQueueId());

        // Different keys should potentially select different queues
        testMessage.setKeys("differentKey");
        MessageQueue selected3 = strategy.selectOne(testMessage, messageQueues, null);
        // May or may not be different, but at least should not throw exception
        assertNotNull(selected3);

        assertEquals("Hash", strategy.getName());
    }

    @Test
    void testRandomPartitionStrategy() {
        RandomPartitionStrategy strategy = new RandomPartitionStrategy();

        MessageQueue selected = strategy.selectOne(testMessage, messageQueues, null);
        assertNotNull(selected);
        assertTrue(selected.getQueueId() >= 0 && selected.getQueueId() < messageQueues.size());

        assertEquals("Random", strategy.getName());
    }

    @Test
    void testPartitionStrategyManager() {
        PartitionStrategyManager manager = new PartitionStrategyManager();

        // Test default strategy
        PartitionStrategy defaultStrategy = manager.getDefaultStrategy();
        assertNotNull(defaultStrategy);
        assertEquals("RoundRobin", defaultStrategy.getName());

        // Test getting strategies by name
        PartitionStrategy roundRobin = manager.getStrategy("RoundRobin");
        assertNotNull(roundRobin);
        assertEquals("RoundRobin", roundRobin.getName());

        PartitionStrategy hash = manager.getStrategy("Hash");
        assertNotNull(hash);
        assertEquals("Hash", hash.getName());

        // Test unknown strategy returns default
        PartitionStrategy unknown = manager.getStrategy("Unknown");
        assertEquals(defaultStrategy, unknown);

        // Test available strategies
        String[] strategies = manager.getAvailableStrategies();
        assertTrue(strategies.length >= 3);
        assertTrue(List.of(strategies).contains("RoundRobin"));
        assertTrue(List.of(strategies).contains("Hash"));
        assertTrue(List.of(strategies).contains("Random"));
    }

    @Test
    void testPartitionStrategyWithEmptyQueues() {
        RoundRobinPartitionStrategy strategy = new RoundRobinPartitionStrategy();

        // Test with empty queue list
        MessageQueue result = strategy.selectOne(testMessage, new ArrayList<>(), null);
        assertNull(result);

        // Test with null queue list
        result = strategy.selectOne(testMessage, null, null);
        assertNull(result);
    }
}