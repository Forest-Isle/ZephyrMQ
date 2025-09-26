package com.zephyr.broker.loadbalance;

import com.zephyr.protocol.message.MessageQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class LoadBalanceManagerTest {

    private LoadBalanceManager loadBalanceManager;
    private List<MessageQueue> testQueues;

    @BeforeEach
    void setUp() {
        loadBalanceManager = new LoadBalanceManager();

        // Create test message queues
        testQueues = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            testQueues.add(new MessageQueue("testTopic", "broker-a", i));
        }

        // Update topic subscribe info
        loadBalanceManager.updateTopicSubscribeInfo("testTopic", testQueues);
    }

    @Test
    void testRegisterAndUnregisterConsumer() {
        String consumerGroup = "testGroup";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        // Register consumers
        loadBalanceManager.registerConsumer(consumerGroup, consumer1);
        loadBalanceManager.registerConsumer(consumerGroup, consumer2);

        List<String> consumers = loadBalanceManager.getConsumerList(consumerGroup);
        assertEquals(2, consumers.size());
        assertTrue(consumers.contains(consumer1));
        assertTrue(consumers.contains(consumer2));

        // Unregister one consumer
        loadBalanceManager.unregisterConsumer(consumerGroup, consumer1);
        consumers = loadBalanceManager.getConsumerList(consumerGroup);
        assertEquals(1, consumers.size());
        assertTrue(consumers.contains(consumer2));
        assertFalse(consumers.contains(consumer1));

        // Unregister last consumer
        loadBalanceManager.unregisterConsumer(consumerGroup, consumer2);
        consumers = loadBalanceManager.getConsumerList(consumerGroup);
        assertTrue(consumers.isEmpty());
    }

    @Test
    void testAllocateMessageQueuesAveragely() {
        String consumerGroup = "testGroup";
        loadBalanceManager.registerConsumer(consumerGroup, "consumer1");
        loadBalanceManager.registerConsumer(consumerGroup, "consumer2");
        loadBalanceManager.registerConsumer(consumerGroup, "consumer3");

        // Allocate queues to consumer1
        List<MessageQueue> queues1 = loadBalanceManager.allocateMessageQueues(
            consumerGroup, "consumer1", "testTopic", "AVG");

        // Allocate queues to consumer2
        List<MessageQueue> queues2 = loadBalanceManager.allocateMessageQueues(
            consumerGroup, "consumer2", "testTopic", "AVG");

        // Allocate queues to consumer3
        List<MessageQueue> queues3 = loadBalanceManager.allocateMessageQueues(
            consumerGroup, "consumer3", "testTopic", "AVG");

        // Verify each consumer gets some queues
        assertFalse(queues1.isEmpty());
        assertFalse(queues2.isEmpty());
        assertFalse(queues3.isEmpty());

        // Verify total allocated queues equals available queues
        int totalAllocated = queues1.size() + queues2.size() + queues3.size();
        assertEquals(testQueues.size(), totalAllocated);

        // Verify no queue is allocated to multiple consumers
        List<MessageQueue> allAllocated = new ArrayList<>();
        allAllocated.addAll(queues1);
        allAllocated.addAll(queues2);
        allAllocated.addAll(queues3);

        assertEquals(testQueues.size(), allAllocated.stream().distinct().count());
    }

    @Test
    void testAllocateMessageQueuesConsistentHash() {
        String consumerGroup = "hashGroup";
        loadBalanceManager.registerConsumer(consumerGroup, "consumer1");
        loadBalanceManager.registerConsumer(consumerGroup, "consumer2");

        // Allocate queues using hash strategy
        List<MessageQueue> queues1_first = loadBalanceManager.allocateMessageQueues(
            consumerGroup, "consumer1", "testTopic", "HASH");

        List<MessageQueue> queues2_first = loadBalanceManager.allocateMessageQueues(
            consumerGroup, "consumer2", "testTopic", "HASH");

        // Allocate again to verify consistency
        List<MessageQueue> queues1_second = loadBalanceManager.allocateMessageQueues(
            consumerGroup, "consumer1", "testTopic", "HASH");

        List<MessageQueue> queues2_second = loadBalanceManager.allocateMessageQueues(
            consumerGroup, "consumer2", "testTopic", "HASH");

        // Verify consistency - same consumer should get same queues
        assertEquals(queues1_first, queues1_second);
        assertEquals(queues2_first, queues2_second);
    }

    @Test
    void testGetConsumerGroupStats() {
        LoadBalanceManager freshManager = new LoadBalanceManager(); // Use fresh instance

        // Register consumers in different groups
        freshManager.registerConsumer("group1", "consumer1");
        freshManager.registerConsumer("group1", "consumer2");
        freshManager.registerConsumer("group2", "consumer3");

        // Update topic info
        freshManager.updateTopicSubscribeInfo("topic1", testQueues);
        freshManager.updateTopicSubscribeInfo("topic2", testQueues.subList(0, 2));

        LoadBalanceManager.ConsumerGroupStats stats = freshManager.getConsumerGroupStats();

        assertEquals(2, stats.getTotalGroups());
        assertEquals(3, stats.getTotalConsumers());
        assertEquals(2, stats.getTotalTopics());
    }

    @Test
    void testGetAvailableStrategies() {
        String[] strategies = loadBalanceManager.getAvailableStrategies();

        assertTrue(strategies.length >= 3);
        assertTrue(List.of(strategies).contains("AVG"));
        assertTrue(List.of(strategies).contains("HASH"));
        assertTrue(List.of(strategies).contains("CONFIG"));
    }

    @Test
    void testSetDefaultStrategy() {
        // Default should be AVG
        LoadBalanceStrategy defaultStrategy = loadBalanceManager.getStrategy(null);
        assertEquals("AVG", defaultStrategy.getName());

        // Change default to HASH
        loadBalanceManager.setDefaultStrategy("HASH");
        defaultStrategy = loadBalanceManager.getStrategy(null);
        assertEquals("HASH", defaultStrategy.getName());

        // Try to set non-existent strategy (should keep current default)
        loadBalanceManager.setDefaultStrategy("NONEXISTENT");
        defaultStrategy = loadBalanceManager.getStrategy(null);
        assertEquals("HASH", defaultStrategy.getName());
    }

    @Test
    void testAllocateWithInvalidParameters() {
        String consumerGroup = "testGroup";
        loadBalanceManager.registerConsumer(consumerGroup, "consumer1");

        // Test with null consumer group
        List<MessageQueue> result = loadBalanceManager.allocateMessageQueues(
            null, "consumer1", "testTopic", "AVG");
        assertTrue(result.isEmpty());

        // Test with null consumer ID
        result = loadBalanceManager.allocateMessageQueues(
            consumerGroup, null, "testTopic", "AVG");
        assertTrue(result.isEmpty());

        // Test with null topic
        result = loadBalanceManager.allocateMessageQueues(
            consumerGroup, "consumer1", null, "AVG");
        assertTrue(result.isEmpty());

        // Test with non-existent consumer group
        result = loadBalanceManager.allocateMessageQueues(
            "nonExistentGroup", "consumer1", "testTopic", "AVG");
        assertTrue(result.isEmpty());
    }
}