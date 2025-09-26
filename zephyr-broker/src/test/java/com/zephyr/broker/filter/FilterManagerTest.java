package com.zephyr.broker.filter;

import com.zephyr.protocol.message.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class FilterManagerTest {

    private FilterManager filterManager;
    private Message testMessage;

    @BeforeEach
    void setUp() {
        filterManager = new FilterManager();

        testMessage = new Message();
        testMessage.setTopic("testTopic");
        testMessage.setTags("tag1,tag2");
        testMessage.putProperty("userId", "12345");
        testMessage.putProperty("region", "US");
        testMessage.putProperty("level", "VIP");
    }

    @Test
    void testTagFilterSubscription() {
        // Subscribe with tag filter
        assertTrue(filterManager.subscribe("group1", "testTopic", "TAG", "tag1"));

        // Message with matching tag should pass
        assertTrue(filterManager.matches(testMessage, "group1"));

        // Subscribe with different tag
        assertTrue(filterManager.subscribe("group2", "testTopic", "TAG", "tag3"));

        // Message without matching tag should not pass
        assertFalse(filterManager.matches(testMessage, "group2"));

        // Wildcard should match all
        assertTrue(filterManager.subscribe("group3", "testTopic", "TAG", "*"));
        assertTrue(filterManager.matches(testMessage, "group3"));
    }

    @Test
    void testTagFilterOrLogic() {
        // Subscribe with OR logic
        assertTrue(filterManager.subscribe("group1", "testTopic", "TAG", "tag1||tag3"));

        // Message with tag1 should pass
        assertTrue(filterManager.matches(testMessage, "group1"));

        // Test message with only tag3
        Message msg2 = new Message();
        msg2.setTopic("testTopic");
        msg2.setTags("tag3,tag4");

        assertTrue(filterManager.matches(msg2, "group1"));
    }

    @Test
    void testSqlFilterSubscription() {
        // Subscribe with SQL filter
        assertTrue(filterManager.subscribe("group1", "testTopic", "SQL", "userId = '12345'"));

        // Message with matching property should pass
        assertTrue(filterManager.matches(testMessage, "group1"));

        // Subscribe with different condition
        assertTrue(filterManager.subscribe("group2", "testTopic", "SQL", "region = 'EU'"));

        // Message without matching property should not pass
        assertFalse(filterManager.matches(testMessage, "group2"));
    }

    @Test
    void testSqlFilterLike() {
        // Subscribe with LIKE filter
        assertTrue(filterManager.subscribe("group1", "testTopic", "SQL", "userId LIKE '123%'"));

        // Message with matching pattern should pass
        assertTrue(filterManager.matches(testMessage, "group1"));

        // Test non-matching pattern
        assertTrue(filterManager.subscribe("group2", "testTopic", "SQL", "userId LIKE '999%'"));
        assertFalse(filterManager.matches(testMessage, "group2"));
    }

    @Test
    void testSqlFilterNull() {
        // Subscribe with IS NULL filter
        assertTrue(filterManager.subscribe("group1", "testTopic", "SQL", "nonExistentProperty IS NULL"));

        // Message without property should pass
        assertTrue(filterManager.matches(testMessage, "group1"));

        // Test IS NOT NULL
        assertTrue(filterManager.subscribe("group2", "testTopic", "SQL", "userId IS NOT NULL"));
        assertTrue(filterManager.matches(testMessage, "group2"));
    }

    @Test
    void testUnsubscribe() {
        // Subscribe and then unsubscribe
        assertTrue(filterManager.subscribe("group1", "testTopic", "TAG", "tag1"));
        assertTrue(filterManager.matches(testMessage, "group1"));

        assertTrue(filterManager.unsubscribe("group1", "testTopic"));

        // After unsubscribe, should default to accept all
        assertTrue(filterManager.matches(testMessage, "group1"));

        // Unsubscribing non-existent subscription should return false
        assertFalse(filterManager.unsubscribe("group1", "testTopic"));
    }

    @Test
    void testGetSubscriptionInfo() {
        // Subscribe
        assertTrue(filterManager.subscribe("group1", "testTopic", "TAG", "tag1"));

        // Get subscription
        FilterManager.FilterSubscription subscription = filterManager.getSubscription("group1", "testTopic");
        assertNotNull(subscription);
        assertEquals("group1", subscription.getConsumerGroup());
        assertEquals("testTopic", subscription.getTopic());
        assertEquals("TAG", subscription.getFilterType());
        assertEquals("tag1", subscription.getFilterExpression());

        // Get subscriptions for group
        assertEquals(1, filterManager.getSubscriptions("group1").size());
        assertEquals(0, filterManager.getSubscriptions("group2").size());
    }

    @Test
    void testInvalidFilterExpression() {
        // Invalid tag filter should be rejected
        assertFalse(filterManager.subscribe("group1", "testTopic", "TAG", "tag1 && invalid_chars_%"));

        // Invalid SQL filter should be rejected
        assertFalse(filterManager.subscribe("group2", "testTopic", "SQL", "userId = 'unclosed"));
    }

    @Test
    void testUnknownFilterType() {
        // Unknown filter type should be rejected
        assertFalse(filterManager.subscribe("group1", "testTopic", "UNKNOWN", "expression"));
    }

    @Test
    void testDefaultBehavior() {
        // No subscription - should accept all messages
        assertTrue(filterManager.matches(testMessage, "group1"));

        // Empty filter expression should accept all
        assertTrue(filterManager.subscribe("group2", "testTopic", "TAG", ""));
        assertTrue(filterManager.matches(testMessage, "group2"));
    }

    @Test
    void testMultipleTopicSubscriptions() {
        // Subscribe to multiple topics
        assertTrue(filterManager.subscribe("group1", "topic1", "TAG", "tag1"));
        assertTrue(filterManager.subscribe("group1", "topic2", "TAG", "tag2"));

        assertEquals(2, filterManager.getSubscriptions("group1").size());

        // Test filtering for different topics
        Message msg1 = new Message();
        msg1.setTopic("topic1");
        msg1.setTags("tag1");

        Message msg2 = new Message();
        msg2.setTopic("topic2");
        msg2.setTags("tag2");

        assertTrue(filterManager.matches(msg1, "group1"));
        assertTrue(filterManager.matches(msg2, "group1"));
    }

    @Test
    void testAvailableFilterTypes() {
        String[] filterTypes = filterManager.getAvailableFilterTypes();
        assertTrue(filterTypes.length >= 2);

        boolean hasTag = false;
        boolean hasSQL = false;
        for (String type : filterTypes) {
            if ("TAG".equals(type)) hasTag = true;
            if ("SQL".equals(type)) hasSQL = true;
        }

        assertTrue(hasTag);
        assertTrue(hasSQL);
    }
}