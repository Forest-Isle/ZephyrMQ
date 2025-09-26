package com.zephyr.broker.topic;

import com.zephyr.common.config.BrokerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TopicServiceTest {

    private TopicService topicService;
    private BrokerConfig brokerConfig;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerName("testBroker");
        brokerConfig.setStorePathRootDir(tempDir.toString());

        TopicConfigManager topicConfigManager = new TopicConfigManager(brokerConfig);
        topicConfigManager.start(); // Ensure manager is started to load default topics
        topicService = new TopicService(topicConfigManager, brokerConfig);
    }

    @Test
    void testCreateTopic() {
        TopicService.TopicCreateRequest request = new TopicService.TopicCreateRequest();
        request.setTopicName("testTopic");
        request.setReadQueueNums(4);
        request.setWriteQueueNums(4);
        request.setPerm(6);

        TopicService.TopicCreateResult result = topicService.createTopic(request);
        assertTrue(result.isSuccess());

        // Test duplicate creation
        TopicService.TopicCreateResult duplicateResult = topicService.createTopic(request);
        assertFalse(duplicateResult.isSuccess());
        assertTrue(duplicateResult.getMessage().contains("already exists"));
    }

    @Test
    void testCreateTopicWithInvalidName() {
        TopicService.TopicCreateRequest request = new TopicService.TopicCreateRequest();

        // Test null name
        request.setTopicName(null);
        TopicService.TopicCreateResult result = topicService.createTopic(request);
        assertFalse(result.isSuccess());

        // Test empty name
        request.setTopicName("");
        result = topicService.createTopic(request);
        assertFalse(result.isSuccess());

        // Test invalid characters
        request.setTopicName("invalid@topic");
        result = topicService.createTopic(request);
        assertFalse(result.isSuccess());

        // Test too long name
        request.setTopicName("a".repeat(128));
        result = topicService.createTopic(request);
        assertFalse(result.isSuccess());
    }

    @Test
    void testQueryTopic() {
        // Create a topic first
        TopicService.TopicCreateRequest request = new TopicService.TopicCreateRequest();
        request.setTopicName("queryTestTopic");
        request.setReadQueueNums(8);
        request.setWriteQueueNums(6);
        request.setOrder(true);

        topicService.createTopic(request);

        // Query the topic
        TopicService.TopicInfo info = topicService.queryTopic("queryTestTopic");
        assertNotNull(info);
        assertEquals("queryTestTopic", info.getTopicName());
        assertEquals(8, info.getReadQueueNums());
        assertEquals(6, info.getWriteQueueNums());
        assertTrue(info.isOrder());
        assertNotNull(info.getRouteInfo());

        // Query non-existent topic
        TopicService.TopicInfo nullInfo = topicService.queryTopic("nonExistentTopic");
        assertNull(nullInfo);
    }

    @Test
    void testDeleteTopic() {
        // Create a topic first
        TopicService.TopicCreateRequest request = new TopicService.TopicCreateRequest();
        request.setTopicName("deleteTestTopic");
        topicService.createTopic(request);

        // Verify it exists
        assertNotNull(topicService.queryTopic("deleteTestTopic"));

        // Delete the topic
        TopicService.TopicDeleteResult result = topicService.deleteTopic("deleteTestTopic");
        assertTrue(result.isSuccess());

        // Verify it's gone
        assertNull(topicService.queryTopic("deleteTestTopic"));

        // Delete non-existent topic
        TopicService.TopicDeleteResult failResult = topicService.deleteTopic("nonExistentTopic");
        assertFalse(failResult.isSuccess());
    }

    @Test
    void testUpdateTopic() {
        // Create a topic first
        TopicService.TopicCreateRequest createRequest = new TopicService.TopicCreateRequest();
        createRequest.setTopicName("updateTestTopic");
        createRequest.setReadQueueNums(4);
        createRequest.setWriteQueueNums(4);
        topicService.createTopic(createRequest);

        // Update the topic
        TopicService.TopicUpdateRequest updateRequest = new TopicService.TopicUpdateRequest();
        updateRequest.setTopicName("updateTestTopic");
        updateRequest.setReadQueueNums(8);
        updateRequest.setWriteQueueNums(6);
        updateRequest.setOrder(true);

        TopicService.TopicUpdateResult result = topicService.updateTopic(updateRequest);
        assertTrue(result.isSuccess());

        // Verify updates
        TopicService.TopicInfo info = topicService.queryTopic("updateTestTopic");
        assertNotNull(info);
        assertEquals(8, info.getReadQueueNums());
        assertEquals(6, info.getWriteQueueNums());
        assertTrue(info.isOrder());

        // Update non-existent topic
        updateRequest.setTopicName("nonExistentTopic");
        TopicService.TopicUpdateResult failResult = topicService.updateTopic(updateRequest);
        assertFalse(failResult.isSuccess());
    }

    @Test
    void testListTopics() {
        // Initially should have default topics
        List<String> initialTopics = topicService.listTopics();
        assertTrue(initialTopics.size() >= 2); // DefaultTopic and TestTopic

        // Create new topics
        TopicService.TopicCreateRequest request1 = new TopicService.TopicCreateRequest();
        request1.setTopicName("listTest1");
        topicService.createTopic(request1);

        TopicService.TopicCreateRequest request2 = new TopicService.TopicCreateRequest();
        request2.setTopicName("listTest2");
        topicService.createTopic(request2);

        List<String> topics = topicService.listTopics();
        assertTrue(topics.contains("listTest1"));
        assertTrue(topics.contains("listTest2"));
        assertTrue(topics.size() >= initialTopics.size() + 2);
    }

    @Test
    void testGetTopicRouteInfo() {
        // Create a topic
        TopicService.TopicCreateRequest request = new TopicService.TopicCreateRequest();
        request.setTopicName("routeTestTopic");
        request.setWriteQueueNums(6);
        topicService.createTopic(request);

        // Get route info
        TopicService.TopicRouteInfo routeInfo = topicService.getTopicRouteInfo("routeTestTopic");
        assertNotNull(routeInfo);
        assertEquals("routeTestTopic", routeInfo.getTopicName());
        assertEquals(6, routeInfo.getWriteQueueNums());
        assertEquals(brokerConfig.getBrokerName(), routeInfo.getBrokerName());

        // Get route info for non-existent topic
        TopicService.TopicRouteInfo nullRoute = topicService.getTopicRouteInfo("nonExistentTopic");
        assertNull(nullRoute);
    }
}