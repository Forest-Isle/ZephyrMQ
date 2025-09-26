package com.zephyr.broker.loadbalance;

import com.zephyr.protocol.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration-based allocation strategy
 * Allocates queues based on predefined configuration or room assignments
 */
public class AllocateMessageQueueByConfig implements LoadBalanceStrategy {

    private static final Logger logger = LoggerFactory.getLogger(AllocateMessageQueueByConfig.class);

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID,
                                     List<MessageQueue> mqAll, List<String> cidAll) {
        List<MessageQueue> result = new ArrayList<>();

        if (currentCID == null || currentCID.isEmpty()) {
            logger.warn("currentCID is empty");
            return result;
        }

        if (mqAll == null || mqAll.isEmpty()) {
            logger.warn("mqAll is null or empty");
            return result;
        }

        if (cidAll == null || cidAll.isEmpty()) {
            logger.warn("cidAll is null or empty");
            return result;
        }

        if (!cidAll.contains(currentCID)) {
            logger.warn("ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                       consumerGroup, currentCID, cidAll);
            return result;
        }

        // Simple configuration-based allocation
        // Can be extended to support more complex configuration rules
        int consumerIndex = cidAll.indexOf(currentCID);

        // Assign queues in round-robin fashion but with configuration preference
        for (int i = consumerIndex; i < mqAll.size(); i += cidAll.size()) {
            result.add(mqAll.get(i));
        }

        logger.debug("Allocated {} queues to consumer {} in group {} by config",
                    result.size(), currentCID, consumerGroup);

        return result;
    }

    @Override
    public String getName() {
        return "CONFIG";
    }
}