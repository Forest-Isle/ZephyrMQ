package com.zephyr.broker.loadbalance;

import com.zephyr.protocol.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Consistent hash allocation strategy
 * Uses consistent hashing for sticky partition assignment
 */
public class AllocateMessageQueueConsistentHash implements LoadBalanceStrategy {

    private static final Logger logger = LoggerFactory.getLogger(AllocateMessageQueueConsistentHash.class);

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

        // Use hash-based allocation for sticky assignment
        for (MessageQueue mq : mqAll) {
            String key = mq.getTopic() + "@" + mq.getBrokerName() + "@" + mq.getQueueId();
            int hash = Math.abs(key.hashCode());
            int selectedConsumerIndex = hash % cidAll.size();
            String selectedConsumer = cidAll.get(selectedConsumerIndex);

            if (selectedConsumer.equals(currentCID)) {
                result.add(mq);
            }
        }

        logger.debug("Allocated {} queues to consumer {} in group {} using consistent hash",
                    result.size(), currentCID, consumerGroup);

        return result;
    }

    @Override
    public String getName() {
        return "HASH";
    }
}