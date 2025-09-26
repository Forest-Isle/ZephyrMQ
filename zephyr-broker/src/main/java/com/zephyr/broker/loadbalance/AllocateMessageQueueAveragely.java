package com.zephyr.broker.loadbalance;

import com.zephyr.protocol.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Average allocation strategy
 * Evenly distributes message queues among consumers
 */
public class AllocateMessageQueueAveragely implements LoadBalanceStrategy {

    private static final Logger logger = LoggerFactory.getLogger(AllocateMessageQueueAveragely.class);

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

        int index = cidAll.indexOf(currentCID);
        int mod = mqAll.size() % cidAll.size();
        int averageSize = mqAll.size() <= cidAll.size() ? 1 :
                         (mod > 0 && index < mod ? mqAll.size() / cidAll.size() + 1 :
                          mqAll.size() / cidAll.size());

        int startIndex = (mod > 0 && index < mod) ? index * averageSize :
                        index * averageSize + mod;

        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }

        logger.debug("Allocated {} queues to consumer {} in group {}",
                    result.size(), currentCID, consumerGroup);

        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}