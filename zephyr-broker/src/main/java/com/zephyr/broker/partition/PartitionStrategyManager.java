package com.zephyr.broker.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Partition strategy manager
 * Manages different partition strategies and provides strategy selection
 */
public class PartitionStrategyManager {

    private static final Logger logger = LoggerFactory.getLogger(PartitionStrategyManager.class);

    private final ConcurrentMap<String, PartitionStrategy> strategies = new ConcurrentHashMap<>();
    private PartitionStrategy defaultStrategy;

    public PartitionStrategyManager() {
        // Register built-in strategies
        registerStrategy(new RoundRobinPartitionStrategy());
        registerStrategy(new HashPartitionStrategy());
        registerStrategy(new RandomPartitionStrategy());

        // Set default strategy
        this.defaultStrategy = strategies.get("RoundRobin");
        logger.info("PartitionStrategyManager initialized with {} strategies", strategies.size());
    }

    /**
     * Register a partition strategy
     *
     * @param strategy the strategy to register
     */
    public void registerStrategy(PartitionStrategy strategy) {
        if (strategy != null && strategy.getName() != null) {
            strategies.put(strategy.getName(), strategy);
            logger.debug("Registered partition strategy: {}", strategy.getName());
        }
    }

    /**
     * Get partition strategy by name
     *
     * @param strategyName strategy name
     * @return partition strategy, or default strategy if not found
     */
    public PartitionStrategy getStrategy(String strategyName) {
        if (strategyName == null || strategyName.isEmpty()) {
            return defaultStrategy;
        }

        PartitionStrategy strategy = strategies.get(strategyName);
        return strategy != null ? strategy : defaultStrategy;
    }

    /**
     * Get default partition strategy
     *
     * @return default partition strategy
     */
    public PartitionStrategy getDefaultStrategy() {
        return defaultStrategy;
    }

    /**
     * Set default partition strategy
     *
     * @param strategyName strategy name
     */
    public void setDefaultStrategy(String strategyName) {
        PartitionStrategy strategy = strategies.get(strategyName);
        if (strategy != null) {
            this.defaultStrategy = strategy;
            logger.info("Default partition strategy set to: {}", strategyName);
        } else {
            logger.warn("Strategy not found: {}, keeping current default", strategyName);
        }
    }

    /**
     * Get all available strategy names
     *
     * @return strategy names
     */
    public String[] getAvailableStrategies() {
        return strategies.keySet().toArray(new String[0]);
    }
}