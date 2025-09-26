package com.zephyr.broker.filter;

import com.zephyr.protocol.message.Message;

/**
 * Message filter interface
 * Determines whether a message should be delivered to a consumer
 */
public interface MessageFilter {

    /**
     * Check if message matches the filter criteria
     *
     * @param message the message to filter
     * @param filterExpression filter expression
     * @return true if message matches, false otherwise
     */
    boolean matches(Message message, String filterExpression);

    /**
     * Get filter type name
     *
     * @return filter type name
     */
    String getType();

    /**
     * Validate filter expression syntax
     *
     * @param filterExpression filter expression to validate
     * @return true if expression is valid
     */
    boolean isValidExpression(String filterExpression);
}