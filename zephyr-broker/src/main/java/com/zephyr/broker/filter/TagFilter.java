package com.zephyr.broker.filter;

import com.zephyr.protocol.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;

/**
 * Tag-based message filter
 * Filters messages based on tags with support for multiple tags
 */
public class TagFilter implements MessageFilter {

    private static final Logger logger = LoggerFactory.getLogger(TagFilter.class);

    @Override
    public boolean matches(Message message, String filterExpression) {
        if (message == null || filterExpression == null || filterExpression.trim().isEmpty()) {
            return true; // No filter means accept all
        }

        String messageTags = message.getTags();
        if (messageTags == null || messageTags.trim().isEmpty()) {
            // Message has no tags, only match if filter allows empty tags
            return filterExpression.equals("*") || filterExpression.contains("null");
        }

        // Handle wildcard
        if ("*".equals(filterExpression.trim())) {
            return true;
        }

        try {
            // Parse filter expression - supports OR logic with ||
            String[] orParts = filterExpression.split("\\|\\|");

            for (String orPart : orParts) {
                if (matchesAndExpression(messageTags, orPart.trim())) {
                    return true;
                }
            }

            return false;

        } catch (Exception e) {
            logger.warn("Error evaluating tag filter expression: {}", filterExpression, e);
            return false;
        }
    }

    @Override
    public String getType() {
        return "TAG";
    }

    @Override
    public boolean isValidExpression(String filterExpression) {
        if (filterExpression == null) {
            return true;
        }

        String expr = filterExpression.trim();
        if (expr.isEmpty() || "*".equals(expr)) {
            return true;
        }

        try {
            // Basic validation - check for valid characters
            // Reject expressions with invalid patterns like "tag && invalid_chars_%"
            if (expr.contains("%") || expr.contains("@") || expr.contains("#")) {
                return false;
            }
            return expr.matches("^[a-zA-Z0-9_\\-\\|\\&\\*\\s]+$");
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Check if message tags match AND expression
     *
     * @param messageTags message tags (comma separated)
     * @param andExpression AND expression (space or && separated)
     * @return true if matches
     */
    private boolean matchesAndExpression(String messageTags, String andExpression) {
        // Parse AND expression
        String[] andParts = andExpression.split("(\\s+|&&)");
        Set<String> messageTagSet = parseTagsToSet(messageTags);

        for (String andPart : andParts) {
            String tag = andPart.trim();
            if (!tag.isEmpty() && !messageTagSet.contains(tag)) {
                return false; // All AND parts must match
            }
        }

        return true;
    }

    /**
     * Parse comma-separated tags to set
     *
     * @param tags comma-separated tags
     * @return set of tags
     */
    private Set<String> parseTagsToSet(String tags) {
        Set<String> tagSet = new HashSet<>();
        if (tags != null && !tags.trim().isEmpty()) {
            Arrays.stream(tags.split(","))
                  .map(String::trim)
                  .filter(tag -> !tag.isEmpty())
                  .forEach(tagSet::add);
        }
        return tagSet;
    }
}