package com.zephyr.broker.filter;

import com.zephyr.protocol.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * SQL-like expression filter for messages
 * Supports filtering based on message properties and content
 */
public class SqlFilter implements MessageFilter {

    private static final Logger logger = LoggerFactory.getLogger(SqlFilter.class);

    // Basic SQL operators
    private static final Pattern SQL_PATTERN = Pattern.compile(
        "\\b(AND|OR|NOT|=|!=|<>|>|>=|<|<=|LIKE|IN|IS|NULL|TRUE|FALSE)\\b",
        Pattern.CASE_INSENSITIVE
    );

    @Override
    public boolean matches(Message message, String filterExpression) {
        if (message == null || filterExpression == null || filterExpression.trim().isEmpty()) {
            return true; // No filter means accept all
        }

        try {
            return evaluateExpression(message, filterExpression.trim());
        } catch (Exception e) {
            logger.warn("Error evaluating SQL filter expression: {}", filterExpression, e);
            return false;
        }
    }

    @Override
    public String getType() {
        return "SQL";
    }

    @Override
    public boolean isValidExpression(String filterExpression) {
        if (filterExpression == null || filterExpression.trim().isEmpty()) {
            return true;
        }

        try {
            String expr = filterExpression.trim();

            // Basic syntax validation
            if (!hasMatchingParentheses(expr)) {
                return false;
            }

            // Check for unclosed quotes
            if (!hasMatchingQuotes(expr)) {
                return false;
            }

            // Check for basic SQL syntax
            return SQL_PATTERN.matcher(expr).find() || expr.contains("property") ||
                   expr.matches("^[a-zA-Z0-9_\\-\\s='\"<>!]+$");

        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Evaluate SQL-like expression against message
     *
     * @param message the message
     * @param expression SQL-like expression
     * @return evaluation result
     */
    private boolean evaluateExpression(Message message, String expression) {
        // This is a simplified SQL evaluator
        // For production use, consider using a proper SQL parser like JSQLParser

        // Handle simple property comparisons
        if (expression.contains("=")) {
            return evaluateComparison(message, expression, "=");
        } else if (expression.contains("!=") || expression.contains("<>")) {
            String op = expression.contains("!=") ? "!=" : "<>";
            return !evaluateComparison(message, expression, op);
        } else if (expression.contains("LIKE")) {
            return evaluateLike(message, expression);
        } else if (expression.contains("IS NULL")) {
            return evaluateIsNull(message, expression);
        } else if (expression.contains("IS NOT NULL")) {
            return !evaluateIsNull(message, expression.replace("IS NOT NULL", "IS NULL"));
        }

        // Default: check if expression is a property name
        String propertyValue = getPropertyValue(message, expression);
        return propertyValue != null && !propertyValue.isEmpty();
    }

    /**
     * Evaluate comparison expression (=, !=, <>, etc.)
     */
    private boolean evaluateComparison(Message message, String expression, String operator) {
        String[] parts = expression.split(Pattern.quote(operator), 2);
        if (parts.length != 2) {
            return false;
        }

        String leftSide = parts[0].trim();
        String rightSide = parts[1].trim();

        // Remove quotes from right side if present
        if (rightSide.startsWith("'") && rightSide.endsWith("'")) {
            rightSide = rightSide.substring(1, rightSide.length() - 1);
        }

        String leftValue = getPropertyValue(message, leftSide);
        return rightSide.equals(leftValue);
    }

    /**
     * Evaluate LIKE expression
     */
    private boolean evaluateLike(Message message, String expression) {
        String[] parts = expression.split("\\s+LIKE\\s+", 2);
        if (parts.length != 2) {
            return false;
        }

        String propertyName = parts[0].trim();
        String pattern = parts[1].trim();

        // Remove quotes
        if (pattern.startsWith("'") && pattern.endsWith("'")) {
            pattern = pattern.substring(1, pattern.length() - 1);
        }

        String propertyValue = getPropertyValue(message, propertyName);
        if (propertyValue == null) {
            return false;
        }

        // Convert SQL LIKE pattern to regex
        String regex = pattern.replace("%", ".*").replace("_", ".");
        return Pattern.compile(regex, Pattern.CASE_INSENSITIVE).matcher(propertyValue).matches();
    }

    /**
     * Evaluate IS NULL expression
     */
    private boolean evaluateIsNull(Message message, String expression) {
        String propertyName = expression.replace("IS NULL", "").trim();
        String propertyValue = getPropertyValue(message, propertyName);
        return propertyValue == null || propertyValue.isEmpty();
    }

    /**
     * Get property value from message
     */
    private String getPropertyValue(Message message, String propertyName) {
        if (message == null || propertyName == null) {
            return null;
        }

        String name = propertyName.trim();

        // Built-in properties
        switch (name.toLowerCase()) {
            case "topic":
                return message.getTopic();
            case "tags":
                return message.getTags();
            case "keys":
                return message.getKeys();
            case "flag":
                return String.valueOf(message.getFlag());
            case "delaytimelevel":
                return String.valueOf(message.getDelayTimeLevel());
            default:
                // Custom properties
                Map<String, String> properties = message.getProperties();
                return properties != null ? properties.get(name) : null;
        }
    }

    /**
     * Check if parentheses are balanced
     */
    private boolean hasMatchingParentheses(String expression) {
        int count = 0;
        for (char c : expression.toCharArray()) {
            if (c == '(') {
                count++;
            } else if (c == ')') {
                count--;
                if (count < 0) {
                    return false;
                }
            }
        }
        return count == 0;
    }

    /**
     * Check if quotes are properly matched
     */
    private boolean hasMatchingQuotes(String expression) {
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        char[] chars = expression.toCharArray();

        for (int i = 0; i < chars.length; i++) {
            char c = chars[i];

            // Skip escaped quotes
            if (i > 0 && chars[i-1] == '\\') {
                continue;
            }

            if (c == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
            } else if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
            }
        }

        return !inSingleQuote && !inDoubleQuote;
    }
}