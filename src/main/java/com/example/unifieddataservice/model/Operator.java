package com.example.unifieddataservice.model;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Enum representing supported SQL comparison operators.
 * This provides a type-safe way to handle operators throughout the query engine.
 */
public enum Operator {
    EQUALS("="),
    NOT_EQUALS("!="),
    GREATER_THAN(">"),
    GREATER_THAN_OR_EQUAL_TO(">="),
    LESS_THAN("<"),
    LESS_THAN_OR_EQUAL_TO("<="),
    IN("IN");

    private final String symbol;

    // For fast lookup
    private static final Map<String, Operator> symbolToOperatorMap =
            Stream.of(values()).collect(Collectors.toMap(Operator::getSymbol, Function.identity()));

    Operator(String symbol) {
        this.symbol = symbol;
    }

    public String getSymbol() {
        return symbol;
    }

    /**
     * Converts a string symbol into its corresponding Operator enum.
     * @param text The operator symbol (e.g., "=", ">=").
     * @return The matching Operator.
     * @throws IllegalArgumentException if the symbol is not supported.
     */
    public static Operator fromString(String text) {
        if (text == null) {
            throw new IllegalArgumentException("Operator symbol cannot be null.");
        }
        Operator op = symbolToOperatorMap.get(text);
        if (op != null) {
            return op;
        }
        // Handle common variations like "==" for equals
        if ("==".equals(text)) {
            return EQUALS;
        }
        throw new IllegalArgumentException("Unsupported operator: '" + text + "'");
    }
}
