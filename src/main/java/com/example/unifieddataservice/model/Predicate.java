package com.example.unifieddataservice.model;

/**
 * Represents a single filter predicate (e.g., "column > value").
 * This is an immutable record that forms the building block for query conditions.
 *
 * @param columnName The name of the column to filter on.
 * @param operator   The comparison operator.
 * @param value      The value to compare against. The type should match the column's data type.
 */
public record Predicate(String columnName, Operator operator, Object value) {
}
