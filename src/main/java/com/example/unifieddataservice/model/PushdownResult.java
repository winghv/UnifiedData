package com.example.unifieddataservice.model;

import java.util.List;

/**
 * A record to hold the results of predicate pushdown analysis.
 * It separates predicates into those that can be pushed down to the data source
 * and those that must be applied as a fallback in memory.
 *
 * @param pushedDown The list of predicates to be pushed down.
 * @param fallback   The list of predicates to be applied in memory.
 */
public record PushdownResult(
    List<Predicate> pushedDown,
    List<Predicate> fallback
) {}
