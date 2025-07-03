package com.example.unifieddataservice.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Represents the plan derived from parsing SQL, ready for execution.
 */
@Data
@Builder
public class MetricQueryPlan {
    private String tableName;
    private TableDefinition tableDefinition;
    /** fields requested in SELECT clause */
    private List<String> selectFields;
    /** mapping field -> metricName (with parameters as part of metric string) */
    private Map<String, String> fieldMetricMapping;
    /** simple equality conditions extracted from WHERE (key -> value). More complex conditions kept as rawWhere. */
    private Map<String, String> whereEqConditions;
    private String rawWhere; // original WHERE clause string
}
