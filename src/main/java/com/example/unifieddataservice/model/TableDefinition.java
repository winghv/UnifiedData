package com.example.unifieddataservice.model;

import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.MapKeyColumn;
import jakarta.persistence.Column;
import jakarta.persistence.CollectionTable;
import jakarta.persistence.JoinColumn;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Defines a logical table – essentially a directory of metrics (fields).
 * Can be loaded from DB or configuration; kept simple for now.
 */
@Data
@Entity
public class TableDefinition {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(unique = true, nullable = false)
    private String tableName;

    // e.g. ["stock_code", "trade_time"]
    @ElementCollection
    private List<String> primaryKeys;

    /**
     * fieldName -> metricName mapping.
     * For parameterised metric (e.g. close_price(adjust=post)), the metricName can include parameters.
     */
    @ElementCollection
    @CollectionTable(name = "table_metric_mappings", joinColumns = @JoinColumn(name = "table_id"))
    @MapKeyColumn(name = "field_name")
    @Column(name = "metric_name")
    private Map<String, String> metricFields;

    private TimeGranularity timeGranularity;
}
