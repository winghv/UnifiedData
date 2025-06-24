package com.example.unifieddataservice.model;

import jakarta.persistence.*;
import lombok.Data;

import java.util.Map;

@Entity
@Data
public class MetricInfo {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(unique = true, nullable = false)
    private String name;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private DataSourceType dataSourceType;

    @Column(nullable = false)
    private String sourceUrl;

    private String dataPath; // Path to the data array in a JSON object

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "metric_field_mappings", joinColumns = @JoinColumn(name = "metric_id"))
    @MapKeyColumn(name = "field_name")
    @Enumerated(EnumType.STRING)
    @Column(name = "data_type")
    private Map<String, DataType> fieldMappings;
}
