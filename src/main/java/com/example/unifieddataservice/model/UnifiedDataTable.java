package com.example.unifieddataservice.model;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

// Remove unused import

/**
 * Represents a table of data with Arrow-based storage.
 * Can track both physical and logical field names for schema mapping.
 */
/**
 * Represents a table of data with Arrow-based storage.
 * Can track both physical and logical field names for schema mapping.
 */
@Component
public class UnifiedDataTable implements AutoCloseable {
    // RootAllocator will be injected by Spring
    private final RootAllocator rootAllocator;
    private final VectorSchemaRoot data;
    private String logicalFieldName;
    private final String tableName;
    
    @Autowired
    public UnifiedDataTable(RootAllocator rootAllocator) {
        this.rootAllocator = rootAllocator;
        this.data = null; // Will be set by other constructors
        this.tableName = "";
    }


    /**
     * Creates a new UnifiedDataTable with the given data.
     * @param tableName The logical name of the table
     * @param data The vector schema root containing the data
     * @param schema The schema for the data (can be null to use data's schema)
     */
    public UnifiedDataTable(String tableName, VectorSchemaRoot data, Schema schema) {
        this.rootAllocator = new RootAllocator();
        this.tableName = tableName;
        // For now, just use the provided data as-is
        // In a production system, we'd need to handle schema conversion properly
        this.data = data;
        this.logicalFieldName = "";
    }
    
    /**
     * Creates a new UnifiedDataTable with the given data, using the data's schema.
     */
    public UnifiedDataTable(String tableName, VectorSchemaRoot data) {
        this.rootAllocator = new RootAllocator();
        this.tableName = tableName;
        this.data = data;
        this.logicalFieldName = "";
    }
    
    /**
     * Creates a new UnifiedDataTable with the given data, using default table name.
     */
    public UnifiedDataTable(VectorSchemaRoot data) {
        this.rootAllocator = new RootAllocator();
        this.tableName = "unnamed_table";
        this.data = data;
        this.logicalFieldName = "";
    }

    public VectorSchemaRoot getData() {
        return data;
    }

    public int getRowCount() {
        return data.getRowCount();
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public String getLogicalFieldName() {
        return logicalFieldName;
    }
    
    public void setLogicalFieldName(String logicalFieldName) {
        this.logicalFieldName = logicalFieldName;
    }

    @Override
    public void close() {
        if (data != null) {
            data.close();
        }
    }
    
    @Override
    public String toString() {
        return "UnifiedDataTable{" +
                "tableName='" + tableName + '\'' +
                ", logicalFieldName='" + logicalFieldName + '\'' +
                ", rowCount=" + (data != null ? data.getRowCount() : 0) +
                '}';
    }
}
