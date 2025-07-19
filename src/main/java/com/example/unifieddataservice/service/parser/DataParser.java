package com.example.unifieddataservice.service.parser;

import com.example.unifieddataservice.model.DataType;
import com.example.unifieddataservice.model.UnifiedDataTable;

import java.io.InputStream;
import java.util.Map;

public interface DataParser {
    /**
     * Parse the input data stream into a UnifiedDataTable.
     * 
     * @param data The input data stream to parse
     * @param fieldMappings Map of field names to their data types
     * @param dataPath Path to the data within the input (for JSON/XML)
     * @param columnAlias Map of source column names to their aliases (can be null or empty)
     * @return A UnifiedDataTable containing the parsed data
     */
    UnifiedDataTable parse(
        InputStream data, 
        Map<String, DataType> fieldMappings, 
        String dataPath,
        Map<String, String> columnAlias
    );
    
    /**
     * Backward-compatible parse method that doesn't include column aliases.
     * Default implementation throws UnsupportedOperationException.
     */
    default UnifiedDataTable parse(InputStream data, Map<String, DataType> fieldMappings, String dataPath) {
        return parse(data, fieldMappings, dataPath, null);
    }
}
