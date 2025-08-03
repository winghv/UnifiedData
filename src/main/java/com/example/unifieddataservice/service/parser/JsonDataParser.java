package com.example.unifieddataservice.service.parser;

import com.example.unifieddataservice.model.DataType;
import com.example.unifieddataservice.model.Predicate;
import com.example.unifieddataservice.model.UnifiedDataTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class JsonDataParser implements DataParser {
    private final RootAllocator rootAllocator;
    private static final Logger logger = LoggerFactory.getLogger(JsonDataParser.class);

    @Autowired
    public JsonDataParser(RootAllocator rootAllocator) {
        this.rootAllocator = rootAllocator;
    }
    
    private String inputStreamToString(InputStream is) {
        try {
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = is.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }
            return result.toString(StandardCharsets.UTF_8.name());
        } catch (IOException e) {
            logger.error("Failed to read input stream", e);
            return "[Error reading input stream: " + e.getMessage() + "]";
        }
    }
    
    private void setVectorValue(FieldVector vector, DataType dataType, int rowIndex, JsonNode cellNode) {
        try {
            switch (dataType) {
                case STRING:
                    String strValue = cellNode.asText();
                    ((VarCharVector) vector).setSafe(rowIndex, strValue.getBytes(StandardCharsets.UTF_8));
                    break;
                case LONG:
                    long longValue = cellNode.asLong();
                    ((BigIntVector) vector).setSafe(rowIndex, longValue);
                    break;
                case DOUBLE:
                    double doubleValue = cellNode.asDouble();
                    ((Float8Vector) vector).setSafe(rowIndex, doubleValue);
                    break;
                case BOOLEAN:
                    boolean boolValue = cellNode.asBoolean();
                    ((BitVector) vector).setSafe(rowIndex, boolValue ? 1 : 0);
                    break;
                case TIMESTAMP:
                    long timestamp = cellNode.asLong();
                    ((TimeStampMilliTZVector) vector).setSafe(rowIndex, timestamp);
                    break;
                default:
                    logger.warn("Unhandled data type: {}", dataType);
            }
        } catch (Exception e) {
            logger.error("Error setting {} value at index {}: {}", dataType, rowIndex, e.getMessage(), e);
            throw e;
        }
    }
    
    private UnifiedDataTable createEmptyTable(Map<String, DataType> fieldMappings) {
        // Create the schema based on fieldMappings (using logical field names, not physical column names)
        Map<String, Field> arrowFields = DataTypeMapper.toArrowFields(fieldMappings);
        List<Field> fields = new ArrayList<>(arrowFields.values());
        Schema schema = new Schema(fields, null);
        
        // Log the schema for debugging
        logger.debug("Created Arrow schema with fields: {}", 
            fields.stream().map(Field::getName).collect(Collectors.toList()));
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator);
        vectorSchemaRoot.allocateNew();
        vectorSchemaRoot.setRowCount(0);
        return new UnifiedDataTable(vectorSchemaRoot);
    }

    @Override
    public UnifiedDataTable parse(InputStream data, Map<String, DataType> fieldMappings, String dataPath, Map<String, String> columnAlias, List<Predicate> predicates) {
        logger.info("Starting JSON parsing with dataPath: '{}', fieldMappings: {}, columnAlias: {}, predicates: {}", 
            dataPath, fieldMappings, columnAlias, predicates);
        
        Objects.requireNonNull(data, "Input stream cannot be null");
        Objects.requireNonNull(fieldMappings, "Field mappings cannot be null");
        
        // Use the columnAlias map if provided, otherwise use an empty map
        Map<String, String> aliasMap = columnAlias != null ? columnAlias : Collections.emptyMap();
        
        // Create a mapping from logical field names to physical column names
        Map<String, String> fieldToColumnMap = new HashMap<>();
        
        // For each field, determine its physical column name (using alias if available)
        for (String fieldName : fieldMappings.keySet()) {
            // If there's an alias for this field, use it; otherwise use the field name as is
            fieldToColumnMap.put(fieldName, aliasMap.getOrDefault(fieldName, fieldName));
        }
        
        if (fieldMappings.isEmpty()) {
            String errorMsg = "Field mappings cannot be empty";
            logger.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }
        
        try {
            // Log the input data for debugging
            String inputData = inputStreamToString(data);
            logger.debug("Raw JSON input data (first 1000 chars): {}", 
                        inputData.length() > 1000 ? inputData.substring(0, 1000) + "..." : inputData);
            
            if (inputData == null || inputData.trim().isEmpty()) {
                String errorMsg = "Input data is null or empty";
                logger.error(errorMsg);
                throw new IllegalArgumentException(errorMsg);
            }
            
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(inputData);
            
            if (rootNode == null) {
                String errorMsg = "Failed to parse JSON data - root node is null";
                logger.error(errorMsg);
                throw new IllegalStateException(errorMsg);
            }
            
            logger.debug("Successfully parsed JSON. Root node type: {}", rootNode.getNodeType());
            
            // If dataPath is provided, use it to navigate to the data array
            JsonNode dataNode;
            if (dataPath != null && !dataPath.isEmpty()) {
                JsonPointer pointer = JsonPointer.compile(dataPath);
                dataNode = rootNode.at(pointer);
                logger.debug("Navigated to data path '{}', found node type: {}", dataPath, dataNode.getNodeType());
            } else {
                dataNode = rootNode;
            }
            
            if (!dataNode.isArray()) {
                logger.error("Data at path '{}' is not an array. Node type: {}", dataPath, dataNode.getNodeType());
                return createEmptyTable(fieldMappings);
            }
            
            // Log the first item's structure for debugging
            if (dataNode.size() > 0) {
                logger.debug("First item structure: {}", dataNode.get(0).toPrettyString());
            }

            // Filter nodes before processing
            List<JsonNode> filteredNodes = new ArrayList<>();
            for (JsonNode itemNode : dataNode) {
                if (nodeMatches(itemNode, predicates, fieldToColumnMap)) {
                    filteredNodes.add(itemNode);
                }
            }
            
            int rowCount = filteredNodes.size();
            logger.debug("Found {} elements after filtering", rowCount);
            
            if (rowCount == 0) {
                logger.warn("Empty array after filtering JSON data");
                return createEmptyTable(fieldMappings);
            }
            
            // Get the fields from the mapper
            Map<String, Field> arrowFields = DataTypeMapper.toArrowFields(fieldMappings);
            logger.debug("Created {} arrow fields", arrowFields.size());
            
            // Create schema and vector schema root
            List<Field> fields = fieldMappings.keySet().stream()
                .map(arrowFields::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
                
            if (fields.size() != fieldMappings.size()) {
                logger.error("Field count mismatch. Expected: {}, Actual: {}", fieldMappings.size(), fields.size());
                throw new IllegalStateException("Failed to create all required fields");
            }
            
            Schema schema = new Schema(fields, null);
            VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator);
            vectorSchemaRoot.allocateNew();
            
            logger.debug("Created VectorSchemaRoot with schema: {}", schema);
            
            // Process each row
            for (int i = 0; i < rowCount; i++) {
                JsonNode itemNode = filteredNodes.get(i);
                if (!itemNode.isObject()) {
                    logger.warn("Skipping non-object row at index {}", i);
                    continue;
                }
                
                // Process each field in the fieldMappings
                for (Map.Entry<String, DataType> entry : fieldMappings.entrySet()) {
                    String fieldName = entry.getKey();
                    DataType dataType = entry.getValue();
                    
                    // Get the physical column name (using alias if available)
                    String physicalColumn = fieldToColumnMap.get(fieldName);
                    
                    // Get the field value from the JSON object using the physical column name
                    JsonNode valueNode = itemNode.path(physicalColumn);
                    
                    // Log if the field is missing or null
                    if (valueNode.isMissingNode() || valueNode.isNull()) {
                        logger.trace("Field '{}' (mapped from '{}') is missing or null in item {}", 
                            physicalColumn, fieldName, i);
                    }
                    
                    try {
                        FieldVector vector = vectorSchemaRoot.getVector(fieldName);
                        if (vector == null) {
                            logger.error("No vector found for field: {}", fieldName);
                            continue;
                        }
                        
                        setVectorValue(vector, dataType, i, valueNode);
                    } catch (Exception e) {
                        logger.error("Error processing field '{}' at row {}: {}", fieldName, i, e.getMessage(), e);
                        if (e instanceof RuntimeException) {
                            throw (RuntimeException) e;
                        }
                        throw new RuntimeException(String.format("Error processing field '%s' at row %d", fieldName, i), e);
                    }
                }
            }
            
            vectorSchemaRoot.setRowCount(rowCount);
            logger.debug("Successfully parsed {} rows", rowCount);
            
            return new UnifiedDataTable(vectorSchemaRoot);

        } catch (Exception e) {
            logger.error("Failed to parse JSON data: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to parse JSON data: " + e.getMessage(), e);
        }
    }

    private boolean nodeMatches(JsonNode node, List<Predicate> predicates, Map<String, String> fieldToColumnMap) {
        if (predicates == null || predicates.isEmpty()) {
            return true;
        }
        for (Predicate predicate : predicates) {
            String physicalColumn = fieldToColumnMap.get(predicate.columnName());
            if (physicalColumn == null) {
                return false; // Column for predicate not mapped
            }
            JsonNode valueNode = node.path(physicalColumn);
            if (valueNode.isMissingNode() || valueNode.isNull()) {
                return false; // Null values don't match for now
            }
            // Simplified comparison, assumes EQUALS and string matching
            if (!valueNode.asText().equals(predicate.value().toString())) {
                return false; // Predicate does not match
            }
        }
        return true;
    }
}
