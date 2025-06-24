package com.example.unifieddataservice.service.parser;

import com.example.unifieddataservice.model.DataType;
import com.example.unifieddataservice.model.UnifiedDataTable;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Component
public class JsonDataParser implements DataParser {
    private static final Logger logger = LoggerFactory.getLogger(JsonDataParser.class);
    private static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    
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
        Map<String, Field> arrowFields = DataTypeMapper.toArrowFields(fieldMappings);
        List<Field> fields = new ArrayList<>(arrowFields.values());
        Schema schema = new Schema(fields, null);
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
        vectorSchemaRoot.allocateNew();
        vectorSchemaRoot.setRowCount(0);
        return new UnifiedDataTable(vectorSchemaRoot);
    }

    @Override
    public UnifiedDataTable parse(InputStream data, Map<String, DataType> fieldMappings, String dataPath) {
        logger.info("Starting JSON parsing with dataPath: '{}' and fieldMappings: {}", dataPath, fieldMappings);
        
        Objects.requireNonNull(data, "Input stream cannot be null");
        Objects.requireNonNull(fieldMappings, "Field mappings cannot be null");
        
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
            
            // If dataPath is provided, navigate to the target node
            JsonNode dataNode = rootNode;
            if (dataPath != null && !dataPath.isEmpty()) {
                logger.debug("Navigating to dataPath: {}", dataPath);
                JsonPointer pointer = JsonPointer.compile(dataPath);
                dataNode = rootNode.at(pointer);
                
                if (dataNode.isMissingNode()) {
                    String errorMsg = String.format("Data path '%s' not found in JSON", dataPath);
                    logger.error(errorMsg);
                    throw new IllegalArgumentException(errorMsg);
                }
                logger.debug("Node after applying dataPath - type: {}, isArray: {}", 
                           dataNode.getNodeType(), dataNode.isArray());
            }
            
            if (!dataNode.isArray()) {
                logger.error("JSON data must be an array. Found type: {}", dataNode.getNodeType());
                throw new IllegalArgumentException("JSON data must be an array. Found: " + dataNode.getNodeType());
            }
            
            int rowCount = dataNode.size();
            logger.debug("Found array with {} elements", rowCount);
            
            if (rowCount == 0) {
                logger.warn("Empty array found in JSON data");
                // Return an empty table with the correct schema
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
            VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
            vectorSchemaRoot.allocateNew();
            
            logger.debug("Created VectorSchemaRoot with schema: {}", schema);
            
            // Process each row
            for (int i = 0; i < rowCount; i++) {
                JsonNode rowNode = dataNode.get(i);
                if (!rowNode.isObject()) {
                    logger.warn("Skipping non-object row at index {}", i);
                    continue;
                }
                
                final int rowIndex = i;
                fieldMappings.forEach((fieldName, dataType) -> {
                    try {
                        FieldVector vector = vectorSchemaRoot.getVector(fieldName);
                        if (vector == null) {
                            logger.error("No vector found for field: {}", fieldName);
                            return;
                        }
                        
                        JsonNode cellNode = rowNode.get(fieldName);
                        if (cellNode == null || cellNode.isNull()) {
                            logger.trace("Null value for field {} at row {}", fieldName, rowIndex);
                            return;
                        }
                        
                        setVectorValue(vector, dataType, rowIndex, cellNode);
                    } catch (Exception e) {
                        logger.error("Error processing field '{}' at row {}: {}", fieldName, rowIndex, e.getMessage(), e);
                        if (e instanceof RuntimeException) {
                            throw (RuntimeException) e;
                        }
                        throw new RuntimeException(String.format("Error processing field '%s' at row %d", fieldName, rowIndex), e);
                    }
                });
            }
            
            vectorSchemaRoot.setRowCount(rowCount);
            logger.debug("Successfully parsed {} rows", rowCount);
            
            return new UnifiedDataTable(vectorSchemaRoot);

        } catch (Exception e) {
            logger.error("Failed to parse JSON data: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to parse JSON data: " + e.getMessage(), e);
        }
    }
}
