package com.example.unifieddataservice.service.parser;

import com.example.unifieddataservice.model.DataType;
import com.example.unifieddataservice.model.Predicate;
import com.example.unifieddataservice.model.UnifiedDataTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class CsvDataParser implements DataParser {
    private final RootAllocator rootAllocator;


    @Autowired
    public CsvDataParser(RootAllocator rootAllocator) {
        this.rootAllocator = rootAllocator;
    }

    @Override
    public UnifiedDataTable parse(InputStream data, Map<String, DataType> fieldMappings, String dataPath, Map<String, String> columnAlias, List<com.example.unifieddataservice.model.Predicate> predicates) {
        // Use the columnAlias map if provided, otherwise use an empty map
        Map<String, String> aliasMap = columnAlias != null ? columnAlias : Collections.emptyMap();

        // Create a mapping from logical field names to physical column names
        Map<String, String> fieldToColumnMap = new HashMap<>();

        // For each field, determine its physical column name (using alias if available)
        for (String fieldName : fieldMappings.keySet()) {
            // If there's an alias for this field, use it; otherwise use the field name as is
            fieldToColumnMap.put(fieldName, aliasMap.getOrDefault(fieldName, fieldName));
        }

        // Validate input parameters
        Objects.requireNonNull(data, "Input stream cannot be null");
        Objects.requireNonNull(fieldMappings, "Field mappings cannot be null");

        if (fieldMappings.isEmpty()) {
            throw new IllegalArgumentException("Field mappings cannot be empty");
        }

        try (InputStreamReader reader = new InputStreamReader(data, StandardCharsets.UTF_8);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

            // Check for empty input (no headers or no data)
            if (csvParser.getHeaderMap().isEmpty()) {
                throw new IllegalArgumentException("CSV input is empty or missing headers");
            }

            Map<String, Integer> headerMap = csvParser.getHeaderMap();

            // Validate all required fields are present in the CSV
            for (Map.Entry<String, String> entry : fieldToColumnMap.entrySet()) {
                String logicalField = entry.getKey();
                String physicalColumn = entry.getValue();

                if (!headerMap.containsKey(physicalColumn)) {
                    throw new IllegalArgumentException(String.format(
                            "Required column '%s' (mapped from field '%s') not found in CSV. Available columns: %s",
                            physicalColumn, logicalField, headerMap.keySet()));
                }
            }

            // Create the map of fields once and reuse it
            Map<String, Field> arrowFields = DataTypeMapper.toArrowFields(fieldMappings);
            List<Field> fields = fieldMappings.keySet().stream()
                    .map(arrowFields::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            Schema schema = new Schema(fields, null);
            VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator);
            
            // Filter records before processing
            List<CSVRecord> filteredRecords = new ArrayList<>();
            for (CSVRecord record : csvParser) {
                if (recordMatches(record, predicates, fieldToColumnMap)) {
                    filteredRecords.add(record);
                }
            }

            vectorSchemaRoot.allocateNew();

            for (int i = 0; i < filteredRecords.size(); i++) {
                CSVRecord record = filteredRecords.get(i);
                final int rowIndex = i;

                fieldMappings.forEach((fieldName, dataType) -> {
                    try {
                        FieldVector vector = vectorSchemaRoot.getVector(fieldName);
                        String physicalColumn = fieldToColumnMap.get(fieldName);
                        String value = record.isMapped(physicalColumn) ? record.get(physicalColumn) : null;

                        if (value != null && !value.trim().isEmpty()) {
                            try {
                                switch (dataType) {
                                    case STRING:
                                        ((VarCharVector) vector).setSafe(rowIndex, value.getBytes(StandardCharsets.UTF_8));
                                        break;
                                    case LONG:
                                        try {
                                            ((BigIntVector) vector).setSafe(rowIndex, Long.parseLong(value.trim()));
                                        } catch (NumberFormatException e) {
                                            vector.setNull(rowIndex);
                                        }
                                        break;
                                    case DOUBLE:
                                        try {
                                            ((Float8Vector) vector).setSafe(rowIndex, Double.parseDouble(value.trim()));
                                        } catch (NumberFormatException e) {
                                            vector.setNull(rowIndex);
                                        }
                                        break;
                                    case BOOLEAN:
                                        ((BitVector) vector).setSafe(rowIndex, Boolean.parseBoolean(value.trim()) ? 1 : 0);
                                        break;
                                    case TIMESTAMP:
                                        try {
                                            long timestamp = Long.parseLong(value.trim());
                                            ((TimeStampMilliTZVector) vector).setSafe(rowIndex, timestamp);
                                        } catch (NumberFormatException e) {
                                            vector.setNull(rowIndex);
                                        }
                                        break;
                                    default:
                                        throw new IllegalStateException("Unsupported data type: " + dataType);
                                }
                            } catch (Exception e) {
                                vector.setNull(rowIndex);
                            }
                        } else {
                            vector.setNull(rowIndex);
                        }
                    } catch (Exception e) {
                        // Skip field on error
                    }
                });
            }

            vectorSchemaRoot.setRowCount(filteredRecords.size());
            return new UnifiedDataTable(vectorSchemaRoot);

        } catch (IllegalArgumentException e) {
            // Re-throw validation exceptions directly
            throw e;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read CSV data: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse CSV data: " + e.getMessage(), e);
        }
    }

    private boolean recordMatches(CSVRecord record, List<Predicate> predicates, Map<String, String> fieldToColumnMap) {
        if (predicates == null || predicates.isEmpty()) {
            return true;
        }
        for (Predicate predicate : predicates) {
            String physicalColumn = fieldToColumnMap.get(predicate.columnName());
            if (physicalColumn == null || !record.isMapped(physicalColumn)) {
                return false; // Column for predicate not found in record
            }
            String recordValue = record.get(physicalColumn);
            if (recordValue == null) {
                return false; // Null values don't match for now
            }
            // Simplified comparison, assumes EQUALS and string matching
            if (!recordValue.equals(predicate.value().toString())) {
                return false; // Predicate does not match
            }
        }
        return true;
    }
}
