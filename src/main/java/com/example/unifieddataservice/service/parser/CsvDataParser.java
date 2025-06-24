package com.example.unifieddataservice.service.parser;

import com.example.unifieddataservice.model.DataType;
import com.example.unifieddataservice.model.UnifiedDataTable;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Component
public class CsvDataParser implements DataParser {

    private static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    @Override
    public UnifiedDataTable parse(InputStream data, Map<String, DataType> fieldMappings, String dataPath) {
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
            for (String requiredField : fieldMappings.keySet()) {
                if (!headerMap.containsKey(requiredField)) {
                    throw new IllegalArgumentException("Required header '" + requiredField + "' not found in CSV.");
                }
            }

            List<Field> fields = fieldMappings.entrySet().stream()
                .map(entry -> DataTypeMapper.toArrowFields(fieldMappings).get(entry.getKey()))
                .collect(Collectors.toList());

            Schema schema = new Schema(fields, null);
            VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
            vectorSchemaRoot.allocateNew();

            List<CSVRecord> records = csvParser.getRecords();
            for (int i = 0; i < records.size(); i++) {
                CSVRecord record = records.get(i);
                final int rowIndex = i;
                fieldMappings.forEach((fieldName, dataType) -> {
                    try {
                        FieldVector vector = vectorSchemaRoot.getVector(fieldName);
                        String value = record.get(fieldName);
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
                                            // Set to null if number is malformed
                                            vector.setNull(rowIndex);
                                        }
                                        break;
                                    case DOUBLE:
                                        try {
                                            ((Float8Vector) vector).setSafe(rowIndex, Double.parseDouble(value.trim()));
                                        } catch (NumberFormatException e) {
                                            // Set to null if number is malformed
                                            vector.setNull(rowIndex);
                                        }
                                        break;
                                    case BOOLEAN:
                                        ((BitVector) vector).setSafe(rowIndex, Boolean.parseBoolean(value.trim()) ? 1 : 0);
                                        break;
                                }
                            } catch (Exception e) {
                                // Handle any field-level parsing errors by setting to null
                                vector.setNull(rowIndex);
                            }
                        } else {
                            // Set to null if value is empty or null
                            vector.setNull(rowIndex);
                        }
                    } catch (Exception e) {
                        // Skip this field if there's an error
                    }
                });
            }

            vectorSchemaRoot.setRowCount(records.size());
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
}
