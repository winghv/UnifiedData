package com.example.unifieddataservice.service.parser;

import com.example.unifieddataservice.model.DataType;
import com.example.unifieddataservice.model.MetricInfo;
import com.example.unifieddataservice.model.UnifiedDataTable;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CsvDataParserTest {

    private CsvDataParser parser;
    private MetricInfo metricInfo;

    @BeforeEach
    void setUp() {
        parser = new CsvDataParser();
        metricInfo = new MetricInfo();
        // Create field mappings map
        Map<String, DataType> fieldMappings = new HashMap<>();
        fieldMappings.put("user_id", DataType.STRING);
        fieldMappings.put("event_count", DataType.LONG);
        fieldMappings.put("value", DataType.DOUBLE);
        metricInfo.setFieldMappings(fieldMappings);
    }

    @Test
    void testParse_Success() throws Exception {
        // Arrange
        String csv = "user_id,event_count,value\n" +
                     "alice,10,99.9\n" +
                     "bob,25,85.5\n" +
                     "charlie,,75.0\n"; // Empty value for event_count
        InputStream inputStream = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));

        // Act
        try (UnifiedDataTable result = parser.parse(inputStream, metricInfo.getFieldMappings(), null)) {
            // Assert
            assertNotNull(result);
            VectorSchemaRoot root = result.getData();
            Schema schema = root.getSchema();
            assertEquals(3, schema.getFields().size());  // user_id, event_count, value
            assertEquals(3, root.getRowCount());

            VarCharVector userVector = (VarCharVector) root.getVector("user_id");
            BigIntVector countVector = (BigIntVector) root.getVector("event_count");
            Float8Vector valueVector = (Float8Vector) root.getVector("value");

            // Verify first row
            assertEquals("alice", userVector.getObject(0).toString());
            assertEquals(10L, countVector.get(0));
            assertEquals(99.9, valueVector.get(0), 0.001);
            
            // Verify second row
            assertEquals("bob", userVector.getObject(1).toString());
            assertEquals(25L, countVector.get(1));
            assertEquals(85.5, valueVector.get(1), 0.001);
            
            // Verify third row (with null value)
            assertEquals("charlie", userVector.getObject(2).toString());
            assertTrue(countVector.isNull(2));
            assertEquals(75.0, valueVector.get(2), 0.001);
        }
    }

    @Test
    void testParse_MalformedNumber() throws Exception {
        // Arrange
        String csv = "user_id,event_count,value\n" +
                     "dave,N/A,99.9\n";
        InputStream inputStream = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));

        // Act
        try (UnifiedDataTable result = parser.parse(inputStream, metricInfo.getFieldMappings(), null)) {
            // Assert
            VectorSchemaRoot root = result.getData();
            assertEquals(1, root.getRowCount());
            BigIntVector countVector = (BigIntVector) root.getVector("event_count");
            assertTrue(countVector.isNull(0)); // Should be null due to NumberFormatException
        }
    }

    @Test
    void testParse_MissingColumnInCsv() {
        // Arrange
        String csv = "user_id,value\n" + // event_count is missing
                     "eve,12.3\n";
        InputStream inputStream = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));

        // Act & Assert
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            parser.parse(inputStream, metricInfo.getFieldMappings(), null);
        });
        assertEquals("Required header 'event_count' not found in CSV.", exception.getMessage());
    }

    @Test
    void testParse_NullInput() {
        // Act & Assert
        NullPointerException exception = assertThrows(NullPointerException.class, () -> {
            parser.parse(null, metricInfo.getFieldMappings(), null);
        });
        assertEquals("Input stream cannot be null", exception.getMessage());
    }

    @Test
    void testParse_EmptyCsv() {
        // Arrange
        String csv = "";
        InputStream inputStream = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            parser.parse(inputStream, metricInfo.getFieldMappings(), null);
        });
        assertEquals("CSV input is empty or missing headers", exception.getMessage());
    }

    @Test
    void testParse_EmptyInput() throws Exception {
        // Arrange
        String csv = "user_id,event_count,value\n"; // Header only
        InputStream inputStream = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));

        // Act
        try (UnifiedDataTable result = parser.parse(inputStream, metricInfo.getFieldMappings(), null)) {
            // Assert
            assertNotNull(result);
            assertEquals(0, result.getData().getRowCount());
        }
    }
    
    @Test
    void testParse_EmptyCsvWithNewline() {
        // Arrange
        String csv = "\n"; // Newline only
        InputStream inputStream = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            parser.parse(inputStream, metricInfo.getFieldMappings(), null);
        });
        assertEquals("CSV input is empty or missing headers", exception.getMessage());
    }
}
