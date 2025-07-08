package com.example.unifieddataservice.service.parser;

import com.example.unifieddataservice.model.DataType;
import com.example.unifieddataservice.model.MetricInfo;
import com.example.unifieddataservice.model.UnifiedDataTable;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;

import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonDataParserTest {

    private JsonDataParser parser;
    private MetricInfo metricInfo;
    private RootAllocator rootAllocator;

    @BeforeEach
    void setUp() {
        rootAllocator = new RootAllocator(Long.MAX_VALUE);
        parser = new JsonDataParser(rootAllocator);
        metricInfo = new MetricInfo();
        // Setup default valid metric info
        metricInfo.setDataPath("/data");
        // Create field mappings map with LinkedHashMap to maintain order
        Map<String, DataType> fieldMappings = new LinkedHashMap<>();
        fieldMappings.put("user", DataType.STRING);
        fieldMappings.put("age", DataType.LONG);
        fieldMappings.put("score", DataType.DOUBLE);
        fieldMappings.put("timestamp", DataType.TIMESTAMP);
        metricInfo.setFieldMappings(fieldMappings);
    }

    @AfterEach
    void tearDown() {
        // Any cleanup if needed
    }

    @Test
    void testParse_Success() throws Exception {
        // Arrange
        String json = "{\"data\":[" +
                "{\"user\":\"Alice\",\"age\":30,\"score\":85.5,\"timestamp\":1672531200000}," +
                "{\"user\":\"Bob\",\"age\":25,\"score\":90.1,\"timestamp\":1672617600000}," +
                "{\"user\":\"Charlie\",\"age\":null,\"score\":75.0,\"timestamp\":null}" +
                "]}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));

        // Act
        try (UnifiedDataTable result = parser.parse(inputStream, metricInfo.getFieldMappings(), metricInfo.getDataPath())) {
            // Assert
            assertNotNull(result);
            VectorSchemaRoot root = result.getData();
            Schema schema = root.getSchema();
            assertEquals(4, schema.getFields().size());  // user, age, score, timestamp
            assertEquals(3, root.getRowCount());

            // Check Schema
            assertEquals("user", root.getSchema().getFields().get(0).getName());
            assertEquals("age", root.getSchema().getFields().get(1).getName());
            assertEquals("score", root.getSchema().getFields().get(2).getName());
            assertEquals("timestamp", root.getSchema().getFields().get(3).getName());

            // Verify data in the first row
            VarCharVector userVector = (VarCharVector) root.getVector("user");
            BigIntVector ageVector = (BigIntVector) root.getVector("age");
            Float8Vector scoreVector = (Float8Vector) root.getVector("score");
            TimeStampMilliTZVector tsVector = (TimeStampMilliTZVector) root.getVector("timestamp");
            
            // Verify first row data
            assertEquals("Alice", userVector.getObject(0).toString());
            assertEquals(30L, ageVector.get(0));
            assertEquals(85.5, scoreVector.get(0), 0.001);
            assertFalse(tsVector.isNull(0));
            assertEquals(1672531200000L, tsVector.get(0));
            
            // Verify second row data
            assertEquals("Bob", userVector.getObject(1).toString());
            assertEquals(25L, ageVector.get(1));
            assertEquals(90.1, scoreVector.get(1), 0.001);
            assertFalse(tsVector.isNull(1));
            assertEquals(1672617600000L, tsVector.get(1));
            
            // Verify third row data (with null age and timestamp)
            assertEquals("Charlie", userVector.getObject(2).toString());
            assertTrue(ageVector.isNull(2));
            assertEquals(75.0, scoreVector.get(2), 0.001);
            assertTrue(tsVector.isNull(2));
        }
    }

    @Test
    void testParse_InvalidDataPath() {
        // Arrange
        String json = "{\"wrong_path\":[]}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
        String invalidDataPath = "/nonexistent";

        // Act & Assert
        Exception exception = assertThrows(RuntimeException.class, () -> {
            parser.parse(inputStream, metricInfo.getFieldMappings(), invalidDataPath);
        });
        assertEquals("Data path '/nonexistent' not found in JSON", exception.getCause().getMessage());
    }

    @Test
    void testParse_NullInput() {
        // Act & Assert
        assertThrows(NullPointerException.class, () -> {
            try (UnifiedDataTable ignored = parser.parse(null, metricInfo.getFieldMappings(), metricInfo.getDataPath())) {
                fail("Expected exception was not thrown");
            }
        });
    }

    @Test
    void testParse_EmptyJson() {
        // Arrange
        String json = "{}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));

        // Act & Assert
        assertThrows(RuntimeException.class, () -> {
            try (UnifiedDataTable ignored = parser.parse(inputStream, metricInfo.getFieldMappings(), metricInfo.getDataPath())) {
                fail("Expected exception was not thrown");
            }
        });
    }

    @Test
    void testParse_MissingFieldInJson() throws Exception {
        // Arrange
        String json = "{\"data\":[" +
                "{\"user\":\"Alice\",\"score\":85.5}" + // age and ts are missing
                "]}";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));

        // Act
        try (UnifiedDataTable result = parser.parse(inputStream, metricInfo.getFieldMappings(), metricInfo.getDataPath())) {
            // Assert
            assertNotNull(result);
            VectorSchemaRoot root = result.getData();
            assertEquals(1, root.getRowCount());

            BigIntVector ageVector = (BigIntVector) root.getVector("age");
            TimeStampMilliTZVector tsVector = (TimeStampMilliTZVector) root.getVector("timestamp");

            assertTrue(ageVector.isNull(0));
            assertTrue(tsVector.isNull(0));
            assertEquals("Alice", ((VarCharVector) root.getVector("user")).getObject(0).toString());
            assertEquals(85.5, ((Float8Vector) root.getVector("score")).get(0), 0.001);
        }
    }
}
