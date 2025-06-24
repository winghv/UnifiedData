package com.example.unifieddataservice.service;

import com.example.unifieddataservice.model.UnifiedDataTable;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class DataFilteringServiceTest {

    private DataFilteringService dataFilteringService;
    private RootAllocator allocator;
    private UnifiedDataTable testTable;

    @BeforeEach
    void setUp() {
        dataFilteringService = new DataFilteringService();
        allocator = new RootAllocator(Long.MAX_VALUE);
        testTable = createTestTable();
    }

    @AfterEach
    void tearDown() {
        testTable.close();
        allocator.close();
    }

    private UnifiedDataTable createTestTable() {
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field score = new Field("score", FieldType.nullable(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)), null);
        Schema schema = new Schema(Arrays.asList(name, age, score));

        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

        VarCharVector nameVector = (VarCharVector) root.getVector("name");
        BigIntVector ageVector = (BigIntVector) root.getVector("age");
        Float8Vector scoreVector = (Float8Vector) root.getVector("score");

        root.allocateNew();

        nameVector.setSafe(0, "Alice".getBytes(StandardCharsets.UTF_8));
        ageVector.setSafe(0, 30L);
        scoreVector.setSafe(0, 85.5);

        nameVector.setSafe(1, "Bob".getBytes(StandardCharsets.UTF_8));
        ageVector.setSafe(1, 25L);
        scoreVector.setSafe(1, 92.0);

        nameVector.setSafe(2, "Charlie".getBytes(StandardCharsets.UTF_8));
        ageVector.setSafe(2, 30L);
        scoreVector.setSafe(2, 78.5);

        root.setRowCount(3);

        return new UnifiedDataTable(root);
    }

    @Test
    void testFilterStringEquals() {
        UnifiedDataTable result = dataFilteringService.filter(testTable, "name == 'Bob'");
        assertEquals(1, result.getData().getRowCount());
        assertEquals("Bob", result.getData().getVector("name").getObject(0).toString());
        result.close();
    }

    @Test
    void testFilterLongGreaterThan() {
        UnifiedDataTable result = dataFilteringService.filter(testTable, "age > 25");
        assertEquals(2, result.getData().getRowCount());
        result.close();
    }

    @Test
    void testFilterDoubleLessThanOrEqual() {
        UnifiedDataTable result = dataFilteringService.filter(testTable, "score <= 85.5");
        assertEquals(2, result.getData().getRowCount());
        result.close();
    }

    @Test
    void testFilterNotEquals() {
        UnifiedDataTable result = dataFilteringService.filter(testTable, "age != 25");
        assertEquals(2, result.getData().getRowCount());
        result.close();
    }

    @Test
    void testFilterNoMatch() {
        UnifiedDataTable result = dataFilteringService.filter(testTable, "name == 'David'");
        assertEquals(0, result.getData().getRowCount());
        result.close();
    }

    @Test
    void testFilterInvalidColumn() {
        assertThrows(IllegalArgumentException.class, () -> {
            dataFilteringService.filter(testTable, "invalid_column == 123");
        });
    }

    @Test
    void testFilterInvalidExpression() {
        assertThrows(IllegalArgumentException.class, () -> {
            dataFilteringService.filter(testTable, "age is not 25");
        });
    }

    @Test
    void testFilterNullOrBlankExpression() {
        UnifiedDataTable result1 = dataFilteringService.filter(testTable, null);
        assertSame(testTable, result1);

        UnifiedDataTable result2 = dataFilteringService.filter(testTable, "  ");
        assertSame(testTable, result2);
    }
}
