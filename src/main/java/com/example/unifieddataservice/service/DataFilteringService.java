package com.example.unifieddataservice.service;

import com.example.unifieddataservice.model.Predicate;
import com.example.unifieddataservice.model.UnifiedDataTable;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;



@Service
public class DataFilteringService {
    private static final Logger logger = LoggerFactory.getLogger(DataFilteringService.class);

    @Autowired
    private RootAllocator allocator;

    public UnifiedDataTable applyPredicates(UnifiedDataTable table, List<Predicate> predicates) {
        if (predicates == null || predicates.isEmpty()) {
            return table;
        }

        VectorSchemaRoot root = table.getData();
        List<Integer> matchingRows = new ArrayList<>();

        for (int i = 0; i < root.getRowCount(); i++) {
            boolean matchesAll = true;
            for (Predicate p : predicates) {
                if (!rowMatchesPredicate(root, i, p)) {
                    matchesAll = false;
                    break;
                }
            }
            if (matchesAll) {
                matchingRows.add(i);
            }
        }

        if (matchingRows.isEmpty()) {
            return createEmptyTable(table);
        }
        return createFilteredTable(table, matchingRows);
    }

    private boolean rowMatchesPredicate(VectorSchemaRoot root, int row, Predicate predicate) {
        FieldVector vector = root.getVector(predicate.columnName());
        if (vector == null) {
            return false; // Column not found, cannot match.
        }

        Object value = vector.getObject(row);
        Object predicateValue = predicate.value();

        if (value == null) {
            return predicateValue == null; // Or handle as per SQL NULL semantics
        }

        // Simplified comparison logic. Production code would need robust type handling.
        switch (predicate.operator()) {
            case EQUALS:
                return value.toString().equals(predicateValue.toString());
            case NOT_EQUALS:
                return !value.toString().equals(predicateValue.toString());
            // Implement other operators (>, <, etc.) here.
            default:
                logger.warn("Unsupported operator in fallback filter: {}", predicate.operator());
                return false;
        }
    }

    private UnifiedDataTable createEmptyTable(UnifiedDataTable original) {
        VectorSchemaRoot originalRoot = original.getData();
        Schema schema = originalRoot.getSchema();
        VectorSchemaRoot emptyRoot = VectorSchemaRoot.create(schema, allocator);
        emptyRoot.allocateNew();
        emptyRoot.setRowCount(0);
        return new UnifiedDataTable(original.getTableName(), emptyRoot, schema);
    }

    private UnifiedDataTable createFilteredTable(UnifiedDataTable original, List<Integer> matchingRows) {
        VectorSchemaRoot originalRoot = original.getData();
        Schema schema = originalRoot.getSchema();
        VectorSchemaRoot filteredRoot = VectorSchemaRoot.create(schema, allocator);
        filteredRoot.allocateNew();

        for (int newRow = 0; newRow < matchingRows.size(); newRow++) {
            int originalRow = matchingRows.get(newRow);
            for (FieldVector originalVector : originalRoot.getFieldVectors()) {
                filteredRoot.getVector(originalVector.getField().getName()).copyFromSafe(originalRow, newRow, originalVector);
            }
        }
        filteredRoot.setRowCount(matchingRows.size());
        return new UnifiedDataTable(original.getTableName(), filteredRoot, schema);
    }
}

