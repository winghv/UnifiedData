package com.example.unifieddataservice.util;

import com.example.unifieddataservice.model.UnifiedDataTable;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Utility for performing an in-memory hash join on multiple Arrow tables using equality on key columns.
 * This is a simplified implementation optimised for small-to-medium result sets (<100k rows).
 * For larger data, consider leveraging Gandiva/Arrow algorithms or pushing join to storage layer.
 */
@Component
public class ArrowJoinUtil {
    private static final Logger logger = LoggerFactory.getLogger(ArrowJoinUtil.class);
    private final RootAllocator rootAllocator;

    @Autowired
    public ArrowJoinUtil(RootAllocator rootAllocator) {
        this.rootAllocator = rootAllocator;
    }

    /**
     * Perform inner join of provided tables on specified key columns.
     * The first table is treated as the build (left) side, preserving its row order.
     *
     * @param tables list of UnifiedDataTable, size >= 1
     * @param keyColumns list of logical column names that exist in every table
     * @param fieldMappings map of logical field names to physical field names
     * @return joined UnifiedDataTable
     */
    public UnifiedDataTable joinOnKeys(List<UnifiedDataTable> tables, List<String> keyColumns, Map<String, String> fieldMappings) {
        logger.info("Starting joinOnKeys with {} tables and key columns: {}", tables.size(), keyColumns);

        if (tables == null || tables.isEmpty()) {
            throw new IllegalArgumentException("No tables provided for join");
        }

        if (fieldMappings == null) {
            fieldMappings = Collections.emptyMap();
        }

        final Map<String, String> reverseMappings = new HashMap<>();
        fieldMappings.forEach((logical, physical) -> reverseMappings.put(physical, logical));

        for (int i = 0; i < tables.size(); i++) {
            VectorSchemaRoot data = tables.get(i).getData();
            logger.info("Table {} schema: {}", i, data.getSchema().getFields().stream()
                    .map(Field::toString)
                    .collect(Collectors.joining(", ")));
        }

        if (tables.size() == 1) {
            return tables.get(0);
        }

        List<Map<String, Integer>> tableKeyIndexMaps = new ArrayList<>();
        for (int i = 1; i < tables.size(); i++) {
            logger.info("Building key index map for table {}", i);
            VectorSchemaRoot currentTable = tables.get(i).getData();
            Map<String, Integer> keyIndexMap = buildKeyIndexMap(currentTable, keyColumns, fieldMappings);
            logger.info("Built key index map with {} entries for table {}", keyIndexMap.size(), i);
            tableKeyIndexMaps.add(keyIndexMap);
        }

        VectorSchemaRoot base = tables.get(0).getData();
        int rowCount = base.getRowCount();
        logger.info("Base table has {} rows and {} columns", rowCount, base.getFieldVectors().size());

        List<Field> combinedFields = new ArrayList<>();
        Map<String, String> fieldNameMapping = new HashMap<>();

        for (Field field : base.getSchema().getFields()) {
            combinedFields.add(field);
            fieldNameMapping.put(field.getName(), field.getName());
        }

        for (int i = 1; i < tables.size(); i++) {
            VectorSchemaRoot currentTable = tables.get(i).getData();
            for (Field field : currentTable.getSchema().getFields()) {
                String originalName = field.getName();
                if (combinedFields.stream().noneMatch(f -> f.getName().equals(originalName))) {
                    combinedFields.add(field);
                }
            }
        }

        Schema finalSchema = new Schema(combinedFields);
        VectorSchemaRoot joinedRoot = VectorSchemaRoot.create(finalSchema, rootAllocator);
        joinedRoot.allocateNew();

        Map<String, FieldVector> joinedVectors = new HashMap<>();
        for (FieldVector fv : joinedRoot.getFieldVectors()) {
            joinedVectors.put(fv.getField().getName(), fv);
        }

        for (int row = 0; row < rowCount; row++) {
            String key = buildKeyString(base, keyColumns, row, fieldMappings);
            boolean matchFound = true;
            for (int tIndex = 1; tIndex < tables.size(); tIndex++) {
                if (!tableKeyIndexMaps.get(tIndex - 1).containsKey(key)) {
                    matchFound = false;
                    break;
                }
            }

            if (matchFound) {
                int newRowIndex = joinedRoot.getRowCount();
                for (FieldVector baseVec : base.getFieldVectors()) {
                    joinedVectors.get(baseVec.getField().getName()).copyFromSafe(row, newRowIndex, baseVec);
                }

                for (int tIndex = 1; tIndex < tables.size(); tIndex++) {
                    Integer matchRow = tableKeyIndexMaps.get(tIndex - 1).get(key);
                    VectorSchemaRoot otherRoot = tables.get(tIndex).getData();
                    for (FieldVector otherVec : otherRoot.getFieldVectors()) {
                        if (joinedVectors.containsKey(otherVec.getField().getName())) {
                            joinedVectors.get(otherVec.getField().getName()).copyFromSafe(matchRow, newRowIndex, otherVec);
                        }
                    }
                }
                joinedRoot.setRowCount(newRowIndex + 1);
            }
        }
        return new UnifiedDataTable(joinedRoot);
    }
    
    private String buildKeyString(VectorSchemaRoot root, List<String> keyColumns, int row, Map<String, String> fieldMappings) {
        StringBuilder sb = new StringBuilder();
        for (String logicalCol : keyColumns) {
            String physicalCol = fieldMappings.getOrDefault(logicalCol, logicalCol);
            FieldVector vector = root.getVector(physicalCol);
            if (vector == null) {
                throw new IllegalArgumentException("Key column '" + physicalCol + "' not found in vector schema root");
            }
            Object value = vector.getObject(row);
            sb.append(value != null ? value.toString() : "NULL").append('|');
        }
        return sb.toString();
    }
    
    private Map<String, Integer> buildKeyIndexMap(VectorSchemaRoot table, List<String> keyColumns, Map<String, String> fieldMappings) {
        Map<String, Integer> keyIndexMap = new HashMap<>();
        if (table == null || keyColumns == null || keyColumns.isEmpty()) {
            return keyIndexMap;
        }

        for (int i = 0; i < table.getRowCount(); i++) {
            String key = buildKeyString(table, keyColumns, i, fieldMappings);
            if (!keyIndexMap.containsKey(key)) {
                keyIndexMap.put(key, i);
            }
        }
        return keyIndexMap;
    }
}
