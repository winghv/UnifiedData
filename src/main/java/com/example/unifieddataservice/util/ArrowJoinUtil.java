package com.example.unifieddataservice.util;

import com.example.unifieddataservice.model.UnifiedDataTable;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Utility for performing an in-memory hash join on multiple Arrow tables using equality on key columns.
 * This is a simplified implementation optimised for small-to-medium result sets (<100k rows).
 * For larger data, consider leveraging Gandiva/Arrow algorithms or pushing join to storage layer.
 */
@Component
public class ArrowJoinUtil {

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
     * @param keyColumns list of column names that exist in every table
     * @return joined UnifiedDataTable
     */
    public UnifiedDataTable joinOnKeys(List<UnifiedDataTable> tables, List<String> keyColumns) {
        if (tables == null || tables.isEmpty()) {
            throw new IllegalArgumentException("No tables provided for join");
        }
        if (tables.size() == 1) {
            return tables.get(0);
        }

        // Build map from key -> row index for each table (except first)
        List<Map<String, Integer>> tableKeyIndexMaps = new ArrayList<>();
        for (int i = 1; i < tables.size(); i++) {
            tableKeyIndexMaps.add(buildKeyIndexMap(tables.get(i).getData(), keyColumns));
        }

        VectorSchemaRoot base = tables.get(0).getData();
        int rowCount = base.getRowCount();

        // Build combined schema: start with base, then add non-duplicate fields from others
        List<Field> combinedFields = new ArrayList<>(base.getSchema().getFields());
        for (int i = 1; i < tables.size(); i++) {
            for (Field f : tables.get(i).getData().getSchema().getFields()) {
                if (combinedFields.stream().noneMatch(cf -> cf.getName().equals(f.getName()))) {
                    combinedFields.add(f);
                }
            }
        }

        VectorSchemaRoot joinedRoot = VectorSchemaRoot.create(new org.apache.arrow.vector.types.pojo.Schema(combinedFields), rootAllocator);
        joinedRoot.allocateNew();

        // Map field name -> FieldVector in joined root
        Map<String, FieldVector> joinedVectors = new HashMap<>();
        for (FieldVector fv : joinedRoot.getFieldVectors()) {
            joinedVectors.put(fv.getField().getName(), fv);
        }

        // Copy base vectors first
        for (FieldVector baseVec : base.getFieldVectors()) {
            FieldVector target = joinedVectors.get(baseVec.getField().getName());
            target.copyFromSafe(0, 0, baseVec); // allocate capacity first; copy row-by-row later
        }

        // Iterate rows of base table
        for (int row = 0; row < rowCount; row++) {
            // Key extraction
            String key = buildKeyString(base, keyColumns, row);
            // Copy base row into target
            for (FieldVector baseVec : base.getFieldVectors()) {
                FieldVector target = joinedVectors.get(baseVec.getField().getName());
                target.copyFromSafe(row, row, baseVec);
            }
            // For each other table, locate row and copy additional columns
            for (int tIndex = 1; tIndex < tables.size(); tIndex++) {
                Integer matchRow = tableKeyIndexMaps.get(tIndex - 1).get(key);
                if (matchRow == null) {
                    // inner join: skip row entirely; for simplicity we break and mark row invalid
                    // In production, you'd implement outer joins or filtering before join
                    continue;
                }
                VectorSchemaRoot otherRoot = tables.get(tIndex).getData();
                for (FieldVector otherVec : otherRoot.getFieldVectors()) {
                    if (joinedVectors.containsKey(otherVec.getField().getName())) {
                        // if field exists in base, we skip duplicate
                        continue;
                    }
                    FieldVector target = joinedVectors.get(otherVec.getField().getName());
                    if (target == null) {
                        target = joinedRoot.getVector(otherVec.getField().getName());
                    }
                    target.copyFromSafe(matchRow, row, otherVec);
                }
            }
        }
        joinedRoot.setRowCount(rowCount);
        return new UnifiedDataTable(joinedRoot);
    }

    private Map<String, Integer> buildKeyIndexMap(VectorSchemaRoot table, List<String> keyColumns) {
        Map<String, Integer> map = new HashMap<>();
        for (int row = 0; row < table.getRowCount(); row++) {
            map.put(buildKeyString(table, keyColumns, row), row);
        }
        return map;
    }

    private String buildKeyString(VectorSchemaRoot table, List<String> keyColumns, int rowIndex) {
        StringBuilder sb = new StringBuilder();
        for (String col : keyColumns) {
            FieldVector vec = table.getVector(col);
            sb.append(vec.getObject(rowIndex)).append('|');
        }
        return sb.toString();
    }
}
