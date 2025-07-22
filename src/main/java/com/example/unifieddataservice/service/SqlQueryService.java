package com.example.unifieddataservice.service;

import com.example.unifieddataservice.model.MetricQueryPlan;
import com.example.unifieddataservice.model.TableDefinition;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.FieldVector;

import com.example.unifieddataservice.model.UnifiedDataTable;
import com.example.unifieddataservice.util.ArrowJoinUtil;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.apache.arrow.memory.RootAllocator;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import java.util.HexFormat;

/**
 * Parses simple SELECT statements and orchestrates metric fetch & join.
 * Supports: SELECT field1, field2 FROM table WHERE pk1 = 'x' AND pk2 = 20250101
 */
@Service
public class SqlQueryService {

    /**
     * Utility method to convert bytes to a hexadecimal string for debugging.
     */
    private static String bytesToHex(byte[] bytes) {
        if (bytes == null) return "null";
        return HexFormat.of().formatHex(bytes);
    }

    private static final Logger logger = LoggerFactory.getLogger(SqlQueryService.class);

    private final TableRegistry tableRegistry;
    private final MetricService metricService;
    private final ArrowJoinUtil arrowJoinUtil;

    @Autowired
    private RootAllocator allocator;

    public SqlQueryService(TableRegistry tableRegistry, MetricService metricService, ArrowJoinUtil arrowJoinUtil) {
        this.tableRegistry = tableRegistry;
        this.metricService = metricService;
        this.arrowJoinUtil = arrowJoinUtil;
    }

    @org.springframework.cache.annotation.Cacheable(value = "queryResults", key = "#sql", sync = true)
    public UnifiedDataTable query(String sql) {
        MetricQueryPlan plan = parseSql(sql);

        // Fetch metrics in parallel
        Set<String> uniqueMetricNames = new HashSet<>(plan.getFieldMetricMapping().values());
        Map<String, CompletableFuture<UnifiedDataTable>> futureMap = new LinkedHashMap<>();
        for (String metricName : uniqueMetricNames) {
            futureMap.put(metricName, CompletableFuture.supplyAsync(() -> metricService.getMetricData(metricName)));
        }

        Map<String, UnifiedDataTable> metricDataMap = new LinkedHashMap<>();
        for (Map.Entry<String, CompletableFuture<UnifiedDataTable>> e : futureMap.entrySet()) {
            metricDataMap.put(e.getKey(), e.getValue().join());
        }

        if (metricDataMap.isEmpty()) {
            throw new IllegalStateException("No data fetched for query");
        }

        // Join tables
        List<UnifiedDataTable> tables = new ArrayList<>(metricDataMap.values());
        UnifiedDataTable joined = arrowJoinUtil.joinOnKeys(tables, plan.getTableDefinition().getPrimaryKeys(), plan.getTableDefinition().getFieldMapping());

        // Apply WHERE clause filtering
        return applyWhereClauseFilter(joined, plan.getWhereEqConditions(), plan.getTableDefinition().getFieldMapping());
    }

    /**
     * Parse SQL and produce MetricQueryPlan.
     */
    public MetricQueryPlan parseSql(String sql) {
        try {
            Statement stmt = CCJSqlParserUtil.parse(sql);
            if (!(stmt instanceof Select)) {
                throw new IllegalArgumentException("Only SELECT is supported");
            }

            PlainSelect ps = (PlainSelect) ((Select) stmt).getSelectBody();
            String tableName = ((Table) ps.getFromItem()).getName().replaceAll("^[\"`]|[\"`]$", "");

            if (tableName.isEmpty()) {
                throw new IllegalArgumentException("Table name cannot be empty");
            }

            if (ps.getWhere() == null) {
                throw new IllegalArgumentException("WHERE clause with all primary keys is required");
            }

            TableDefinition td = tableRegistry.getByName(tableName)
                .orElseThrow(() -> new IllegalArgumentException("Unknown table: " + tableName));

            List<String> selectFields;
            if (((SelectItem) ps.getSelectItems().get(0)).getExpression() instanceof AllColumns) {
                selectFields = new ArrayList<>(td.getFieldMapping().keySet());
            } else {
                selectFields = ps.getSelectItems().stream()
                    .map(item -> ((Column) item.getExpression()).getColumnName())
                    .collect(Collectors.toList());
            }

            Map<String, String> fieldMetricMap = new LinkedHashMap<>();
            for (String f : selectFields) {
                String metricName = td.getMetricFields().get(f);
                if (metricName == null) {
                    throw new IllegalArgumentException("Unknown field '" + f + "' in table '" + tableName + "'");
                }
                fieldMetricMap.put(f, metricName);
            }

            Map<String, Object> pkValues = new HashMap<>();
            extractConditions(ps.getWhere(), pkValues);

            for (String pk : td.getPrimaryKeys()) {
                if (!pkValues.containsKey(pk)) {
                    throw new IllegalArgumentException("Missing primary key in WHERE: " + pk);
                }
            }

            Map<String, String> whereEqConditions = new HashMap<>();
            pkValues.forEach((k, v) -> whereEqConditions.put(k, v.toString()));

            return MetricQueryPlan.builder()
                .tableName(tableName)
                .tableDefinition(td)
                .selectFields(selectFields)
                .fieldMetricMapping(fieldMetricMap)
                .whereEqConditions(whereEqConditions)
                .rawWhere(ps.getWhere().toString())
                .build();
        } catch (Exception e) {
            logger.error("Failed to parse SQL: {}", sql, e);
            throw new RuntimeException("Failed to parse SQL: " + sql, e);
        }
    }

    private void extractConditions(Expression expression, Map<String, Object> pkValues) {
        if (expression instanceof EqualsTo) {
            extractEqualsTo(pkValues, (EqualsTo) expression);
        } else if (expression instanceof AndExpression) {
            AndExpression and = (AndExpression) expression;
            extractConditions(and.getLeftExpression(), pkValues);
            extractConditions(and.getRightExpression(), pkValues);
        }
    }

    private void extractEqualsTo(Map<String, Object> pkValues, EqualsTo equalsTo) {
        String key = ((Column) equalsTo.getLeftExpression()).getColumnName();
        Expression rightExpr = equalsTo.getRightExpression();

        if (rightExpr instanceof StringValue) {
            pkValues.put(key, ((StringValue) rightExpr).getValue());
        } else if (rightExpr instanceof LongValue) {
            pkValues.put(key, ((LongValue) rightExpr).getValue());
        } else {
            throw new IllegalArgumentException("Unsupported value type for " + key);
        }
    }

    /**
     * Apply WHERE clause filtering to the joined table.
     */
    private UnifiedDataTable applyWhereClauseFilter(UnifiedDataTable table, Map<String, String> whereConditions, Map<String, String> fieldMappings) {
        if (whereConditions == null || whereConditions.isEmpty()) {
            return table;
        }

        VectorSchemaRoot root = table.getData();
        List<Integer> matchingRows = new ArrayList<>();

        for (int row = 0; row < root.getRowCount(); row++) {
            boolean matchesAll = true;
            for (Map.Entry<String, String> condition : whereConditions.entrySet()) {
                String logicalFieldName = condition.getKey();
                // Use the logical name to find the physical name from the mapping
                String physicalFieldName = fieldMappings.getOrDefault(logicalFieldName, logicalFieldName);
                String expectedValue = condition.getValue();

                // Use the physical name to get the vector
                FieldVector vector = root.getVector(physicalFieldName);
                if (vector == null) {
                    // If the physical column doesn't exist, this row can't match.
                    matchesAll = false;
                    break;
                }

                Object actualValue = vector.getObject(row);
                String actualValueStr;

                if (actualValue instanceof byte[]) {
                    actualValueStr = new String((byte[]) actualValue, java.nio.charset.StandardCharsets.UTF_8);
                } else {
                    actualValueStr = String.valueOf(actualValue);
                }

                if (!expectedValue.equals(actualValueStr)) {
                    matchesAll = false;
                    break;
                }
            }
            if (matchesAll) {
                matchingRows.add(row);
            }
        }

        if (matchingRows.isEmpty()) {
            return createEmptyTable(table);
        }

        return createFilteredTable(table, matchingRows);
    }

    /**
     * Create an empty table with the same schema as the original.
     */
    private UnifiedDataTable createEmptyTable(UnifiedDataTable original) {
        VectorSchemaRoot originalRoot = original.getData();
        Schema schema = originalRoot.getSchema();
        VectorSchemaRoot emptyRoot = VectorSchemaRoot.create(schema, allocator);
        emptyRoot.allocateNew();
        emptyRoot.setRowCount(0);
        return new UnifiedDataTable(original.getTableName(), emptyRoot, schema);
    }

    /**
     * Create a filtered table containing only the specified rows.
     */
    private UnifiedDataTable createFilteredTable(UnifiedDataTable original, List<Integer> matchingRows) {
        VectorSchemaRoot originalRoot = original.getData();
        Schema schema = originalRoot.getSchema();
        VectorSchemaRoot filteredRoot = VectorSchemaRoot.create(schema, allocator);
        filteredRoot.allocateNew();

        for (int newRow = 0; newRow < matchingRows.size(); newRow++) {
            int originalRow = matchingRows.get(newRow);
            for (org.apache.arrow.vector.FieldVector originalVector : originalRoot.getFieldVectors()) {
                filteredRoot.getVector(originalVector.getField().getName()).copyFromSafe(originalRow, newRow, originalVector);
            }
        }
        filteredRoot.setRowCount(matchingRows.size());
        return new UnifiedDataTable(original.getTableName(), filteredRoot, schema);
    }
}
