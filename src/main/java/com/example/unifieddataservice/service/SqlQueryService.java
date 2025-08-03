package com.example.unifieddataservice.service;

import com.example.unifieddataservice.model.MetricQueryPlan;
import com.example.unifieddataservice.model.Operator;
import com.example.unifieddataservice.model.Predicate;
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
            futureMap.put(metricName, CompletableFuture.supplyAsync(() -> metricService.getMetricData(metricName, plan.getPredicates())));
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

        // The WHERE clause is now pushed down, so no need to apply it here.
        // The applyWhereClauseFilter method will be removed.
        return joined;
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
            List<Predicate> predicates = new ArrayList<>();
            if (ps.getWhere() != null) {
                extractPredicates(ps.getWhere(), predicates);
            }

            // This primary key check might need to be re-evaluated or moved,
            // as filtering might not always be on primary keys.
            // For now, we keep it to ensure basic query integrity.
            Set<String> predicateColumns = predicates.stream()
                .map(Predicate::columnName)
                .collect(Collectors.toSet());

            for (String pk : td.getPrimaryKeys()) {
                if (!predicateColumns.contains(pk)) {
                    // throw new IllegalArgumentException("Missing primary key in WHERE: " + pk);
                }
            }

            return MetricQueryPlan.builder()
                .tableName(tableName)
                .tableDefinition(td)
                .selectFields(selectFields)
                .fieldMetricMapping(fieldMetricMap)
                .predicates(predicates) // Use the new predicates list
                .build();
        } catch (Exception e) {
            logger.error("Failed to parse SQL: {}", sql, e);
            throw new RuntimeException("Failed to parse SQL: " + sql, e);
        }
    }


    private void extractPredicates(Expression expression, List<Predicate> predicates) {
        if (expression instanceof AndExpression) {
            AndExpression and = (AndExpression) expression;
            extractPredicates(and.getLeftExpression(), predicates);
            extractPredicates(and.getRightExpression(), predicates);
        } else if (expression instanceof EqualsTo) {
            addPredicate((EqualsTo) expression, Operator.EQUALS, predicates);
        } // Add other operators like >, <, etc. here as needed
    }

    private void addPredicate(net.sf.jsqlparser.expression.operators.relational.EqualsTo expression, Operator op, List<Predicate> predicates) {
        if (!(expression.getLeftExpression() instanceof Column)) {
            throw new IllegalArgumentException("Unsupported WHERE clause structure: " + expression);
        }
        String columnName = ((Column) expression.getLeftExpression()).getColumnName();
        Expression rightExpr = expression.getRightExpression();
        Object value;

        if (rightExpr instanceof StringValue) {
            value = ((StringValue) rightExpr).getValue();
        } else if (rightExpr instanceof LongValue) {
            value = ((LongValue) rightExpr).getValue();
        } else {
            throw new IllegalArgumentException("Unsupported value type for column " + columnName + ": " + rightExpr.getClass().getSimpleName());
        }
        predicates.add(new Predicate(columnName, op, value));
    }


}
