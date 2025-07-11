package com.example.unifieddataservice.service;

import com.example.unifieddataservice.model.MetricQueryPlan;
import com.example.unifieddataservice.model.TableDefinition;

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

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Parses simple SELECT statements and orchestrates metric fetch & join.
 * Supports: SELECT field1, field2 FROM table WHERE pk1 = 'x' AND pk2 = 20250101
 */
@Service
public class SqlQueryService {

    private static final Logger logger = LoggerFactory.getLogger(SqlQueryService.class);
    private final TableRegistry tableRegistry;
    private final MetricService metricService;
    private final ArrowJoinUtil arrowJoinUtil;

    public SqlQueryService(TableRegistry tableRegistry, MetricService metricService, ArrowJoinUtil arrowJoinUtil) {
        this.tableRegistry = tableRegistry;
        this.metricService = metricService;
        this.arrowJoinUtil = arrowJoinUtil;
    }

    @org.springframework.cache.annotation.Cacheable(value = "queryResults", key = "#sql", sync = true)
    public UnifiedDataTable query(String sql) {
        MetricQueryPlan plan = parseSql(sql);
        logger.info("Executing MetricQueryPlan: {}", plan);

        // Fetch metrics in parallel for better latency
        Map<String, CompletableFuture<UnifiedDataTable>> futureMap = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : plan.getFieldMetricMapping().entrySet()) {
            String field = entry.getKey();
            String metricName = entry.getValue();
            futureMap.put(field, CompletableFuture.supplyAsync(() -> metricService.getMetricData(metricName)));
        }

        Map<String, UnifiedDataTable> fieldDataMap = new LinkedHashMap<>();
        for (Map.Entry<String, CompletableFuture<UnifiedDataTable>> e : futureMap.entrySet()) {
            fieldDataMap.put(e.getKey(), e.getValue().join());
        }

        if (fieldDataMap.isEmpty()) {
            throw new IllegalStateException("No data fetched for query");
        }

        // Join ArrowTables on primary keys from TableDefinition for high-performance merge
        List<UnifiedDataTable> tables = new ArrayList<>(fieldDataMap.values());
        UnifiedDataTable joined = arrowJoinUtil.joinOnKeys(
                tables,
                plan.getTableDefinition().getPrimaryKeys()
        );
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
            Select select = (Select) stmt;
            PlainSelect ps = (PlainSelect) select.getSelectBody();
            String tableName = ((Table) ps.getFromItem()).getName();

            if (ps.getWhere() == null) {
                throw new IllegalArgumentException("WHERE clause with all primary keys is required for querying logical tables.");
            }

            TableDefinition td = tableRegistry.getByName(tableName)
                    .orElseThrow(() -> new IllegalArgumentException("Unknown table: " + tableName));

            List<String> selectFields;
            Object firstItem = ps.getSelectItems().get(0);

            if (firstItem instanceof AllColumns) {
                selectFields = new ArrayList<>(td.getFieldMapping().keySet());
            } else {
                selectFields = ps.getSelectItems().stream()
                    .map(item -> {
                        if (item.getAlias() != null) {
                            return item.getAlias().getName();
                        }
                        Expression expression = item.getExpression();
                        if (expression instanceof Column) {
                            return ((Column) expression).getColumnName();
                        }
                        // Fallback for other expression types, though our use case is simple columns
                        return expression.toString();
                    })
                    .collect(Collectors.toList());
            }

            // Map fields -> metrics
            Map<String, String> fieldMetricMap = new LinkedHashMap<>();
            for (String f : selectFields) {
                String metricName = td.getMetricFields().get(f);
                if (metricName == null) {
                    throw new IllegalArgumentException("Unknown field '" + f + "' in table '" + tableName + "'");
                }
                fieldMetricMap.put(f, metricName);
            }

            // Extract primary key values from WHERE clause
            Map<String, Object> pkValues = new HashMap<>();
            extractConditions(ps.getWhere(), pkValues);

            // Validate all pks are present
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
            e.printStackTrace(); // Add full stack trace to console log
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
        logger.debug("Extracting EqualsTo for key '{}', right expression type: {}", key, rightExpr.getClass().getName());
        if (rightExpr instanceof StringValue) {
            pkValues.put(key, ((StringValue) rightExpr).getValue());
        } else if (rightExpr instanceof LongValue) {
            pkValues.put(key, ((LongValue) rightExpr).getValue());
        }
    }
}
