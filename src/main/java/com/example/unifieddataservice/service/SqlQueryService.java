package com.example.unifieddataservice.service;

import com.example.unifieddataservice.model.MetricQueryPlan;
import com.example.unifieddataservice.model.TableDefinition;
import com.example.unifieddataservice.model.UnifiedDataTable;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.CompletableFuture;
import com.example.unifieddataservice.util.ArrowJoinUtil;

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
            if (!(stmt instanceof Select select)) {
                throw new IllegalArgumentException("Only SELECT is supported");
            }
            PlainSelect ps = (PlainSelect) select.getSelectBody();
            String tableName = ps.getFromItem().toString();

            TableDefinition td = tableRegistry.getByName(tableName)
                    .orElseThrow(() -> new IllegalArgumentException("Unknown table: " + tableName));

            List<String> selectFields = ps.getSelectItems().stream()
                    .map(SelectItem::toString)
                    .collect(Collectors.toList());
            if (selectFields.size() == 1 && selectFields.get(0).equals("*")) {
                selectFields = new ArrayList<>(td.getMetricFields().keySet());
            }

            // Map fields -> metrics
            Map<String, String> fieldMetricMap = new LinkedHashMap<>();
            for (String f : selectFields) {
                String metricName = td.getMetricFields().get(f);
                if (metricName == null) {
                    throw new IllegalArgumentException("Field " + f + " not defined in table " + tableName);
                }
                fieldMetricMap.put(f, metricName);
            }

            // Handle simple equality conditions in WHERE
            Map<String, String> eqConditions = new HashMap<>();
            Expression where = ps.getWhere();
            if (where != null) {
                extractEqConditions(where, eqConditions);
            }

            return MetricQueryPlan.builder()
                    .tableName(tableName)
                    .tableDefinition(td)
                    .selectFields(selectFields)
                    .fieldMetricMapping(fieldMetricMap)
                    .whereEqConditions(eqConditions)
                    .rawWhere(where == null ? null : where.toString())
                    .build();
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse SQL: " + sql, e);
        }
    }

    private void extractEqConditions(Expression expr, Map<String, String> out) {
        if (expr instanceof EqualsTo eq) {
            Column col = (Column) eq.getLeftExpression();
            String value;
            if (eq.getRightExpression() instanceof StringValue sv) {
                value = sv.getValue();
            } else if (eq.getRightExpression() instanceof LongValue lv) {
                value = lv.getStringValue();
            } else {
                value = eq.getRightExpression().toString();
            }
            out.put(col.getColumnName(), value);
        } else if (expr.getClass().getSimpleName().contains("AndExpression")) {
            // recurse on left/right via reflection-friendly API not exposed; use string parse split for MVP
            String[] parts = expr.toString().split("(?i) AND ");
            for (String part : parts) {
                try {
                    extractEqConditions(CCJSqlParserUtil.parseCondExpression(part), out);
                } catch (Exception ignored) {
                }
            }
        }
    }
}
