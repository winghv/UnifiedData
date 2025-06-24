package com.example.unifieddataservice.service;

import com.example.unifieddataservice.model.UnifiedDataTable;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntPredicate;

@Service
public class DataFilteringService {
    private static final Logger logger = LoggerFactory.getLogger(DataFilteringService.class);
    
    public UnifiedDataTable filter(UnifiedDataTable dataTable, String filterExpression) {
        if (filterExpression == null || filterExpression.isBlank()) {
            return dataTable;
        }
        
        try {
            // Parse the filter expression
            FilterCondition condition = parseFilterExpression(filterExpression);
            if (condition == null) {
                logger.warn("Invalid filter expression: {}", filterExpression);
                return dataTable;
            }
            
            // Apply the filter
            return applyFilter(dataTable, condition);
            
        } catch (Exception e) {
            logger.error("Error applying filter: " + filterExpression, e);
            throw new IllegalArgumentException("Invalid filter expression: " + filterExpression, e);
        }
    }
    
    private FilterCondition parseFilterExpression(String expression) {
        try {
            String[] parts = expression.trim().split("\\s*([=!<>]=?|<=?|>=?)\\s*");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid filter expression format");
            }
            
            String column = parts[0].trim();
            String operator = extractOperator(expression);
            String value = parts[1].trim().replaceAll("['\"]", "");
            
            return new FilterCondition(column, operator, value);
            
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse filter expression: " + expression, e);
        }
    }
    
    private String extractOperator(String expression) {
        if (expression.contains("==")) return "==";
        if (expression.contains("!=")) return "!=";
        if (expression.contains("<=")) return "<=";
        if (expression.contains(">=")) return ">=";
        if (expression.contains("<")) return "<";
        if (expression.contains(">")) return ">";
        throw new IllegalArgumentException("Unsupported operator in expression: " + expression);
    }
    
    private UnifiedDataTable applyFilter(UnifiedDataTable dataTable, FilterCondition condition) {
        VectorSchemaRoot root = dataTable.getData();
        
        // Get the vector for the column to filter on
        FieldVector vector = null;
        for (FieldVector fieldVector : root.getFieldVectors()) {
            if (fieldVector.getField().getName().equals(condition.column)) {
                vector = fieldVector;
                break;
            }
        }
        
        if (vector == null) {
            throw new IllegalArgumentException("Column not found: " + condition.column);
        }
        
        // Create a predicate based on the condition
        IntPredicate rowPredicate = createRowPredicate(vector, condition);
        
        // Count matching rows and collect matching row indices
        List<Integer> matchingRows = new ArrayList<>();
        for (int i = 0; i < root.getRowCount(); i++) {
            if (rowPredicate.test(i)) {
                matchingRows.add(i);
            }
        }
        
        int matchCount = matchingRows.size();
        
        // Get the schema from the original root
        Schema schema = root.getSchema();
        
        // Create a new root with the same schema
        try (VectorSchemaRoot newRoot = new VectorSchemaRoot(root.getFieldVectors())) {
            // Create new vectors with the same types as the original
            List<FieldVector> newVectors = new ArrayList<>();
            for (Field field : schema.getFields()) {
                FieldVector newVector = field.createVector(root.getFieldVectors().get(0).getAllocator());
                newVector.allocateNew();
                newVectors.add(newVector);
            }
            
            // Transfer data for matching rows
            for (int targetRow = 0; targetRow < matchCount; targetRow++) {
                int srcRow = matchingRows.get(targetRow);
                
                for (int colIdx = 0; colIdx < schema.getFields().size(); colIdx++) {
                    FieldVector srcVector = root.getFieldVectors().get(colIdx);
                    FieldVector destVector = newVectors.get(colIdx);
                    
                    if (!srcVector.isNull(srcRow)) {
                        Object value = srcVector.getObject(srcRow);
                        copyValue(destVector, targetRow, value);
                    } else {
                        destVector.setNull(targetRow);
                    }
                }
            }
            
            // Set the row count for all vectors
            newVectors.forEach(v -> v.setValueCount(matchCount));
            
            // Create a new root with the filtered vectors
            VectorSchemaRoot filteredRoot = new VectorSchemaRoot(schema.getFields(), newVectors, matchCount);
            
            // Create a deep copy of the filtered data
            return new UnifiedDataTable(filteredRoot);
        }
    }
    
    private void copyValue(FieldVector destVector, int row, Object value) {
        if (destVector instanceof VarCharVector) {
            ((VarCharVector) destVector).setSafe(row, value.toString().getBytes());
        } else if (destVector instanceof BigIntVector) {
            ((BigIntVector) destVector).setSafe(row, ((Number) value).longValue());
        } else if (destVector instanceof IntVector) {
            ((IntVector) destVector).setSafe(row, ((Number) value).intValue());
        } else if (destVector instanceof Float8Vector) {
            ((Float8Vector) destVector).setSafe(row, ((Number) value).doubleValue());
        } else if (destVector instanceof Float4Vector) {
            ((Float4Vector) destVector).setSafe(row, ((Number) value).floatValue());
        } else {
            throw new UnsupportedOperationException("Unsupported vector type: " + destVector.getClass().getSimpleName());
        }
    }
    
    private IntPredicate createRowPredicate(FieldVector vector, FilterCondition condition) {
        if (vector instanceof VarCharVector) {
            return createStringPredicate((VarCharVector) vector, condition);
        } else if (vector instanceof BigIntVector || vector instanceof IntVector) {
            return createLongPredicate(vector, condition);
        } else if (vector instanceof Float8Vector || vector instanceof Float4Vector) {
            return createDoublePredicate(vector, condition);
        } else {
            throw new UnsupportedOperationException("Unsupported vector type: " + vector.getClass().getSimpleName());
        }
    }
    
    private IntPredicate createStringPredicate(VarCharVector vector, FilterCondition condition) {
        String targetValue = condition.value;
        
        switch (condition.operator) {
            case "==":
                return i -> !vector.isNull(i) && new String(vector.get(i)).equals(targetValue);
            case "!=":
                return i -> vector.isNull(i) || !new String(vector.get(i)).equals(targetValue);
            default:
                throw new UnsupportedOperationException("Unsupported operator for string comparison: " + condition.operator);
        }
    }
    
    private IntPredicate createLongPredicate(FieldVector vector, FilterCondition condition) {
        long targetValue = Long.parseLong(condition.value);
        
        return i -> {
            if (vector.isNull(i)) return false;
            
            long value = (vector instanceof BigIntVector) 
                ? ((BigIntVector) vector).get(i)
                : ((IntVector) vector).get(i);
                
            return switch (condition.operator) {
                case "==" -> value == targetValue;
                case "!=" -> value != targetValue;
                case "<" -> value < targetValue;
                case "<=" -> value <= targetValue;
                case ">" -> value > targetValue;
                case ">=" -> value >= targetValue;
                default -> throw new UnsupportedOperationException("Unsupported operator: " + condition.operator);
            };
        };
    }
    
    private IntPredicate createDoublePredicate(FieldVector vector, FilterCondition condition) {
        double targetValue = Double.parseDouble(condition.value);
        
        return i -> {
            if (vector.isNull(i)) return false;
            
            double value = (vector instanceof Float8Vector)
                ? ((Float8Vector) vector).get(i)
                : ((Float4Vector) vector).get(i);
                
            return switch (condition.operator) {
                case "==" -> Math.abs(value - targetValue) < 0.000001; // Handle floating point precision
                case "!=" -> Math.abs(value - targetValue) >= 0.000001;
                case "<" -> value < targetValue;
                case "<=" -> value <= targetValue + 0.000001;
                case ">" -> value > targetValue;
                case ">=" -> value >= targetValue - 0.000001;
                default -> throw new UnsupportedOperationException("Unsupported operator: " + condition.operator);
            };
        };
    }
    
    private static class FilterCondition {
        final String column;
        final String operator;
        final String value;
        
        FilterCondition(String column, String operator, String value) {
            this.column = column;
            this.operator = operator;
            this.value = value;
        }
    }
}
