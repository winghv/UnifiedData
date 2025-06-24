package com.example.unifieddataservice.service.parser;

import com.example.unifieddataservice.model.DataType;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.Map;
import java.util.stream.Collectors;

public class DataTypeMapper {

    public static Map<String, Field> toArrowFields(Map<String, DataType> fieldMappings) {
        return fieldMappings.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    ArrowType arrowType = toArrowType(entry.getValue());
                    return new Field(entry.getKey(), FieldType.nullable(arrowType), null);
                }
            ));
    }

    private static ArrowType toArrowType(DataType dataType) {
        switch (dataType) {
            case STRING:
                return new ArrowType.Utf8();
            case LONG:
                return new ArrowType.Int(64, true);
            case DOUBLE:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case BOOLEAN:
                return new ArrowType.Bool();
            case TIMESTAMP:
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }
}
