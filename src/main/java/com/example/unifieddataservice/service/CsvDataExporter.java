package com.example.unifieddataservice.service;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;

import java.io.StringWriter;
import java.util.stream.Collectors;

public class CsvDataExporter {

    public static String exportToCsv(VectorSchemaRoot vectorSchemaRoot) {
        StringWriter stringWriter = new StringWriter();

        // Write header
        String header = vectorSchemaRoot.getSchema().getFields().stream()
                .map(field -> field.getName())
                .collect(Collectors.joining(","));
        stringWriter.write(header + "\n");

        // Write rows
        int rowCount = vectorSchemaRoot.getRowCount();
        for (int i = 0; i < rowCount; i++) {
            final int rowIndex = i;
            String row = vectorSchemaRoot.getFieldVectors().stream()
                    .map(vector -> getVectorValueAsString(vector, rowIndex))
                    .collect(Collectors.joining(","));
            stringWriter.write(row + "\n");
        }

        return stringWriter.toString();
    }

    private static String getVectorValueAsString(FieldVector vector, int index) {
        Object value = vector.getObject(index);
        if (value == null) {
            return "";
        }
        if (value instanceof Text) {
            return value.toString();
        }
        if (value instanceof byte[]) {
            return new String((byte[]) value);
        }
        return String.valueOf(value);
    }
}
