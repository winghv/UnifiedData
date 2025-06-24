package com.example.unifieddataservice.model;

import org.apache.arrow.vector.VectorSchemaRoot;
import java.lang.AutoCloseable;

public class UnifiedDataTable implements AutoCloseable {

    private final VectorSchemaRoot data;

    public UnifiedDataTable(VectorSchemaRoot data) {
        this.data = data;
    }

    public VectorSchemaRoot getData() {
        return data;
    }

    public int getRowCount() {
        return data.getRowCount();
    }

    @Override
    public void close() {
        data.close();
    }
}
