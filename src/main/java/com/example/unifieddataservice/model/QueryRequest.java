package com.example.unifieddataservice.model;

public class QueryRequest {
    private String sql;
    private String format;

    // Getters and setters
    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }
}
