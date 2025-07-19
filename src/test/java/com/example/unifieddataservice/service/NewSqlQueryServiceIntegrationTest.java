package com.example.unifieddataservice.service;

import com.example.unifieddataservice.model.UnifiedDataTable;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class NewSqlQueryServiceIntegrationTest {

    @Autowired
    private SqlQueryService sqlQueryService;

    @Test
    @org.springframework.cache.annotation.CacheEvict(value = "metrics", allEntries = true)
    void testStockQuoteQuery() {
        String sql = "SELECT ticker, date, volume, price FROM stock_quote WHERE ticker = 'AAPL' AND date = 1672531200000";
        UnifiedDataTable result = sqlQueryService.query(sql);

        assertNotNull(result);
        VectorSchemaRoot root = result.getData();
        assertNotNull(root);

        // Add assertions to check the data
        assertEquals(1, root.getRowCount());
        // Further assertions to check the values in the table
    }
}
