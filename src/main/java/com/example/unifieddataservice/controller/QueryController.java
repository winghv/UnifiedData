package com.example.unifieddataservice.controller;

import com.example.unifieddataservice.model.UnifiedDataTable;
import com.example.unifieddataservice.service.SqlQueryService;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Exposes SQL query endpoint. Returns Arrow IPC binary stream for efficiency.
 * Example: /query?sql=SELECT+close_price+FROM+daily_bar+WHERE+stock_code='600000'&format=json
 */
@RestController
@RequestMapping("/api")
public class QueryController {
    private static final Logger logger = LoggerFactory.getLogger(QueryController.class);

    private final SqlQueryService sqlQueryService;
    private final Executor asyncExecutor;

    public QueryController(SqlQueryService sqlQueryService) {
        this.sqlQueryService = sqlQueryService;
        this.asyncExecutor = Executors.newFixedThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors() * 2)
        );
    }

    @PostMapping("/query")
    public CompletableFuture<ResponseEntity<?>> query(@RequestBody String sql) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                UnifiedDataTable dataTable = sqlQueryService.query(sql);
                return inMemoryArrowResponse(dataTable);
            } catch (Exception e) {
                logger.error("Query failed: {}", sql, e);
                return ResponseEntity.internalServerError().body("Query failed: " + e.getMessage());
            }
        }, asyncExecutor);
    }

    private ResponseEntity<byte[]> inMemoryArrowResponse(UnifiedDataTable table) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (VectorSchemaRoot vectorSchemaRoot = table.getData();
             ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, out)) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM_VALUE)
                .body(out.toByteArray());
    }
}
