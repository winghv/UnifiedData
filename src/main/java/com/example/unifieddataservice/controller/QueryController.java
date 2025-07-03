package com.example.unifieddataservice.controller;

import com.example.unifieddataservice.model.UnifiedDataTable;
import com.example.unifieddataservice.service.SqlQueryService;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Exposes SQL query endpoint. Returns Arrow IPC binary stream for efficiency.
 * Example: /query?sql=SELECT+close_price+FROM+daily_bar+WHERE+stock_code='600000'&format=json
 */
@RestController
public class QueryController {
    private static final Logger logger = LoggerFactory.getLogger(QueryController.class);
    private static final int STREAMING_BATCH_SIZE = 8192; // Rows per batch for streaming
    private final SqlQueryService sqlQueryService;
    private final Executor asyncExecutor;

    public QueryController(SqlQueryService sqlQueryService) {
        this.sqlQueryService = sqlQueryService;
        this.asyncExecutor = Executors.newFixedThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors() * 2)
        );
    }

    @GetMapping("/query")
    public CompletableFuture<ResponseEntity<?>> query(
            @RequestParam String sql,
            @RequestParam(defaultValue = "arrow") String format) {
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Validate format
                if (!format.equalsIgnoreCase("arrow") && !format.equalsIgnoreCase("json")) {
                    return ResponseEntity.badRequest()
                            .body("Unsupported format: " + format + ". Supported formats: arrow, json");
                }
                
                UnifiedDataTable table = sqlQueryService.query(sql);
                if (table.getRowCount() <= STREAMING_BATCH_SIZE) {
                    // Small result - return in memory
                    return handleSmallResult(table, format);
                } else {
                    // Large result - stream directly
                    return handleLargeResult(table, format);
                }
            } catch (Exception e) {
                logger.error("Query failed: {}", sql, e);
                return ResponseEntity.internalServerError()
                        .body("Query failed: " + e.getMessage());
            }
        }, asyncExecutor);
    }

    private ResponseEntity<?> handleSmallResult(UnifiedDataTable table, String format) throws IOException {
        if ("json".equalsIgnoreCase(format)) {
            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(table.getData().contentToTSVString());
        }
        
        // Small Arrow IPC in memory
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
             ArrowStreamWriter writer = new ArrowStreamWriter(table.getData(), null, out)) {
            writer.start();
            writer.writeBatch();
            writer.end();
            byte[] bytes = out.toByteArray();
            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=results.arrow")
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .contentLength(bytes.length)
                    .body(bytes);
        }
    }

    private ResponseEntity<?> handleLargeResult(UnifiedDataTable table, String format) {
        if ("json".equalsIgnoreCase(format)) {
            // For JSON streaming, we'd need a different approach
            return ResponseEntity.badRequest()
                    .body("Streaming JSON not implemented. Please use format=arrow for large result sets.");
        }

        // For Arrow format, return a streaming response
        StreamingResponseBody stream = output -> {
            try (RootAllocator allocator = new RootAllocator();
                 VectorSchemaRoot root = table.getData()) {
                
                // Create a writer that writes directly to the output stream
                try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, output)) {
                    writer.start();
                    
                    // Write in batches to control memory usage
                    int totalRows = root.getRowCount();
                    for (int i = 0; i < totalRows; i += STREAMING_BATCH_SIZE) {
                        int end = Math.min(i + STREAMING_BATCH_SIZE, totalRows);
                        root.setRowCount(end - i);
                        writer.writeBatch();
                        output.flush();
                    }
                    
                    writer.end();
                }
            } catch (Exception e) {
                logger.error("Streaming failed for table with {} rows", table.getRowCount(), e);
                throw new IOException("Failed to stream results: " + e.getMessage(), e);
            }
        };

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=results.arrow")
                .contentType(MediaType.parseMediaType("application/vnd.apache.arrow.stream"))
                .body(stream);
    }
}
