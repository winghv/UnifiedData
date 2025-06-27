package com.example.unifieddataservice.controller;

import org.springframework.core.io.ClassPathResource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/stock-data")
@CrossOrigin(origins = "*")
public class StockDataController {

    @GetMapping
    public ResponseEntity<?> getStockData(@RequestParam(defaultValue = "json") String source) {
        try {
            String fileName = "sample-data/stock_data." + ("csv".equalsIgnoreCase(source) ? "csv" : "json");
            ClassPathResource resource = new ClassPathResource(fileName);
            
            if (!resource.exists()) {
                return ResponseEntity.notFound().build();
            }

            String content = new String(Files.readAllBytes(Paths.get(resource.getURI())), StandardCharsets.UTF_8);
            
            if ("csv".equalsIgnoreCase(source)) {
                return ResponseEntity.ok(convertCsvToJson(content));
            } else {
                // Parse JSON to ensure it's valid
                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, Object> jsonData = objectMapper.readValue(content, new TypeReference<Map<String, Object>>() {});
                return ResponseEntity.ok(jsonData);
            }
        } catch (IOException e) {
            e.printStackTrace();
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status_code", 500);
            errorResponse.put("error", "Failed to load stock data");
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }

    private Map<String, Object> convertCsvToJson(String csvContent) {
        // Simple CSV to JSON conversion
        String[] lines = csvContent.split("\n");
        if (lines.length < 2) {
            Map<String, Object> error = new HashMap<>();
            error.put("status_code", 400);
            error.put("error", "Invalid CSV format");
            return error;
        }

        String[] headers = lines[0].split(",");
        Map<String, Object> response = new HashMap<>();
        response.put("status_code", 0);
        
        Object[] data = new Object[lines.length - 1];
        
        for (int i = 1; i < lines.length; i++) {
            String[] values = lines[i].split(",");
            Map<String, String> row = new HashMap<>();
            
            for (int j = 0; j < headers.length && j < values.length; j++) {
                row.put(headers[j], values[j]);
            }
            
            data[i - 1] = row;
        }
        
        response.put("datas", data);
        return response;
    }
}
