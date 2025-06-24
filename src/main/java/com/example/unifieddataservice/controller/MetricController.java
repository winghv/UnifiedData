package com.example.unifieddataservice.controller;

import com.example.unifieddataservice.model.UnifiedDataTable;
import com.example.unifieddataservice.service.MetricService;
import com.example.unifieddataservice.service.CsvDataExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/metrics")
public class MetricController {
    private static final Logger logger = LoggerFactory.getLogger(MetricController.class);
    
    private final MetricService metricService;

    public MetricController(MetricService metricService) {
        this.metricService = metricService;
        logger.info("MetricController initialized with MetricService: {}", metricService != null ? "present" : "null");
    }

    @GetMapping(value = "/{metricName}", produces = "text/csv")
    public ResponseEntity<String> getMetric(
            @PathVariable String metricName,
            @RequestParam(required = false) String filter) {
        logger.info("Received request for metric: {}, filter: {}", metricName, filter);
        
        try {
            logger.debug("Calling metricService.getFilteredMetricData for: {}", metricName);
            UnifiedDataTable table = metricService.getFilteredMetricData(metricName, filter);
            
            if (table == null) {
                logger.error("Received null table from metricService for metric: {}", metricName);
                return new ResponseEntity<>("No data available for metric: " + metricName, 
                                          HttpStatus.NOT_FOUND);
            }
            
            logger.debug("Converting table to CSV. Row count: {}", table.getRowCount());
            String csvData = CsvDataExporter.exportToCsv(table.getData());
            
            if (csvData == null || csvData.isEmpty()) {
                logger.warn("Empty CSV data generated for metric: {}", metricName);
                return new ResponseEntity<>("No data available for metric: " + metricName, 
                                          HttpStatus.NO_CONTENT);
            }

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.TEXT_PLAIN);
            headers.setContentDispositionFormData("attachment", metricName + ".csv");
            headers.setContentLength(csvData.getBytes().length);
            
            logger.info("Successfully processed request for metric: {}. CSV size: {} bytes", 
                      metricName, csvData.getBytes().length);
                      
            return new ResponseEntity<>(csvData, headers, HttpStatus.OK);
            
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid request for metric: {}. Error: {}", metricName, e.getMessage());
            return new ResponseEntity<>("Error: " + e.getMessage(), HttpStatus.NOT_FOUND);
            
        } catch (Exception e) {
            logger.error("Error processing request for metric: {}", metricName, e);
            return new ResponseEntity<>(
                "An internal error occurred while processing your request. Please try again later.", 
                HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
