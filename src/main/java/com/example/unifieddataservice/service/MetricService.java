package com.example.unifieddataservice.service;

import com.example.unifieddataservice.model.DataSourceType;
import com.example.unifieddataservice.model.MetricInfo;
import com.example.unifieddataservice.model.UnifiedDataTable;
import com.example.unifieddataservice.repository.MetricInfoRepository;
import com.example.unifieddataservice.service.parser.CsvDataParser;
import com.example.unifieddataservice.service.parser.DataParser;
import com.example.unifieddataservice.service.parser.JsonDataParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Service
public class MetricService {
    private static final Logger logger = LoggerFactory.getLogger(MetricService.class);

    private final MetricInfoRepository metricInfoRepository;
    private final DataFetcherService dataFetcherService;
    private final DataFilteringService dataFilteringService;
    private final JsonDataParser jsonDataParser;
    private final CsvDataParser csvDataParser;
    
    // Self-reference for handling self-invocation caching
    @Lazy
    @Autowired
    private MetricService self;

    public MetricService(MetricInfoRepository metricInfoRepository, 
                        DataFetcherService dataFetcherService, 
                        DataFilteringService dataFilteringService,
                        JsonDataParser jsonDataParser,
                        CsvDataParser csvDataParser) {
        this.metricInfoRepository = metricInfoRepository;
        this.dataFetcherService = dataFetcherService;
        this.dataFilteringService = dataFilteringService;
        this.jsonDataParser = jsonDataParser;
        this.csvDataParser = csvDataParser;
    }

    // CRUD operations for MetricInfo

    @CacheEvict(value = "metrics", allEntries = true)
    public MetricInfo saveMetric(MetricInfo metricInfo) {
        logger.info("Saving new metric: {}", metricInfo.getName());
        return metricInfoRepository.save(metricInfo);
    }

    @Cacheable(value = "metrics", key = "'all'")
    public List<MetricInfo> getAllMetrics() {
        logger.info("Fetching all metrics");
        return metricInfoRepository.findAll();
    }

    @Cacheable(value = "metrics", key = "#id")
    public Optional<MetricInfo> getMetricById(Long id) {
        logger.info("Fetching metric by id: {}", id);
        return metricInfoRepository.findById(id);
    }

    @CacheEvict(value = "metrics", allEntries = true)
    public MetricInfo updateMetric(Long id, MetricInfo metricDetails) {
        logger.info("Updating metric id: {}", id);
        MetricInfo metricInfo = metricInfoRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Metric not found with id: " + id));

        metricInfo.setName(metricDetails.getName());
        metricInfo.setDataSourceType(metricDetails.getDataSourceType());
        metricInfo.setSourceUrl(metricDetails.getSourceUrl());
        metricInfo.setDataPath(metricDetails.getDataPath());
        metricInfo.setFieldMappings(metricDetails.getFieldMappings());

        return metricInfoRepository.save(metricInfo);
    }

    @CacheEvict(value = "metrics", allEntries = true)
    public void deleteMetric(Long id) {
        logger.info("Deleting metric id: {}", id);
        if (!metricInfoRepository.existsById(id)) {
            throw new IllegalArgumentException("Metric not found with id: " + id);
        }
        metricInfoRepository.deleteById(id);
    }

    @Cacheable(value = "metrics", key = "#metricName", sync = true)
    public UnifiedDataTable getMetricData(String metricName) {
        logger.info("Getting metric data for: {}", metricName);
        return loadMetricData(metricName, null);
    }

    /**
     * Parameterized version supporting options for metrics that accept arguments, e.g. adjust=post.
     * Options map can be null or empty if no parameters are required.
     */
    @Cacheable(value = "metrics", key = "#metricName + ':' + (#options == null ? '' : #options.hashCode())", sync = true)
    public UnifiedDataTable getMetricData(String metricName, Map<String, String> options) {
        logger.info("Getting metric data for: {}, options: {}", metricName, options);
        return loadMetricData(metricName, options);
    }
    
    /**
     * Loads metric data without checking the cache.
     * This method is package-private for testing purposes.
     */
    UnifiedDataTable loadMetricData(String metricName, Map<String, String> options) {
        logger.info("Loading metric data for: {}", metricName);
        
        try {
            // Find the metric configuration
            Optional<MetricInfo> metricInfoOpt = metricInfoRepository.findByName(metricName);
            if (metricInfoOpt.isEmpty()) {
                String errorMsg = String.format("Metric not found: %s", metricName);
                logger.error(errorMsg);
                throw new IllegalArgumentException(errorMsg);
            }

            MetricInfo metricInfo = metricInfoOpt.get();
            logger.debug("Found metric info: {}", metricInfo);
            // TODO: If metric supports parameters, validate & apply them here using 'options'.
            
            // Fetch the data
            logger.info("Fetching data from: {}", metricInfo.getSourceUrl());
            try (InputStream dataStream = dataFetcherService.fetchData(metricInfo.getSourceUrl())) {
                if (dataStream == null) {
                    String errorMsg = String.format("Failed to fetch data from URL: %s", metricInfo.getSourceUrl());
                    logger.error(errorMsg);
                    throw new IllegalStateException(errorMsg);
                }

                // Get the appropriate parser
                DataParser parser = getParser(metricInfo.getDataSourceType());
                logger.debug("Using parser: {}", parser.getClass().getSimpleName());
                
                // Parse the data with column aliases
                UnifiedDataTable result = parser.parse(
                    dataStream, 
                    metricInfo.getFieldMappings(), 
                    metricInfo.getDataPath(),
                    metricInfo.getColumnAlias()
                );
                
                logger.debug("Parsed data with {} rows and column aliases: {}", 
                    result.getRowCount(), metricInfo.getColumnAlias());
                
                if (result == null) {
                    String errorMsg = String.format("Parser returned null for metric: %s", metricName);
                    logger.error(errorMsg);
                    throw new IllegalStateException(errorMsg);
                }
                
                logger.info("Successfully parsed data. Rows: {}", result.getRowCount());
                return result;
            } catch (IOException e) {
                String errorMsg = String.format("Error processing data stream for metric: %s", metricName);
                logger.error(errorMsg, e);
                throw new IllegalStateException(errorMsg, e);
            }
            
        } catch (Exception e) {
            logger.error("Error in loadMetricData for metric: " + metricName, e);
            throw e;
        }
    }

    @CacheEvict(value = "metrics", key = "#metricName")
    public void evictMetricData(String metricName) {
        logger.info("Evicting metric data from cache: {}", metricName);
        // The actual eviction is handled by the @CacheEvict annotation
    }
    
    @Scheduled(fixedRate = 1, timeUnit = TimeUnit.HOURS)
    public void evictAllCaches() {
        logger.info("Scheduled eviction of all caches");
        // This will trigger the removal listener in CacheConfig for each evicted entry
    }
    
    public UnifiedDataTable getFilteredMetricData(String metricName, String filter) {
        logger.info("Getting filtered data for metric: {}, filter: {}", metricName, filter);
        
        // Get the data from cache or load it
        // Use self-invocation proxy to ensure caching works
        UnifiedDataTable dataTable = self.getMetricData(metricName);
        
        if (dataTable == null) {
            String errorMsg = String.format("No data available for metric: %s", metricName);
            logger.error(errorMsg);
            throw new IllegalStateException(errorMsg);
        }
        
        // Apply filter if provided
        if (filter != null && !filter.isEmpty()) {
            logger.debug("Applying filter: {}", filter);
            try {
                UnifiedDataTable filteredTable = dataFilteringService.filter(dataTable, filter);
                if (filteredTable == null) {
                    logger.warn("Filter returned null for metric: {}", metricName);
                    return dataTable; // Return original if filter fails
                }
                
                logger.info("Filter applied. Rows before: {}, after: {}", 
                           dataTable.getRowCount(), filteredTable.getRowCount());
                return filteredTable;
            } catch (Exception e) {
                logger.warn("Error applying filter, returning unfiltered data", e);
                return dataTable;
            }
        }
        
        logger.debug("No filter applied, returning full dataset");
        return dataTable;
    }

    private DataParser getParser(DataSourceType type) {
        logger.debug("Getting parser for data source type: {}", type);
        
        if (type == null) {
            String errorMsg = "Data source type cannot be null";
            logger.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }
        
        switch (type) {
            case HTTP_JSON:
            case FILE_JSON:
                return jsonDataParser;
            case HTTP_CSV:
            case FILE_CSV:
                return csvDataParser;
            default:
                String errorMsg = "Unsupported data source type: " + type;
                logger.error(errorMsg);
                throw new UnsupportedOperationException(errorMsg);
        }
    }
}
