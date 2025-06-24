package com.example.unifieddataservice.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Optional;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import com.example.unifieddataservice.model.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.context.annotation.Import;

import com.example.unifieddataservice.model.DataSourceType;
import com.example.unifieddataservice.model.MetricInfo;
import com.example.unifieddataservice.model.UnifiedDataTable;
import com.example.unifieddataservice.repository.MetricInfoRepository;
import com.example.unifieddataservice.service.parser.CsvDataParser;
import com.example.unifieddataservice.service.parser.JsonDataParser;

@SpringBootTest(classes = {MetricService.class})
@ExtendWith(SpringExtension.class)
@EnableCaching
@Import({MetricServiceTest.TestConfig.class})
class MetricServiceTest {

    @Configuration
    static class TestConfig {
        @Bean
        public CacheManager cacheManager() {
            return new ConcurrentMapCacheManager("metrics");
        }
        
        @Bean
        public MetricInfoRepository metricInfoRepository() {
            return mock(MetricInfoRepository.class);
        }
        
        @Bean
        public DataFetcherService dataFetcherService() {
            return mock(DataFetcherService.class);
        }
        
        @Bean
        public DataFilteringService dataFilteringService() {
            return mock(DataFilteringService.class);
        }
        
        @Bean
        public JsonDataParser jsonDataParser() {
            return mock(JsonDataParser.class);
        }
        
        @Bean
        public CsvDataParser csvDataParser() {
            return mock(CsvDataParser.class);
        }
    }

    @Autowired
    private MetricInfoRepository metricInfoRepository;
    
    @Autowired
    private DataFetcherService dataFetcherService;
    
    @Autowired
    private DataFilteringService dataFilteringService;
    
    @Autowired
    private JsonDataParser jsonDataParser;
    
    @Autowired
    private CsvDataParser csvDataParser;
    
    @Autowired
    private CacheManager cacheManager;
    
    @Autowired
    private MetricService metricService;

    private UnifiedDataTable createTestDataTable() {
        Schema schema = new Schema(java.util.Collections.emptyList());
        return new UnifiedDataTable(VectorSchemaRoot.create(schema, new RootAllocator()));
    }

    private MetricInfo createTestMetricInfo() {
        MetricInfo metricInfo = new MetricInfo();
        metricInfo.setName("test-metric");
        metricInfo.setDataSourceType(DataSourceType.HTTP_JSON);
        metricInfo.setSourceUrl("http://example.com/data.json");
        metricInfo.setDataPath("");
        
        java.util.Map<String, DataType> fieldMappings = new java.util.HashMap<>();
        fieldMappings.put("id", DataType.STRING);
        fieldMappings.put("value", DataType.LONG);
        metricInfo.setFieldMappings(fieldMappings);
        
        return metricInfo;
    }

    @BeforeEach
    void setUp() {
        // Clear cache before each test
        cacheManager.getCache("metrics").clear();
        
        // Reset mocks
        Mockito.reset(metricInfoRepository, dataFetcherService, dataFilteringService, 
                     jsonDataParser, csvDataParser);
    }

    @Test
    void testGetFilteredMetricData_Success() throws Exception {
        // Arrange
        String metricName = "test-metric";
        String filter = "field1 = 'value1'";
        MetricInfo metricInfo = createTestMetricInfo();
        UnifiedDataTable expectedTable = createTestDataTable();
        UnifiedDataTable filteredTable = createTestDataTable();
        
        when(metricInfoRepository.findByName(eq(metricName))).thenReturn(Optional.of(metricInfo));
        when(dataFetcherService.fetchData(anyString()))
            .thenReturn(new ByteArrayInputStream("[{\"id\":1}]".getBytes()));
        when(jsonDataParser.parse(any(InputStream.class), anyMap(), anyString()))
            .thenReturn(expectedTable);
        when(dataFilteringService.filter(eq(expectedTable), eq(filter))).thenReturn(filteredTable);
        
        // Act
        UnifiedDataTable result = metricService.getFilteredMetricData(metricName, filter);
        
        // Assert
        assertNotNull(result);
        assertEquals(filteredTable, result);
        
        // Verify interactions
        verify(metricInfoRepository).findByName(eq(metricName));
        verify(dataFetcherService).fetchData(eq(metricInfo.getSourceUrl()));
        verify(jsonDataParser).parse(any(InputStream.class), anyMap(), eq(metricInfo.getDataPath()));
        verify(dataFilteringService).filter(eq(expectedTable), eq(filter));
    }

    @Test
    void testGetFilteredMetricData_WithFilter() throws Exception {
        // Arrange
        String metricName = "test-metric";
        String filter = "field2 > 30";
        MetricInfo metricInfo = createTestMetricInfo();
        UnifiedDataTable expectedTable = createTestDataTable();
        UnifiedDataTable filteredTable = createTestDataTable();
        
        // Set up the test data and mocks
        when(metricInfoRepository.findByName(eq(metricName))).thenReturn(Optional.of(metricInfo));
        when(dataFetcherService.fetchData(anyString()))
            .thenReturn(new ByteArrayInputStream("[{\"id\":1}]".getBytes()));
        when(jsonDataParser.parse(any(InputStream.class), anyMap(), anyString()))
            .thenReturn(expectedTable);
        when(dataFilteringService.filter(eq(expectedTable), eq(filter)))
            .thenReturn(filteredTable);
            
        // Act - First call with filter (should fetch, parse, and filter)
        UnifiedDataTable result1 = metricService.getFilteredMetricData(metricName, filter);
        
        // Assert first call
        assertNotNull(result1);
        assertSame(filteredTable, result1);
        
        // Verify first call interactions
        verify(metricInfoRepository).findByName(eq(metricName));
        verify(dataFetcherService).fetchData(eq(metricInfo.getSourceUrl()));
        verify(jsonDataParser).parse(any(InputStream.class), anyMap(), eq(metricInfo.getDataPath()));
        verify(dataFilteringService).filter(eq(expectedTable), eq(filter));
        
        // Reset mocks to verify second call uses cache but still applies filter
        Mockito.reset(metricInfoRepository, dataFetcherService, jsonDataParser, dataFilteringService);
        
        // Set up mocks again for the second call
        // The data loading should be cached, but filtering should still happen
        when(dataFilteringService.filter(eq(expectedTable), eq(filter))).thenReturn(filteredTable);
        
        // Act - Second call with same filter (should use cached data but re-apply filter)
        UnifiedDataTable result2 = metricService.getFilteredMetricData(metricName, filter);

        // Assert second call
        assertNotNull(result2);
        assertSame(filteredTable, result2);
        
        // Verify second call interactions - should use cached data but re-apply filter
        verify(metricInfoRepository, never()).findByName(any());
        verify(dataFetcherService, never()).fetchData(anyString());
        verify(jsonDataParser, never()).parse(any(InputStream.class), anyMap(), anyString());
        verify(dataFilteringService).filter(eq(expectedTable), eq(filter));
    }

    @Test
    void testGetFilteredMetricData_MetricNotFound() {
        // Arrange
        String unknownMetric = "unknown-metric";
        when(metricInfoRepository.findByName(eq(unknownMetric))).thenReturn(Optional.empty());
        
        // Act & Assert
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> metricService.getFilteredMetricData(unknownMetric, null)
        );
        
        assertEquals("Metric not found: " + unknownMetric, exception.getMessage());
        
        // Verify no other interactions occurred
        verify(metricInfoRepository).findByName(eq(unknownMetric));
        verifyNoMoreInteractions(dataFetcherService, jsonDataParser, dataFilteringService);
    }

    @Test
    void testGetMetricData_MetricNotFound() {
        // Arrange
        String unknownMetric = "nonExistentMetric";
        when(metricInfoRepository.findByName(eq(unknownMetric))).thenReturn(Optional.empty());

        // Act & Assert
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class, 
            () -> metricService.getMetricData(unknownMetric)
        );
        
        assertEquals("Metric not found: " + unknownMetric, exception.getMessage());
        
        // Verify no other interactions occurred
        verify(metricInfoRepository).findByName(eq(unknownMetric));
        verifyNoMoreInteractions(metricInfoRepository);
        verifyNoInteractions(dataFetcherService, jsonDataParser, dataFilteringService);
    }

    @Test
    void testGetMetricData_WithCacheHit() throws Exception {
        // Arrange
        String metricName = "test-metric";
        MetricInfo metricInfo = createTestMetricInfo();
        UnifiedDataTable expectedTable = createTestDataTable();
        
        // First call - cache miss
        when(metricInfoRepository.findByName(eq(metricName))).thenReturn(Optional.of(metricInfo));
        when(dataFetcherService.fetchData(anyString()))
            .thenReturn(new ByteArrayInputStream("[{\"id\":1}]".getBytes()));
        when(jsonDataParser.parse(any(InputStream.class), anyMap(), anyString()))
            .thenReturn(expectedTable);
        
        // First call - should populate cache
        UnifiedDataTable firstResult = metricService.getMetricData(metricName);
        assertNotNull(firstResult);
        
        // Reset mocks to verify second call
        Mockito.reset(metricInfoRepository, dataFetcherService, jsonDataParser);
        
        // Second call - should hit cache
        // No need to set up mocks as they shouldn't be called
        UnifiedDataTable secondResult = metricService.getMetricData(metricName);
        
        // Assert
        assertNotNull(secondResult);
        assertSame(expectedTable, secondResult);
        
        // Verify cache hit behavior - should not call any of these methods
        verifyNoInteractions(metricInfoRepository, dataFetcherService, jsonDataParser);
    }
    
    @Test
    void testGetMetricData_WithCacheMiss() throws Exception {
        // Arrange
        String metricName = "test-metric";
        MetricInfo metricInfo = createTestMetricInfo();
        UnifiedDataTable expectedTable = createTestDataTable();
        
        when(metricInfoRepository.findByName(eq(metricName))).thenReturn(Optional.of(metricInfo));
        when(dataFetcherService.fetchData(anyString()))
            .thenReturn(new ByteArrayInputStream("[{\"id\":1}]".getBytes()));
        when(jsonDataParser.parse(any(InputStream.class), anyMap(), anyString()))
            .thenReturn(expectedTable);
        
        // Act
        UnifiedDataTable result = metricService.getMetricData(metricName);
        
        // Assert
        assertNotNull(result);
        assertEquals(expectedTable, result);
        
        // Verify cache miss behavior
        verify(metricInfoRepository).findByName(eq(metricName));
        verify(dataFetcherService).fetchData(eq(metricInfo.getSourceUrl()));
        verify(jsonDataParser).parse(any(InputStream.class), anyMap(), eq(metricInfo.getDataPath()));
    }    
}
