package com.example.unifieddataservice.service;

import com.example.unifieddataservice.model.MetricInfo;
import com.example.unifieddataservice.model.Predicate;
import com.example.unifieddataservice.model.TableDefinition;
import com.example.unifieddataservice.model.UnifiedDataTable;
import com.example.unifieddataservice.service.parser.DataParser;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@SpringBootTest
public class PredicatePushdownIntegrationTest {

    @Autowired
    private SqlQueryService sqlQueryService;

    @Autowired
    private RootAllocator rootAllocator;

    @MockBean
    private TableRegistry tableRegistry;

    @MockBean
    private MetricService metricService;
    
    @MockBean
    private HttpClient httpClient;

    @BeforeEach
    void setUp() throws Exception {
        HttpResponse<byte[]> mockHttpResponse = (HttpResponse<byte[]>) mock(HttpResponse.class);
        when(mockHttpResponse.statusCode()).thenReturn(200);
        when(mockHttpResponse.body()).thenReturn("[]".getBytes(StandardCharsets.UTF_8));
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(mockHttpResponse);

        when(metricService.getMetricData(anyString(), any(List.class)))
            .thenAnswer(invocation -> {
                DataFetcherService realDataFetcherService = new DataFetcherService(httpClient);
                
                DataParser mockParser = mock(DataParser.class);
                Schema emptySchema = new Schema(Collections.emptyList());
                VectorSchemaRoot emptyRoot = VectorSchemaRoot.create(emptySchema, rootAllocator);
                when(mockParser.parse(any(), any(), any(), any(), any())).thenReturn(new UnifiedDataTable("parsed", emptyRoot));

                MetricInfo metricInfo = new MetricInfo();
                metricInfo.setSourceUrl("http://example.com/prices");

                realDataFetcherService.fetchData(metricInfo.getSourceUrl(), invocation.getArgument(1));
                
                // Return a valid, empty UnifiedDataTable
                return new UnifiedDataTable("metric_data", VectorSchemaRoot.create(new Schema(Collections.emptyList()), rootAllocator));
            });
    }

    @Test
    void testPredicatePushdownInUrl() throws Exception {
        // 1. Setup Mock Data
        String tableName = "test_stock";
        TableDefinition tableDef = new TableDefinition();
        tableDef.setTableName(tableName); // Corrected method call
        tableDef.setPrimaryKeys(List.of("ticker"));
        tableDef.setFieldMapping(Map.of("price", "price"));
        tableDef.setMetricFields(Map.of("price", "stock_price_metric"));

        when(tableRegistry.getByName(tableName)).thenReturn(Optional.of(tableDef));
        
        // 2. Execute Query
        String sql = "SELECT price FROM test_stock WHERE ticker = 'AAPL'";
        sqlQueryService.query(sql);

        // 3. Verify Predicate Pushdown by capturing the HTTP request
        ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(httpClient).send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class));

        HttpRequest sentRequest = requestCaptor.getValue();
        String requestedUri = sentRequest.uri().toString();

        assertTrue(requestedUri.startsWith("http://example.com/prices?"), "URL should contain query parameters");
        assertTrue(requestedUri.contains("ticker=AAPL"), "URL should contain the pushed down predicate");
    }
}
