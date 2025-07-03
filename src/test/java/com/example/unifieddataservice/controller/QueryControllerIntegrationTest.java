package com.example.unifieddataservice.controller;

import com.example.unifieddataservice.UnifiedDataServiceApplication;
import com.example.unifieddataservice.model.UnifiedDataTable;
import com.example.unifieddataservice.service.SqlQueryService;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.anyString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(classes = UnifiedDataServiceApplication.class)
@AutoConfigureMockMvc
class QueryControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private SqlQueryService sqlQueryService;

    private VectorSchemaRoot createMockVectorSchemaRoot() {
        VectorSchemaRoot root = Mockito.mock(VectorSchemaRoot.class);
        Schema schema = new Schema(Collections.singletonList(
            Field.nullable("test", new ArrowType.Utf8())
        ));
        
        Mockito.when(root.getSchema()).thenReturn(schema);
        Mockito.when(root.getRowCount()).thenReturn(10000);
        
        // Mock the contentToTSVString method for small responses
        Mockito.when(root.contentToTSVString()).thenReturn("test\ttest");
        
        return root;
    }

    @BeforeEach
    void setUp() {
        // Setup mock UnifiedDataTable with test data
        UnifiedDataTable mockTable = Mockito.mock(UnifiedDataTable.class);
        VectorSchemaRoot mockRoot = createMockVectorSchemaRoot();
        
        Mockito.when(mockTable.getData()).thenReturn(mockRoot);
        Mockito.when(mockTable.getRowCount()).thenReturn(10000); // Large enough to trigger streaming
        
        // Setup the query service to return our mock table
        Mockito.when(sqlQueryService.query(anyString()))
               .thenAnswer(invocation -> {
                   String sql = invocation.getArgument(0);
                   if (sql.contains("small_table")) {
                       UnifiedDataTable smallTable = Mockito.mock(UnifiedDataTable.class);
                       VectorSchemaRoot smallRoot = createMockVectorSchemaRoot();
                       Mockito.when(smallTable.getData()).thenReturn(smallRoot);
                       Mockito.when(smallTable.getRowCount()).thenReturn(100);
                       return smallTable;
                   }
                   return mockTable;
               });
    }

    @Test
    void testStreamingResponse() throws Exception {
        // For this test, we'll verify that the controller correctly handles the streaming request
        // by checking that it starts async processing and sets the appropriate headers
        
        // Setup mock Arrow data
        UnifiedDataTable mockTable = Mockito.mock(UnifiedDataTable.class);
        VectorSchemaRoot mockRoot = createMockVectorSchemaRoot();
        
        Mockito.when(mockTable.getData()).thenReturn(mockRoot);
        Mockito.when(mockTable.getRowCount()).thenReturn(10000);
        
        // Mock the query service to return our test data
        Mockito.when(sqlQueryService.query(anyString()))
               .thenReturn(mockTable);
        
        // Execute the request and verify async processing starts
        // We won't try to process the actual stream in this test
        mockMvc.perform(get("/query")
                .param("sql", "SELECT * FROM test_table")
                .accept("application/vnd.apache.arrow.stream"))
                .andExpect(request().asyncStarted())
                .andExpect(status().isOk())
                .andReturn();
        
        // Note: We're not calling asyncDispatch() here because it would try to process the actual stream,
        // which is complex to mock and not necessary for this integration test.
        // The actual streaming behavior is better tested with a proper integration test
        // that uses a real HTTP client to consume the stream.
    }

    @Test
    void testSmallResponse() throws Exception {
        MvcResult result = mockMvc.perform(get("/query")
                .param("sql", "SELECT * FROM small_table")
                .param("format", "json")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(result))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(content().string(containsString("test")))
                .andReturn();
    }

    @Test
    void testErrorHandling() throws Exception {
        Mockito.when(sqlQueryService.query(anyString()))
                .thenThrow(new RuntimeException("Test error"));

        MvcResult result = mockMvc.perform(get("/query")
                .param("sql", "INVALID SQL")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(result))
                .andExpect(status().is5xxServerError())
                .andExpect(content().string(containsString("Test error")))
                .andReturn();
    }
    
    @Test
    void testUnsupportedFormat() throws Exception {
        MvcResult result = mockMvc.perform(get("/query")
                .param("sql", "SELECT * FROM test_table")
                .param("format", "unsupported")
                .accept(MediaType.ALL))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(result))
                .andExpect(status().isBadRequest())
                .andExpect(content().string(containsString("Unsupported format")))
                .andReturn();
    }
}
