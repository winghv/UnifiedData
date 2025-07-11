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

import static org.mockito.ArgumentMatchers.anyString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
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
    void testQuerySuccess() throws Exception {
        UnifiedDataTable mockTable = Mockito.mock(UnifiedDataTable.class);
        VectorSchemaRoot mockRoot = createMockVectorSchemaRoot();
        Mockito.when(mockTable.getData()).thenReturn(mockRoot);

        Mockito.when(sqlQueryService.query(anyString())).thenReturn(mockTable);

        MvcResult result = mockMvc.perform(post("/api/query")
                .contentType(MediaType.APPLICATION_JSON)
                .content("SELECT * FROM test_table"))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(result))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_OCTET_STREAM_VALUE));
    }



    @Test
    void testErrorHandling() throws Exception {
        Mockito.when(sqlQueryService.query(anyString())).thenThrow(new RuntimeException("Test error"));

        MvcResult result = mockMvc.perform(post("/api/query")
                .contentType(MediaType.APPLICATION_JSON)
                .content("INVALID SQL"))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(result))
                .andExpect(status().isInternalServerError());
    }
    

}
