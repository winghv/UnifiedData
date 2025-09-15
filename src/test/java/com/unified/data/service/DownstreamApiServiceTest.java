package com.unified.data.service;

import com.unified.data.http.HttpClientFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DownstreamApiServiceTest {
    
    @Mock
    private HttpClientFactory httpClientFactory;
    
    @Mock
    private WebClient webClient;
    
    @Mock
    private WebClient.RequestHeadersUriSpec requestHeadersUriSpec;
    
    @Mock
    private WebClient.RequestHeadersSpec requestHeadersSpec;
    
    @Mock
    private WebClient.RequestBodyUriSpec requestBodyUriSpec;
    
    @Mock
    private WebClient.RequestBodySpec requestBodySpec;
    
    @Mock
    private WebClient.ResponseSpec responseSpec;
    
    private DownstreamApiService downstreamApiService;
    
    @BeforeEach
    void setUp() {
        downstreamApiService = new DownstreamApiService(httpClientFactory);
    }
    
    @Test
    void testCallService() {
        String serviceName = "test-service";
        String url = "http://example.com/api";
        String expectedResponse = "test response";
        
        when(httpClientFactory.getClient(serviceName)).thenReturn(webClient);
        when(webClient.get()).thenReturn(requestHeadersUriSpec);
        when(requestHeadersUriSpec.uri(url)).thenReturn(requestHeadersSpec);
        when(requestHeadersSpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(expectedResponse));
        
        Mono<String> result = downstreamApiService.callService(serviceName, url, String.class);
        
        StepVerifier.create(result)
                .expectNext(expectedResponse)
                .verifyComplete();
        
        verify(httpClientFactory).getClient(serviceName);
    }
    
    @Test
    void testPostToService() {
        String serviceName = "test-service";
        String url = "http://example.com/api";
        Map<String, String> requestBody = new HashMap<>();
        requestBody.put("key", "value");
        String expectedResponse = "post response";
        
        when(httpClientFactory.getClient(serviceName)).thenReturn(webClient);
        when(webClient.post()).thenReturn(requestBodyUriSpec);
        when(requestBodyUriSpec.uri(url)).thenReturn(requestBodySpec);
        when(requestBodySpec.bodyValue(requestBody)).thenReturn(requestHeadersSpec);
        when(requestHeadersSpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(expectedResponse));
        
        Mono<String> result = downstreamApiService.postToService(serviceName, url, requestBody, String.class);
        
        StepVerifier.create(result)
                .expectNext(expectedResponse)
                .verifyComplete();
        
        verify(httpClientFactory).getClient(serviceName);
    }
    
    @Test
    void testCallServiceAsync() throws Exception {
        String serviceName = "test-service";
        String url = "http://example.com/api";
        String expectedResponse = "async response";
        
        when(httpClientFactory.getClient(serviceName)).thenReturn(webClient);
        when(webClient.get()).thenReturn(requestHeadersUriSpec);
        when(requestHeadersUriSpec.uri(url)).thenReturn(requestHeadersSpec);
        when(requestHeadersSpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(expectedResponse));
        
        CompletableFuture<String> result = downstreamApiService.callServiceAsync(serviceName, url, String.class);
        
        assertEquals(expectedResponse, result.get());
    }
    
    @Test
    void testCallServiceWithHeaders() {
        String serviceName = "test-service";
        String url = "http://example.com/api";
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer token");
        headers.put("X-Custom-Header", "value");
        String expectedResponse = "response with headers";
        
        when(httpClientFactory.getClient(serviceName)).thenReturn(webClient);
        when(webClient.get()).thenReturn(requestHeadersUriSpec);
        when(requestHeadersUriSpec.uri(url)).thenReturn(requestHeadersSpec);
        when(requestHeadersSpec.header(anyString(), anyString())).thenReturn(requestHeadersSpec);
        when(requestHeadersSpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(expectedResponse));
        
        Mono<String> result = downstreamApiService.callServiceWithHeaders(serviceName, url, headers, String.class);
        
        StepVerifier.create(result)
                .expectNext(expectedResponse)
                .verifyComplete();
        
        verify(requestHeadersSpec, times(2)).header(anyString(), anyString());
    }
    
    @Test
    void testCallServiceHandlesError() {
        String serviceName = "test-service";
        String url = "http://example.com/api";
        RuntimeException error = new RuntimeException("Network error");
        
        when(httpClientFactory.getClient(serviceName)).thenReturn(webClient);
        when(webClient.get()).thenReturn(requestHeadersUriSpec);
        when(requestHeadersUriSpec.uri(url)).thenReturn(requestHeadersSpec);
        when(requestHeadersSpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.error(error));
        
        Mono<String> result = downstreamApiService.callService(serviceName, url, String.class);
        
        StepVerifier.create(result)
                .expectError(RuntimeException.class)
                .verify();
    }
}