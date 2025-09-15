package com.unified.data.http;

import com.unified.data.config.HttpClientConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class HttpClientFactoryTest {
    
    private HttpClientFactory httpClientFactory;
    private HttpClientConfig httpClientConfig;
    
    @BeforeEach
    void setUp() {
        httpClientConfig = new HttpClientConfig();
        
        HttpClientConfig.TimeoutConfig defaultTimeout = new HttpClientConfig.TimeoutConfig(
                Duration.ofSeconds(30), Duration.ofSeconds(5));
        httpClientConfig.setDefaultTimeout(defaultTimeout);
        
        Map<String, HttpClientConfig.TimeoutConfig> services = new HashMap<>();
        services.put("fast-service", new HttpClientConfig.TimeoutConfig(
                Duration.ofSeconds(5), Duration.ofSeconds(1)));
        services.put("slow-service", new HttpClientConfig.TimeoutConfig(
                Duration.ofSeconds(60), Duration.ofSeconds(10)));
        httpClientConfig.setServices(services);
        
        httpClientFactory = new HttpClientFactory(httpClientConfig);
    }
    
    @Test
    void testGetClientReturnsWebClient() {
        WebClient client = httpClientFactory.getClient("fast-service");
        assertNotNull(client);
    }
    
    @Test
    void testGetClientCachesInstances() {
        WebClient client1 = httpClientFactory.getClient("fast-service");
        WebClient client2 = httpClientFactory.getClient("fast-service");
        assertSame(client1, client2);
    }
    
    @Test
    void testGetClientWithDifferentServices() {
        WebClient fastClient = httpClientFactory.getClient("fast-service");
        WebClient slowClient = httpClientFactory.getClient("slow-service");
        assertNotSame(fastClient, slowClient);
    }
    
    @Test
    void testGetClientWithUnknownServiceUsesDefault() {
        WebClient client = httpClientFactory.getClient("unknown-service");
        assertNotNull(client);
    }
    
    @Test
    void testClearCache() {
        WebClient client1 = httpClientFactory.getClient("fast-service");
        httpClientFactory.clearCache();
        WebClient client2 = httpClientFactory.getClient("fast-service");
        assertNotSame(client1, client2);
    }
    
    @Test
    void testEvictClient() {
        WebClient client1 = httpClientFactory.getClient("fast-service");
        httpClientFactory.evictClient("fast-service");
        WebClient client2 = httpClientFactory.getClient("fast-service");
        assertNotSame(client1, client2);
    }
}