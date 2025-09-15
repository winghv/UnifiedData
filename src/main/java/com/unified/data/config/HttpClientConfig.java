package com.unified.data.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Configuration
@ConfigurationProperties(prefix = "http.client")
public class HttpClientConfig {
    
    private Map<String, TimeoutConfig> services = new ConcurrentHashMap<>();
    private TimeoutConfig defaultTimeout = new TimeoutConfig(Duration.ofSeconds(30), Duration.ofSeconds(5));
    
    public Map<String, TimeoutConfig> getServices() {
        return services;
    }
    
    public void setServices(Map<String, TimeoutConfig> services) {
        this.services = new ConcurrentHashMap<>(services);
    }
    
    public TimeoutConfig getDefaultTimeout() {
        return defaultTimeout;
    }
    
    public void setDefaultTimeout(TimeoutConfig defaultTimeout) {
        this.defaultTimeout = defaultTimeout;
    }
    
    public TimeoutConfig getTimeoutForService(String serviceName) {
        return services.getOrDefault(serviceName, defaultTimeout);
    }
    
    public static class TimeoutConfig {
        private Duration readTimeout;
        private Duration connectTimeout;
        
        public TimeoutConfig() {
        }
        
        public TimeoutConfig(Duration readTimeout, Duration connectTimeout) {
            this.readTimeout = readTimeout;
            this.connectTimeout = connectTimeout;
        }
        
        public Duration getReadTimeout() {
            return readTimeout;
        }
        
        public void setReadTimeout(Duration readTimeout) {
            this.readTimeout = readTimeout;
        }
        
        public Duration getConnectTimeout() {
            return connectTimeout;
        }
        
        public void setConnectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
        }
    }
}