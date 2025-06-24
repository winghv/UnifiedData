package com.example.unifieddataservice.config;

import com.example.unifieddataservice.model.UnifiedDataTable;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.concurrent.TimeUnit;

@Configuration
@EnableCaching
public class CacheConfig {

    private static final Logger logger = LoggerFactory.getLogger(CacheConfig.class);

    @Bean
    @Primary
    public CacheManager cacheManager() {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager("metrics");
        
        // Create a type-safe Caffeine cache configuration
        Caffeine<Object, Object> caffeine = Caffeine.newBuilder()
                .maximumSize(100)  // Maximum number of entries in the cache
                .expireAfterWrite(1, TimeUnit.HOURS)  // Cache entries expire after 1 hour
                .recordStats()  // Enable cache statistics
                .removalListener((RemovalListener<Object, Object>) (key, value, cause) -> {
                    if (value != null && cause.wasEvicted() && value instanceof UnifiedDataTable) {
                        try {
                            logger.debug("Evicting and closing UnifiedDataTable for key: {}", key);
                            ((UnifiedDataTable) value).close();
                        } catch (Exception e) {
                            logger.error("Error closing UnifiedDataTable for key: " + key, e);
                        }
                    }
                });
                
        // Set the Caffeine cache configuration
        cacheManager.setCaffeine(caffeine);
        
        return cacheManager;
    }
}
