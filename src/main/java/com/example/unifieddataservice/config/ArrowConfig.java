package com.example.unifieddataservice.config;

import org.apache.arrow.memory.RootAllocator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ArrowConfig {

    @Bean(destroyMethod = "close")
    public RootAllocator rootAllocator() {
        // Create a new RootAllocator with a virtually unlimited size.
        // This can be configured from application.properties if needed.
        return new RootAllocator(Long.MAX_VALUE);
    }
}
