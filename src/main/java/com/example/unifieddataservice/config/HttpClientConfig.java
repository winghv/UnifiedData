package com.example.unifieddataservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.http.HttpClient;
import java.time.Duration;

@Configuration
public class HttpClientConfig {

    @Bean
    public HttpClient httpClient() {
        return HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10)) // 10 seconds connection timeout
                .followRedirects(HttpClient.Redirect.NORMAL) // Follow redirects
                .version(HttpClient.Version.HTTP_2) // Use HTTP/2 if possible, fallback to HTTP/1.1
                .build();
    }
}
