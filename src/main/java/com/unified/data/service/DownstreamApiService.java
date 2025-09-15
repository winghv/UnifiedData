package com.unified.data.service;

import com.unified.data.http.HttpClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
public class DownstreamApiService {
    
    private static final Logger logger = LoggerFactory.getLogger(DownstreamApiService.class);
    
    private final HttpClientFactory httpClientFactory;
    
    public DownstreamApiService(HttpClientFactory httpClientFactory) {
        this.httpClientFactory = httpClientFactory;
    }
    
    public <T> Mono<T> callService(String serviceName, String url, Class<T> responseType) {
        WebClient client = httpClientFactory.getClient(serviceName);
        
        return client.get()
                .uri(url)
                .retrieve()
                .bodyToMono(responseType)
                .doOnSuccess(response -> logger.debug("Successfully called service: {} at URL: {}", serviceName, url))
                .doOnError(error -> logger.error("Error calling service: {} at URL: {}", serviceName, url, error));
    }
    
    public <T> Mono<T> postToService(String serviceName, String url, Object requestBody, Class<T> responseType) {
        WebClient client = httpClientFactory.getClient(serviceName);
        
        return client.post()
                .uri(url)
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(responseType)
                .doOnSuccess(response -> logger.debug("Successfully posted to service: {} at URL: {}", serviceName, url))
                .doOnError(error -> logger.error("Error posting to service: {} at URL: {}", serviceName, url, error));
    }
    
    public <T> CompletableFuture<T> callServiceAsync(String serviceName, String url, Class<T> responseType) {
        return callService(serviceName, url, responseType).toFuture();
    }
    
    public <T> CompletableFuture<T> postToServiceAsync(String serviceName, String url, Object requestBody, Class<T> responseType) {
        return postToService(serviceName, url, requestBody, responseType).toFuture();
    }
    
    public <T> Mono<T> callServiceWithHeaders(String serviceName, String url, Map<String, String> headers, Class<T> responseType) {
        WebClient client = httpClientFactory.getClient(serviceName);
        
        WebClient.RequestHeadersSpec<?> request = client.get().uri(url);
        headers.forEach(request::header);
        
        return request
                .retrieve()
                .bodyToMono(responseType)
                .doOnSuccess(response -> logger.debug("Successfully called service with headers: {} at URL: {}", serviceName, url))
                .doOnError(error -> logger.error("Error calling service with headers: {} at URL: {}", serviceName, url, error));
    }
}