package com.unified.data.http;

import com.unified.data.config.HttpClientConfig;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class HttpClientFactory {
    
    private final HttpClientConfig httpClientConfig;
    private final Map<String, WebClient> clientCache = new ConcurrentHashMap<>();
    private final ConnectionProvider connectionProvider;
    private final ExchangeStrategies exchangeStrategies;
    
    public HttpClientFactory(HttpClientConfig httpClientConfig) {
        this.httpClientConfig = httpClientConfig;
        
        this.connectionProvider = ConnectionProvider.builder("custom")
                .maxConnections(500)
                .maxIdleTime(Duration.ofSeconds(20))
                .maxLifeTime(Duration.ofSeconds(60))
                .pendingAcquireTimeout(Duration.ofSeconds(60))
                .evictInBackground(Duration.ofSeconds(120))
                .build();
        
        this.exchangeStrategies = ExchangeStrategies.builder()
                .codecs(configurer -> configurer
                        .defaultCodecs()
                        .maxInMemorySize(16 * 1024 * 1024))
                .build();
    }
    
    public WebClient getClient(String serviceName) {
        return clientCache.computeIfAbsent(serviceName, this::createClient);
    }
    
    private WebClient createClient(String serviceName) {
        HttpClientConfig.TimeoutConfig timeoutConfig = httpClientConfig.getTimeoutForService(serviceName);
        
        HttpClient httpClient = HttpClient.create(connectionProvider)
                .responseTimeout(timeoutConfig.getReadTimeout())
                .option(io.netty.channel.ChannelOption.CONNECT_TIMEOUT_MILLIS, 
                        (int) timeoutConfig.getConnectTimeout().toMillis())
                .doOnConnected(conn -> conn
                        .addHandlerLast(new io.netty.handler.timeout.ReadTimeoutHandler(
                                (int) timeoutConfig.getReadTimeout().getSeconds()))
                        .addHandlerLast(new io.netty.handler.timeout.WriteTimeoutHandler(
                                (int) timeoutConfig.getReadTimeout().getSeconds())));
        
        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .exchangeStrategies(exchangeStrategies)
                .build();
    }
    
    public void clearCache() {
        clientCache.clear();
    }
    
    public void evictClient(String serviceName) {
        clientCache.remove(serviceName);
    }
}