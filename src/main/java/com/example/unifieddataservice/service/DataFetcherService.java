package com.example.unifieddataservice.service;

import com.example.unifieddataservice.model.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Service
public class DataFetcherService {
    private static final Logger logger = LoggerFactory.getLogger(DataFetcherService.class);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private final HttpClient httpClient;

    public DataFetcherService(HttpClient httpClient) {
        Objects.requireNonNull(httpClient, "HttpClient cannot be null");
        this.httpClient = httpClient;
        logger.info("DataFetcherService initialized with HttpClient: {}", 
                  httpClient != null ? "provided" : "null");
    }

        public InputStream fetchData(String url) {
        return fetchData(url, Collections.emptyList());
    }

    public InputStream fetchData(String url, List<Predicate> predicates) {
        if (url == null || url.trim().isEmpty()) {
            String errorMsg = "URL cannot be null or empty";
            logger.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }

        logger.info("Fetching data from URL: {}", url);
        
        // 判断是否为本地文件路径
        if (url.startsWith("file://")) {
            String filePath = url.substring("file://".length());
            try {
                logger.info("Reading local file: {}", filePath);
                return new java.io.FileInputStream(filePath);
            } catch (IOException e) {
                String errorMsg = String.format("Failed to read local file: %s. Error: %s", filePath, e.getMessage());
                logger.error(errorMsg, e);
                throw new RuntimeException(errorMsg, e);
            }
        } else if (!url.startsWith("http://") && !url.startsWith("https://")) {
            // 直接认为是本地路径
            try {
                logger.info("Reading local file: {}", url);
                return new java.io.FileInputStream(url);
            } catch (IOException e) {
                String errorMsg = String.format("Failed to read local file: %s. Error: %s", url, e.getMessage());
                logger.error(errorMsg, e);
                throw new RuntimeException(errorMsg, e);
            }
        }

        // Build URL with predicates for HTTP sources
        String finalUrl = url;
        if ((url.startsWith("http://") || url.startsWith("https://")) && predicates != null && !predicates.isEmpty()) {
            finalUrl = buildUrlWithPredicates(url, predicates);
        }

        try {
            // Create HTTP request with timeout
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(createUri(finalUrl))
                    .timeout(REQUEST_TIMEOUT)
                    .header("Accept", "application/json")
                    .GET()
                    .build();

            logger.debug("Sending HTTP request to: {}", finalUrl);
            
            // Send request and get response
            HttpResponse<byte[]> response = httpClient.send(
                request, 
                HttpResponse.BodyHandlers.ofByteArray()
            );
            
            int statusCode = response.statusCode();
            logger.debug("Received HTTP response. Status: {}, Content-Length: {}", 
                       statusCode, response.body() != null ? response.body().length : 0);
            
            if (statusCode < 200 || statusCode >= 300) {
                String errorBody = response.body() != null ? 
                    new String(response.body(), StandardCharsets.UTF_8) : "<no body>";
                String errorMsg = String.format("HTTP request failed with status %d for URL %s. Response: %s", 
                                               statusCode, finalUrl, errorBody);
                logger.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }
            
            if (response.body() == null || response.body().length == 0) {
                logger.warn("Received empty response body from URL: {}", finalUrl);
                return new ByteArrayInputStream(new byte[0]);
            }
            
            // Log first 500 characters of the response for debugging
            String responsePreview = new String(response.body(), StandardCharsets.UTF_8);
            int previewLength = Math.min(500, responsePreview.length());
            logger.debug("Response preview (first {} chars): {}", 
                        previewLength, 
                        responsePreview.substring(0, previewLength) + 
                        (responsePreview.length() > previewLength ? "..." : ""));
            
            return new ByteArrayInputStream(response.body());
            
        } catch (URISyntaxException e) {
            String errorMsg = String.format("Invalid URL format: %s. Error: %s", finalUrl, e.getMessage());
            logger.error(errorMsg, e);
            throw new IllegalArgumentException(errorMsg, e);
            
        } catch (IOException e) {
            String errorMsg = String.format("I/O error while fetching data from %s: %s", finalUrl, e.getMessage());
            logger.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            String errorMsg = String.format("Request to %s was interrupted", finalUrl);
            logger.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
            
        } catch (Exception e) {
            String errorMsg = String.format("Unexpected error while fetching data from %s: %s", finalUrl, e.getMessage());
            logger.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }
    }

    private String buildUrlWithPredicates(String baseUrl, List<Predicate> predicates) {
        if (predicates == null || predicates.isEmpty()) {
            return baseUrl;
        }
        StringBuilder sb = new StringBuilder(baseUrl);
        // Use contains to check for existing query params
        sb.append(baseUrl.contains("?") ? "&" : "?");

        for (int i = 0; i < predicates.size(); i++) {
            Predicate p = predicates.get(i);
            // This is a simplified conversion. A real implementation would need URL encoding
            // and more robust operator mapping.
            sb.append(p.columnName()).append("=").append(p.value().toString());
            if (i < predicates.size() - 1) {
                sb.append("&");
            }
        }
        String finalUrl = sb.toString();
        logger.info("Built URL with predicates: {}", finalUrl);
        return finalUrl;
    }
    
    private URI createUri(String url) throws URISyntaxException {
        try {
            return new URI(url);
        } catch (URISyntaxException e) {
            logger.error("Failed to create URI from: " + url, e);
            throw e;
        }
    }
}
