package com.example.unifieddataservice.config;

import com.example.unifieddataservice.model.DataSourceType;
import com.example.unifieddataservice.model.DataType;
import com.example.unifieddataservice.model.MetricInfo;
import com.example.unifieddataservice.repository.MetricInfoRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class DataInitializer implements CommandLineRunner {

    private final MetricInfoRepository metricInfoRepository;

    public DataInitializer(MetricInfoRepository metricInfoRepository) {
        this.metricInfoRepository = metricInfoRepository;
    }

    @Override
    public void run(String... args) throws Exception {
        // Metric 1: Posts (JSON)
        MetricInfo posts = new MetricInfo();
        posts.setName("posts");
        posts.setDataSourceType(DataSourceType.HTTP_JSON);
        posts.setSourceUrl("https://jsonplaceholder.typicode.com/posts");
        posts.setDataPath(""); // Root is an array
        posts.setFieldMappings(Map.of(
            "id", DataType.LONG,
            "userId", DataType.LONG,
            "title", DataType.STRING,
            "body", DataType.STRING
        ));
        metricInfoRepository.save(posts);

        // Metric 2: Users (JSON)
        MetricInfo users = new MetricInfo();
        users.setName("users");
        users.setDataSourceType(DataSourceType.HTTP_JSON);
        users.setSourceUrl("https://jsonplaceholder.typicode.com/users");
        users.setDataPath(""); // Root is an array
        users.setFieldMappings(Map.of(
            "id", DataType.LONG,
            "name", DataType.STRING,
            "username", DataType.STRING,
            "email", DataType.STRING,
            "phone", DataType.STRING,
            "website", DataType.STRING
        ));
        metricInfoRepository.save(users);
    }
}
