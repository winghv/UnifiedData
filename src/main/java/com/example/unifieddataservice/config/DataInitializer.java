package com.example.unifieddataservice.config;

import com.example.unifieddataservice.entity.Configuration;
import com.example.unifieddataservice.model.DataSourceType;
import com.example.unifieddataservice.model.DataType;
import com.example.unifieddataservice.model.MetricInfo;
import com.example.unifieddataservice.repository.ConfigurationRepository;
import com.example.unifieddataservice.repository.MetricInfoRepository;
import com.example.unifieddataservice.repository.TableDefinitionRepository;
import com.example.unifieddataservice.model.TableDefinition;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class DataInitializer implements CommandLineRunner {

    private final MetricInfoRepository metricInfoRepository;
    private final ConfigurationRepository configurationRepository;
    private final TableDefinitionRepository tableDefinitionRepository;

    public DataInitializer(MetricInfoRepository metricInfoRepository,
                           ConfigurationRepository configurationRepository,
                           TableDefinitionRepository tableDefinitionRepository) {
        this.metricInfoRepository = metricInfoRepository;
        this.configurationRepository = configurationRepository;
        this.tableDefinitionRepository = tableDefinitionRepository;
    }

    @Override
    public void run(String... args) throws Exception {
        // Initialize MetricInfo data if the table is empty
        if (metricInfoRepository.count() == 0) {
            // Metric 0: Stock Data (CSV from local file)
            MetricInfo stockData = new MetricInfo();
            stockData.setName("stock_data");
            stockData.setDataSourceType(DataSourceType.FILE_CSV);
            stockData.setSourceUrl("/Users/mac/VscodeProjects/UnifiedData/sample-data/stock_data.csv");
            stockData.setDataPath(""); // CSV does not need dataPath
            stockData.setFieldMappings(Map.of(
                "stkcode", DataType.STRING,
                "timestamp", DataType.LONG,
                "macd", DataType.DOUBLE,
                "pe", DataType.DOUBLE,
                "close", DataType.DOUBLE,
                "volume", DataType.LONG
            ));
            metricInfoRepository.save(stockData);

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

        // Initialize Configuration data if the table is empty
        if (configurationRepository.count() == 0) {
            // Stock Market Data Source Configuration
            Configuration stockDataSource = new Configuration();
            stockDataSource.setName("StockMarketDataSource");
            stockDataSource.setConfigType("DataSource");
            stockDataSource.setConfigValue("{\"type\":\"REST_API\",\"endpoint\":\"https://api.marketdata.com/v1/stocks\",\"authType\":\"API_KEY\"}");
            stockDataSource.setDescription("Configuration for connecting to the stock market data provider");
            configurationRepository.save(stockDataSource);

            // Stock Tickers to Monitor
            Configuration stockTickers = new Configuration();
            stockTickers.setName("MonitoredStocks");
            stockTickers.setConfigType("StockList");
            stockTickers.setConfigValue("AAPL,MSFT,GOOGL,AMZN,TSLA,FB,BRK.A,TSM,V,JNJ,JPM");
            stockTickers.setDescription("List of stock tickers to monitor and analyze");
            configurationRepository.save(stockTickers);

            // Market Indicators Configuration
            Configuration marketIndicators = new Configuration();
            marketIndicators.setName("TechnicalIndicators");
            marketIndicators.setConfigType("AnalysisConfig");
            marketIndicators.setConfigValue("{\"movingAverages\":[50,100,200],\"rsiPeriod\":14,\"macd\":{\"fast\":12,\"slow\":26,\"signal\":9}}");
            marketIndicators.setDescription("Technical analysis indicators configuration");
            configurationRepository.save(marketIndicators);

            // Stock Alert Thresholds
            Configuration alertThresholds = new Configuration();
            alertThresholds.setName("PriceAlertThresholds");
            alertThresholds.setConfigType("AlertConfig");
            alertThresholds.setConfigValue("{\"priceChange24h\":5.0,\"volumeSpike\":200.0,\"rsiOverbought\":70,\"rsiOversold\":30}");
            alertThresholds.setDescription("Thresholds for generating stock price and volume alerts");
            configurationRepository.save(alertThresholds);

            // Market Data Refresh Interval
            Configuration refreshConfig = new Configuration();
            refreshConfig.setName("DataRefreshConfig");
            refreshConfig.setConfigType("Scheduler");
            refreshConfig.setConfigValue("{\"intervalMinutes\":15}");
            refreshConfig.setDescription("Configuration for data refresh intervals");
            configurationRepository.save(refreshConfig);
        }

        // Initialize TableDefinition data if the table is empty
        if (tableDefinitionRepository.count() == 0) {
            TableDefinition stockQuoteTable = new TableDefinition();
            stockQuoteTable.setTableName("stock_quote");
            stockQuoteTable.setPrimaryKeys(java.util.Arrays.asList("ticker", "date"));

            stockQuoteTable.setMetricFields(Map.of(
                "ticker", "stock_data",
                "date", "stock_data",
                "price", "stock_data",
                "volume", "stock_data"
            ));

            stockQuoteTable.setFieldMapping(Map.of(
                "ticker", "stkcode",
                "date", "timestamp",
                "price", "close",
                "volume", "volume"
            ));

            stockQuoteTable.setFieldTypes(Map.of(
                "ticker", DataType.STRING,
                "date", DataType.LONG,
                "price", DataType.DOUBLE,
                "volume", DataType.LONG
            ));

            tableDefinitionRepository.save(stockQuoteTable);
        }
    }
}
