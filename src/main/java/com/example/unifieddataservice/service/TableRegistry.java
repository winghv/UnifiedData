package com.example.unifieddataservice.service;

import com.example.unifieddataservice.event.TableRegistryRefreshEvent;
import com.example.unifieddataservice.model.TableDefinition;
import com.example.unifieddataservice.repository.TableDefinitionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Component
@DependsOn("dataInitializer")
public class TableRegistry implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(TableRegistry.class);

    private final TableDefinitionRepository repository;
    private Map<String, TableDefinition> cache = new HashMap<>();

    public TableRegistry(TableDefinitionRepository repository) {
        this.repository = repository;
    }

    @Override
    public void afterPropertiesSet() {
        logger.info("Initializing TableRegistry cache...");
        try {
            refresh();
        } catch (Exception e) {
            logger.error("Error initializing TableRegistry cache", e);
            throw e;
        }
    }

    @Transactional(readOnly = true)
    public synchronized void refresh() {
        logger.info("Refreshing TableRegistry cache...");
        List<TableDefinition> all = repository.findAll();
        logger.info("Found {} table definitions in repository", all.size());

        Map<String, TableDefinition> tmp = new HashMap<>();
        for (TableDefinition td : all) {
            String tableName = td.getTableName();
            if (tableName != null) {
                String lowerName = tableName.toLowerCase();
                logger.debug("Registering table: '{}' as '{}'", tableName, lowerName);
                tmp.put(lowerName, td);
            }
        }

        this.cache = tmp;
        logger.info("TableRegistry cache refreshed. Current tables in cache: {}",
                cache.keySet().stream().collect(Collectors.joining(", ", "[", "]")));
    }

    public Optional<TableDefinition> getByName(String name) {
        if (name == null || name.trim().isEmpty()) {
            logger.warn("Empty or null table name provided");
            return Optional.empty();
        }
        String lookupKey = name.toLowerCase().trim();
        TableDefinition result = cache.get(lookupKey);
        return Optional.ofNullable(result);
    }

    public boolean exists(String tableName) {
        return cache.containsKey(tableName.toLowerCase());
    }

    @EventListener
    public void handleTableRegistryRefresh(TableRegistryRefreshEvent event) {
        logger.info("Received TableRegistryRefreshEvent, refreshing cache...");
        refresh();
        logger.info("TableRegistry cache refreshed. Current tables in cache: {}",
                cache.keySet().stream().collect(Collectors.joining(", ", "[", "]")));
    }

    public List<String> getAllTableNames() {
        return cache.values().stream()
                .map(TableDefinition::getTableName)
                .collect(Collectors.toList());
    }
}