package com.example.unifieddataservice.service;

import com.example.unifieddataservice.model.TableDefinition;
import com.example.unifieddataservice.repository.TableDefinitionRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * In-memory registry of logical tables, backed by JPA repository.
 * On startup it loads all table definitions; can be refreshed at runtime.
 */
@Component
public class TableRegistry {

    private final TableDefinitionRepository repository;
    private Map<String, TableDefinition> cache = new HashMap<>();

    public TableRegistry(TableDefinitionRepository repository) {
        this.repository = repository;
        refresh();
    }

    /**
     * Reload definitions from DB into memory cache.
     */
    @Transactional(readOnly = true)
    public synchronized void refresh() {
        List<TableDefinition> all = repository.findAll();
        Map<String, TableDefinition> tmp = new HashMap<>();
        for (TableDefinition td : all) {
            tmp.put(td.getTableName().toLowerCase(), td);
        }
        this.cache = tmp;
    }

    public Optional<TableDefinition> getByName(String tableName) {
        return Optional.ofNullable(cache.get(tableName.toLowerCase()));
    }

    public boolean exists(String tableName) {
        return cache.containsKey(tableName.toLowerCase());
    }
}
