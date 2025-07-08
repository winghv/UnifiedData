package com.example.unifieddataservice.service;

import com.example.unifieddataservice.model.TableDefinition;
import com.example.unifieddataservice.repository.TableDefinitionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class TableDefinitionService {
    private static final Logger logger = LoggerFactory.getLogger(TableDefinitionService.class);

    private final TableDefinitionRepository repository;

    public TableDefinitionService(TableDefinitionRepository repository) {
        this.repository = repository;
    }

    public TableDefinition save(TableDefinition tableDefinition) {
        logger.info("Saving table definition: {}", tableDefinition.getTableName());
        return repository.save(tableDefinition);
    }

    public List<TableDefinition> findAll() {
        logger.info("Fetching all table definitions");
        return repository.findAll();
    }

    public Optional<TableDefinition> findById(Long id) {
        logger.info("Fetching table definition by id: {}", id);
        return repository.findById(id);
    }

    public Optional<TableDefinition> findByTableName(String tableName) {
        logger.info("Fetching table definition by name: {}", tableName);
        return repository.findByTableName(tableName);
    }

    public TableDefinition update(Long id, TableDefinition tableDetails) {
        logger.info("Updating table definition id: {}", id);
        TableDefinition table = repository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Table definition not found with id: " + id));

        table.setTableName(tableDetails.getTableName());
        table.setPrimaryKeys(tableDetails.getPrimaryKeys());
        table.setMetricFields(tableDetails.getMetricFields());
        table.setTimeGranularity(tableDetails.getTimeGranularity());

        return repository.save(table);
    }

    public void delete(Long id) {
        logger.info("Deleting table definition id: {}", id);
        if (!repository.existsById(id)) {
            throw new IllegalArgumentException("Table definition not found with id: " + id);
        }
        repository.deleteById(id);
    }
}
