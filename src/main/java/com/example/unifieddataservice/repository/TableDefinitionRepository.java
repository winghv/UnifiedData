package com.example.unifieddataservice.repository;

import com.example.unifieddataservice.model.TableDefinition;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface TableDefinitionRepository extends JpaRepository<TableDefinition, Long> {
    Optional<TableDefinition> findByTableName(String tableName);
}
