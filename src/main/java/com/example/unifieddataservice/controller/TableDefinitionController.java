package com.example.unifieddataservice.controller;

import com.example.unifieddataservice.model.TableDefinition;
import com.example.unifieddataservice.service.TableDefinitionService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/tables")
public class TableDefinitionController {

    private final TableDefinitionService service;

    public TableDefinitionController(TableDefinitionService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<TableDefinition> create(@RequestBody TableDefinition tableDefinition) {
        TableDefinition createdTable = service.save(tableDefinition);
        return new ResponseEntity<>(createdTable, HttpStatus.CREATED);
    }

    @GetMapping
    public ResponseEntity<List<TableDefinition>> getAll() {
        List<TableDefinition> tables = service.findAll();
        return new ResponseEntity<>(tables, HttpStatus.OK);
    }

    @GetMapping("/{id}")
    public ResponseEntity<TableDefinition> getById(@PathVariable Long id) {
        return service.findById(id)
                .map(table -> new ResponseEntity<>(table, HttpStatus.OK))
                .orElse(new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    @GetMapping("/byName/{tableName}")
    public ResponseEntity<TableDefinition> getByTableName(@PathVariable String tableName) {
        return service.findByTableName(tableName)
                .map(table -> new ResponseEntity<>(table, HttpStatus.OK))
                .orElse(new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    @PutMapping("/{id}")
    public ResponseEntity<TableDefinition> update(@PathVariable Long id, @RequestBody TableDefinition tableDefinition) {
        TableDefinition updatedTable = service.update(id, tableDefinition);
        return new ResponseEntity<>(updatedTable, HttpStatus.OK);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        service.delete(id);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
}
