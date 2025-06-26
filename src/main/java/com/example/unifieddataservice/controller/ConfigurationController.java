package com.example.unifieddataservice.controller;

import com.example.unifieddataservice.entity.Configuration;
import com.example.unifieddataservice.service.ConfigurationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/configurations")
@CrossOrigin(origins = "*") // Allow all origins for simplicity in development
public class ConfigurationController {

    @Autowired
    private ConfigurationService configurationService;

    @GetMapping
    public List<Configuration> getAllConfigurations() {
        return configurationService.getAllConfigurations();
    }

    @GetMapping("/{id}")
    public ResponseEntity<Configuration> getConfigurationById(@PathVariable Long id) {
        return configurationService.getConfigurationById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping
    public Configuration createConfiguration(@RequestBody Configuration configuration) {
        return configurationService.createConfiguration(configuration);
    }

    @PutMapping("/{id}")
    public ResponseEntity<Configuration> updateConfiguration(@PathVariable Long id, @RequestBody Configuration configurationDetails) {
        try {
            return ResponseEntity.ok(configurationService.updateConfiguration(id, configurationDetails));
        } catch (RuntimeException e) {
            return ResponseEntity.notFound().build();
        }
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteConfiguration(@PathVariable Long id) {
        configurationService.deleteConfiguration(id);
        return ResponseEntity.noContent().build();
    }

    // Placeholder for data preview endpoint
    @GetMapping("/preview/{id}")
    public ResponseEntity<String> previewData(@PathVariable Long id) {
        // This is a placeholder. In a real application, you would fetch data based on the configuration.
        return configurationService.getConfigurationById(id)
                .map(config -> ResponseEntity.ok("Previewing data for: " + config.getName()))
                .orElse(ResponseEntity.notFound().build());
    }
}
