package com.example.unifieddataservice.service;

import com.example.unifieddataservice.entity.Configuration;
import com.example.unifieddataservice.repository.ConfigurationRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class ConfigurationService {

    @Autowired
    private ConfigurationRepository configurationRepository;

    public List<Configuration> getAllConfigurations() {
        return configurationRepository.findAll();
    }

    public Optional<Configuration> getConfigurationById(Long id) {
        return configurationRepository.findById(id);
    }

    public Configuration createConfiguration(Configuration configuration) {
        return configurationRepository.save(configuration);
    }

    public Configuration updateConfiguration(Long id, Configuration configurationDetails) {
        Configuration configuration = configurationRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Configuration not found with id: " + id));

        configuration.setName(configurationDetails.getName());
        configuration.setConfigType(configurationDetails.getConfigType());
        configuration.setConfigValue(configurationDetails.getConfigValue());
        configuration.setDescription(configurationDetails.getDescription());

        return configurationRepository.save(configuration);
    }

    public void deleteConfiguration(Long id) {
        configurationRepository.deleteById(id);
    }
}
