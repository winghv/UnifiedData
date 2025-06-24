package com.example.unifieddataservice.repository;

import com.example.unifieddataservice.model.MetricInfo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface MetricInfoRepository extends JpaRepository<MetricInfo, Long> {
    Optional<MetricInfo> findByName(String name);
}