package com.example.unifieddataservice.service.parser;

import com.example.unifieddataservice.model.DataType;
import com.example.unifieddataservice.model.UnifiedDataTable;

import java.io.InputStream;
import java.util.Map;

public interface DataParser {
    UnifiedDataTable parse(InputStream data, Map<String, DataType> fieldMappings, String dataPath);
}
