# Unified Data Service / 统一数据服务

[![Java Version](https://img.shields.io/badge/Java-17%2B-blue)](https://www.oracle.com/java/technologies/javase/jdk17-archive-downloads.html)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2.5-green)](https://spring.io/projects/spring-boot)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## 📖 概述 / Overview

Unified Data Service is a high-performance Spring Boot application designed to fetch, parse, filter, and serve metric data from various sources. It leverages Apache Arrow for efficient in-memory columnar data processing, providing a fast and scalable solution for data unification.

统一数据服务是一个基于 Java 的数据服务，旨在从多种异构数据源（如提供 JSON 或 CSV 的 HTTP 接口）获取、处理并统一数据。它将不同来源的数据转换为一致的中间格式（Apache Arrow Table），并通过统一的查询接口提供服务。

## ✨ 核心优势 / Key Features

- **配置驱动 / Configuration-Driven**
  - 无需修改代码，仅通过在数据库中添加元数据定义，即可动态接入新的数据指标
  - No code changes needed, dynamically integrate new data metrics by adding metadata definitions to the database

- **高度可扩展 / Highly Extensible**
  - 采用策略模式，通过实现 `DataParser` 接口，轻松添加新的数据格式解析能力
  - Easily add support for new data formats by implementing the `DataParser` interface

- **高性能 / High Performance**
  - 使用 Apache Arrow 进行高效的内存列式数据处理
  - Efficient in-memory columnar data processing with Apache Arrow
  - 基于 Caffeine 的缓存层，提高重复查询性能
  - Caffeine-based caching layer for improved performance on repeated queries

- **统一与标准化 / Unification & Standardization**
  - 将多格式的外部数据源统一清洗为标准化的中间数据结构
  - Unify and clean multi-format data sources into standardized intermediate data structures

## 🚀 快速开始 / Quick Start

### 环境要求 / Prerequisites

- Java 17 或更高版本 / JDK 17 or later
- Apache Maven 3.6+

### 构建项目 / Build the Project

```bash
mvn clean install
```

### 运行应用 / Run the Application

```bash
mvn spring-boot:run
```

应用将在 `http://localhost:8080` 启动 / The application will start at `http://localhost:8080`

## 📚 技术栈 / Tech Stack

- **核心框架 / Core Framework**
  - Java 17
  - Spring Boot 3.2.5
  - Spring Data JPA
  - Spring Cache (Caffeine)

- **数据处理 / Data Processing**
  - Apache Arrow 15.0.0
  - Jackson (JSON)
  - Apache Commons CSV

- **数据库 / Database**
  - H2 (内存数据库 / In-memory)
  - Spring Data JPA

- **工具 / Tools**
  - Lombok
  - JUnit 5
  - Mockito
  - Spring Boot Actuator

## 🔍 API 文档 / API Documentation

### 获取指标数据 / Get Metric Data

- **URL**: `GET /api/metrics/{metricName}`
- **Response**: `text/csv`

#### 参数 / Parameters

| 参数 / Parameter | 类型 / Type | 必填 / Required | 描述 / Description |
|----------------|------------|----------------|-------------------|
| metricName     | Path       | 是 / Yes       | 指标名称 / Metric name (e.g., `user_signups`, `system_load`) |
| filter         | Query      | 否 / No        | 过滤表达式 / Filter expression (e.g., `age > 30`, `region == 'US'`) |

#### 操作符 / Operators
- 比较: `==`, `!=`, `>`, `<`, `>=`, `<=`
- 字符串值必须用单引号括起来 / String values must be enclosed in single quotes
- 数值不需要引号 / Numeric values should not be quoted

#### 示例 / Examples

```bash
# 获取未过滤的数据 / Get unfiltered data
curl http://localhost:8080/api/metrics/user_signups

# 获取年龄大于30的用户数据 / Get users older than 30
curl "http://localhost:8080/api/metrics/user_signups?filter=age > 30"

# 获取美国地区用户数据 / Get users from US region
curl "http://localhost:8080/api/metrics/user_signups?filter=region == 'US'"
```

## 🏗️ 项目结构 / Project Structure

```
src/main/java/com/example/unifieddataservice/
├── config/               # 配置类 / Configuration classes
│   ├── CacheConfig.java   # 缓存配置 / Cache configuration
│   ├── DataInitializer.java # 数据初始化 / Data initialization
│   └── HttpClientConfig.java # HTTP客户端配置 / HTTP client configuration
├── controller/           # 控制器 / Controllers
│   └── MetricController.java # 指标API / Metrics API
├── model/                # 数据模型 / Data models
│   ├── DataSourceType.java # 数据源类型枚举 / Data source type enum
│   ├── DataType.java     # 数据类型枚举 / Data type enum
│   ├── MetricInfo.java   # 指标信息实体 / Metric info entity
│   └── UnifiedDataTable.java # 统一数据表 / Unified data table
├── repository/           # 数据访问层 / Data access layer
│   └── MetricInfoRepository.java # 指标仓库 / Metric repository
└── service/              # 业务逻辑 / Business logic
    ├── CsvDataExporter.java # CSV导出器 / CSV exporter
    ├── DataFetcherService.java # 数据获取服务 / Data fetcher service
    ├── DataFilteringService.java # 数据过滤服务 / Data filtering service
    ├── MetricService.java # 指标服务 / Metric service
    └── parser/          # 数据解析器 / Data parsers
        ├── CsvDataParser.java # CSV解析器 / CSV parser
        ├── DataParser.java   # 解析器接口 / Parser interface
        ├── DataTypeMapper.java # 数据类型映射 / Data type mapper
        └── JsonDataParser.java # JSON解析器 / JSON parser
```

## 📝 变更日志 / Changelog

### [2025-06-24] - 初始版本 / Initial Release
- **修复 / Fixed**: 通过添加JVM参数解决API端点的500错误 / Resolved 500 error on API endpoints by adding JVM argument
- **修复 / Fixed**: 清理重复的应用程序属性解决启动失败 / Fixed application startup by cleaning up duplicate properties
- **新增 / Added**: 添加Spring Boot Actuator支持 / Added Spring Boot Actuator support
- **改进 / Improved**: 增强日志配置 / Enhanced logging configuration
- **新增 / Added**: 初始化Git仓库 / Initialized Git repository

### [2025-06-23] - Bug修复和测试改进 / Bug Fixes and Test Improvements
- **修复JSON解析器时间戳处理 / Fixed JSON Parser Timestamp Handling**
  - 在`JsonDataParser`中添加对`TIMESTAMP`数据类型的支持 / Added support for `TIMESTAMP` data type
  - 修复JSON数据中时间戳字段的空值处理 / Fixed null handling for timestamp fields
  - 更新测试数据以正确表示空时间戳值 / Updated test data for null timestamp values

- **测试套件改进 / Test Suite Improvements**
  - 修复测试数据中的字段名称不匹配 / Fixed field name mismatches in test data
  - 解决时间戳向量的`ClassCastException` / Resolved `ClassCastException` for timestamp vectors
  - 确保`JsonDataParserTest`中的所有测试通过 / Ensured all tests in `JsonDataParserTest` pass

## 📄 许可证 / License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🤝 贡献 / Contributing

欢迎提交问题和拉取请求！请确保您的代码符合项目编码标准，并包含适当的测试。

Issues and pull requests are welcome! Please ensure your code follows the project's coding standards and includes appropriate tests.

### 获取指标数据

-   **URL**: `/api/metrics/{metricName}`
-   **方法**: `GET`
-   **返回格式**: `text/csv`
-   **描述**: 获取指定指标的数据，处理后以 CSV 文件形式返回。

#### 请求示例

你可以使用浏览器或 `curl` 等工具，通过预置的样本指标来测试服务：

```bash
curl http://localhost:8080/api/metrics/sample_json_metric
```

#### 响应示例

响应将是一个 CSV 格式的字符串:

```csv
stock_code,event_time,metric_field_1,metric_field_2
000001.SZ,1733414400000,value1,value2
300033.SZ,1733414400000,value1,value2
```

## 如何扩展

### 添加新指标

要从一个已支持的数据源类型（如另一个 JSON API）添加新指标：

1.  在 `METRIC_INFO` 表中插入一条新记录。你可以通过数据库客户端操作，或扩展 `DataInitializer` 来实现。
2.  提供以下信息:
    -   `name`: 指标的唯一名称 (例如, `my_new_metric`).
    -   `source_type`: `HTTP_JSON` 或 `HTTP_CSV`.
    -   `source_url`: 数据源的 URL.
    -   `data_path`: (仅用于 JSON) 指向数据数组的 JSONPath (例如, `/results/data`).
    -   `field_mappings`: 一个 JSON 字符串，用于映射源字段到目标字段名和类型 (例如, `{"id":"user_id:LONG", "val":"metric_value:DOUBLE"}`).

### 添加新数据源类型

要支持一种新的数据格式（如 XML）:

1.  在 `pom.xml` 中添加所需的解析库。
2.  在 `DataSourceType` 枚举中创建一个新值 (例如, `HTTP_XML`).
3.  创建一个实现 `DataParser` 接口的新解析器类 (例如, `XmlDataParser.java`).
4.  使用 `@Component("HTTP_XML")` 注解你的新解析器。注解中的字符串必须与新枚举值的 `name()` 匹配。
5.  实现 `parse` 方法，将 `InputStream` 转换为 `UnifiedDataTable`。

当 `MetricService` 遇到相应 `sourceType` 的 `MetricInfo` 记录时，它会自动发现并使用你的新解析器。

---

# Changelog

-   **2025-06-19**:
    -   Initialized project structure with Spring Boot.
    -   Implemented core logic for data fetching, parsing (JSON/CSV), and caching.
    -   Exposed data via a REST API (`/api/metrics/{metricName}`).
    -   Added `README.md` with English and Chinese documentation.
-   **2025-06-19**: Added server-side filtering capabilities to the API via a `filter` query parameter.
-   **2025-06-19**: Build and Stability Fixes
    -   **Dependency & Build Fixes**:
        -   Resolved Apache Arrow native library issues on Apple Silicon by adding the `maven-surefire-plugin` with the `--add-opens=java.base/java.nio=ALL-UNNAMED` JVM argument, ensuring tests run correctly on modern Java versions.
        -   Upgraded `byte-buddy` to version `1.14.17` to ensure compatibility with Java 23 and Mockito.
    -   **Code & Test Fixes**:
        -   Corrected an import for `DefaultHttpRequestRetryStrategy` in `HttpClientConfig.java`.
        -   Added a missing import for `java.util.stream.Collectors` in `MetricController.java`.
        -   Fixed a `NumberFormatException` in `CsvDataParser.java` by adding `try-catch` blocks to handle malformed numbers gracefully.
        -   Fixed a `ClassCastException` in `JsonDataParser.java` by using the correct `TimeStampMilliTZVector` for timestamp values.
        -   Resolved a `ClassCastException` in `JsonDataParserTest.java` by updating the test to expect the correct `TimeStampMilliTZVector` type.
        -   Fixed an `UnnecessaryStubbingException` in `MetricServiceTest.java` by using lenient Mockito settings to prevent test failures from unused stubs.
