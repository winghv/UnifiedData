# Unified Data Service

## 📝 Changelog

### [2025-06-24] - Initial Release
- **Fixed**: Resolved 500 Internal Server Error on API endpoints by adding JVM argument `--add-opens=java.base/java.nio=ALL-UNNAMED`
- **Fixed**: Resolved application startup failure by cleaning up duplicate properties in `application.properties`
- **Added**: Spring Boot Actuator with `/actuator/health` and `/actuator/metrics` endpoints
- **Improved**: Enhanced logging configuration for better debugging
- **Added**: Git repository initialization with comprehensive `.gitignore`

## Overview

The Unified Data Service is a high-performance Spring Boot application designed to fetch, parse, filter, and serve metric data from various sources. It leverages Apache Arrow for efficient in-memory columnar data processing, providing a fast and scalable solution for data unification.

The service exposes a simple REST API to retrieve data in CSV format, with powerful filtering capabilities.

## Tech Stack

- **Java 17** & **Spring Boot 3.2.5**
- **Apache Arrow 15.0.0**: For high-performance, zero-copy in-memory columnar data representation.
- **Apache Commons CSV**: For parsing CSV data streams.
- **Jackson**: For parsing JSON data and handling configurations.
- **Spring Cache (Caffeine)**: For in-memory caching of parsed data tables to improve performance.
- **Lombok**: To reduce boilerplate code.
- **H2 Database**: In-memory database for storing metric metadata.
- **Maven**: For dependency management and building the project.
- **JUnit 5 & Mockito**: For unit and integration testing.

## How to Build and Run

### Prerequisites
- JDK 17 or later
- Apache Maven

### Build

To build the project, run the following command from the root directory:

```bash
mvn clean install
```

This will compile the code, run all unit tests, and package the application into a single executable JAR file in the `target/` directory.

### Run

To run the application, use the following command:

```bash
java -jar target/unified-data-service-0.0.1-SNAPSHOT.jar
```

The service will start on port `8080` by default.

## API Documentation

The service provides a single primary endpoint for retrieving metric data.

### Get Metric Data

- **URL**: `/api/metrics/{metricName}`
- **Method**: `GET`
- **Produces**: `text/csv`

#### Parameters

- **`metricName`** (path variable): The name of the metric to retrieve (e.g., `user_signups`, `system_load`).
- **`filter`** (request parameter, optional): An expression to filter the data. The format is `columnName operator value`.
    - **Operators**: `==`, `!=`, `>`, `<`, `>=`, `<=`
    - **Value Types**: String values must be enclosed in single quotes (e.g., `'value'`). Numeric values do not require quotes.

#### Example Usage

**1. Get unfiltered data for the `user_signups` metric:**

```bash
curl http://localhost:8080/api/metrics/user_signups
```

**2. Get data for users with an age greater than 30:**

```bash
curl "http://localhost:8080/api/metrics/user_signups?filter=age > 30"
```

**3. Get data for users from the 'US' region:**

```bash
curl "http://localhost:8080/api/metrics/user_signups?filter=region == 'US'"
```

## Changelog

### Bug Fixes and Test Improvements (June 23, 2025)

- **Fixed JSON Parser Timestamp Handling**:
  - Added proper support for `TIMESTAMP` data type in `JsonDataParser`
  - Fixed null handling for timestamp fields in JSON data
  - Updated test data to correctly represent null timestamp values

- **Test Suite Improvements**:
  - Fixed field name mismatches in test data
  - Resolved `ClassCastException` for timestamp vectors
  - Ensured all tests in `JsonDataParserTest` pass successfully
  - Verified full test suite passes with 100% success rate

- **Code Quality**:
  - Improved error handling in data parsers
  - Added proper null checks and validations
  - Enhanced test coverage for edge cases

### Project Rewrite (June 2025)

- **Complete Project Restoration**: The entire application was rebuilt from scratch after an accidental data loss.
- **Core Architecture**: Re-established a robust, modular architecture with clear separation of concerns (fetching, parsing, filtering, caching, API).
- **High-Performance Data Handling**: Integrated Apache Arrow (`VectorSchemaRoot`) for efficient, zero-copy in-memory data management.
- **Unified Data Parsers**: Implemented extensible `DataParser` interface for JSON and CSV sources.
- **Robust Filtering**: Created a `DataFilteringService` with a regex-based engine to support complex filtering expressions.
- **Caching Layer**: Added a Caffeine-based caching layer via Spring Cache to significantly improve performance for repeated queries.
- **Comprehensive Unit Tests**: Wrote extensive unit tests for all core services (`MetricService`, `DataFilteringService`) and data parsers (`JsonDataParser`, `CsvDataParser`), achieving full test coverage for the business logic.

This project is a Java-based service designed to fetch, process, and unify data from various heterogeneous sources (initially HTTP endpoints providing JSON or CSV). It transforms the data into a consistent intermediate format (Apache Arrow Table) and provides it through a unified query interface.

---

# 统一数据服务

本项目是一个基于 Java 的数据服务，旨在从多种异构数据源（初期为提供 JSON 或 CSV 的 HTTP 接口）获取、处理并统一数据。它将不同来源的数据转换为一致的中间格式（Apache Arrow Table），并通过统一的查询接口提供服务。

## 核心优势

-   **配置驱动 (Configuration-Driven)**: 无需修改代码，仅通过在数据库中添加元数据定义，即可动态接入新的数据指标。
-   **高度可扩展 (Highly Extensible)**: 采用策略模式，通过实现 `DataParser` 接口，可以轻松地为系统添加新的数据格式解析能力（如 XML、Protobuf 等）。
-   **高性能 (High Performance)**: 结合使用高性能的列式内存格式（Apache Arrow）和本地被动缓存（Caffeine），确保高缓存命中率和毫秒级的查询响应。
-   **高容错 (High Fault Tolerance)**: 内置的 HTTP 客户端具备连接池和失败自动重试机制。服务关键路径拥有完善的日志和异常处理，确保系统稳定性。
-   **统一与标准化 (Unification & Standardization)**: 将杂乱的、多格式的外部数据源，统一清洗为标准化的中间数据结构，为上层应用（如未来的SQL查询引擎）提供一个干净、一致的数据视图。

## 技术架构

服务基于 Spring Boot 构建，并遵循模块化的架构设计：

-   **Controller (`MetricController`)**: 暴露 REST API 端点，作为服务的外部入口。
-   **Service (`MetricService`)**: 编排核心业务逻辑，包括数据获取、解析和缓存。
-   **Repository (`MetricInfoRepository`)**: 使用 Spring Data JPA 管理指标元数据的持久化。
-   **Data Fetcher (`DataFetcherService`)**: 封装从 HTTP 源获取原始数据的逻辑。
-   **Parsers (`DataParser` 接口及其实现)**: 用于解析不同数据格式（JSON, CSV）的策略模式实现。
-   **Model**: 包含 JPA 实体 (`MetricInfo`)、数据枚举和 Apache Arrow 表的包装类 `UnifiedDataTable`。
-   **Configuration**: 包含 Spring Boot 配置、`HttpClient` 设置以及用于植入样本数据的 `DataInitializer`。

## 技术选型

-   **框架**: Spring Boot 3
-   **语言**: Java 17
-   **构建工具**: Maven
-   **HTTP 客户端**: Apache HttpClient 5
-   **数据格式**: Jackson (JSON), Apache Commons CSV
-   **中间格式**: Apache Arrow
-   **缓存**: Spring Cache with Caffeine
-   **数据库**: H2 (内存数据库, 用于演示), Spring Data JPA
-   **监控**: Spring Boot Actuator, Micrometer (Prometheus)
-   **测试**: JUnit 5, Mockito

## 快速开始

### 环境要求

-   Java 17 或更高版本
-   Apache Maven

### 运行应用

1.  克隆本仓库。
2.  进入项目根目录。
3.  使用 Spring Boot Maven 插件运行应用:

    ```bash
    mvn spring-boot:run
    ```

服务将启动在 `http://localhost:8080`。

### Filtering Results / 过滤结果

The service supports server-side filtering via the `filter` query parameter. The filter expression is a simple string with the format `column_name operator value`.

-   **Supported Operators**: `==`, `!=`, `>`, `<`, `>=`, `<=`
-   **Value Types**: String values must be enclosed in single quotes (e.g., `'some value'`), while numeric values should not be.

**Example:**

```bash
# Get data for 'sample_json_metric' where the 'name' field is 'user_a'
curl "http://localhost:8080/api/metrics/sample_json_metric?filter=name=='user_a'"
```

## API 使用说明

服务主要通过一个端点来获取指标数据。

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
