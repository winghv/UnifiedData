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

- **SQL 查询接口 / SQL Query Interface**
  - 支持标准 SQL 语法查询逻辑表数据
  - Supports standard SQL syntax for querying logical tables
  - 自动将 SQL 查询转换为底层指标查询计划
  - Automatically translates SQL queries to underlying metric query plans

- **逻辑表抽象 / Logical Table Abstraction**
  - 将多个指标组织为逻辑表，支持表连接和复杂查询
  - Organizes multiple metrics into logical tables with support for joins and complex queries
  - 支持按时间和代码分区，优化查询性能
  - Supports partitioning by time and code for optimized query performance

- **高性能 / High Performance**
  - 使用 Apache Arrow 进行高效的内存列式数据处理
  - Efficient in-memory columnar data processing with Apache Arrow
  - 基于 Caffeine 的缓存层，提高重复查询性能
  - Caffeine-based caching layer for improved performance on repeated queries
  - 流式处理大型结果集，降低内存占用
  - Streaming of large result sets to reduce memory footprint

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
  - JSqlParser (SQL 解析)

- **数据处理 / Data Processing**
  - Apache Arrow 15.0.0 (列式存储与处理 / Columnar storage and processing)
  - Jackson (JSON 处理 / JSON processing)
  - Apache Commons CSV (CSV 处理 / CSV processing)

- **数据库 / Database**
  - H2 (内存数据库 / In-memory)
  - Spring Data JPA

- **工具 / Tools**
  - Lombok
  - JUnit 5
  - Mockito
  - Spring Boot Actuator

## 🔍 API 文档 / API Documentation

### 1. 获取指标数据 / Get Metric Data

- **URL**: `GET /api/metrics/{metricName}`
- **Response**: `text/csv`

#### 参数 / Parameters

| 参数 / Parameter | 类型 / Type | 必填 / Required | 描述 / Description |
|----------------|------------|----------------|-------------------|
| metricName     | Path       | 是 / Yes       | 指标名称 / Metric name (e.g., `user_signups`, `system_load`) |
| filter         | Query      | 否 / No        | 过滤表达式 / Filter expression (e.g., `age > 30`, `region == 'US'`) |

### 2. 执行 SQL 查询 / Execute SQL Query

- **URL**: `GET /api/query`
- **Response**: `application/json` 或 `application/vnd.apache.arrow.stream`

#### 参数 / Parameters

| 参数 / Parameter | 类型 / Type | 必填 / Required | 描述 / Description |
|----------------|------------|----------------|-------------------|
| sql            | Query      | 是 / Yes       | SQL 查询语句 / SQL query statement |
| format         | Query      | 否 / No        | 响应格式: `json` 或 `arrow` (默认为 `arrow`) / Response format: `json` or `arrow` (default: `arrow`)

#### 操作符 / Operators
- 比较: `==`, `!=`, `>`, `<`, `>=`, `<=`
- 字符串值必须用单引号括起来 / String values must be enclosed in single quotes
- 数值不需要引号 / Numeric values should not be quoted

#### SQL 查询示例 / SQL Query Examples

```sql
-- 查询股票日线数据
SELECT * FROM stock_daily WHERE code = '000001.SZ' AND trade_date >= '2023-01-01';

-- 多表连接查询
SELECT a.code, a.trade_date, a.close, b.volume 
FROM stock_daily a 
JOIN stock_volume b ON a.code = b.code AND a.trade_date = b.trade_date
WHERE a.code IN ('000001.SZ', '600000.SH');

-- 聚合查询
SELECT code, AVG(close) as avg_price, SUM(volume) as total_volume
FROM stock_daily
WHERE trade_date BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY code
HAVING AVG(close) > 10.0;
```

#### 流式响应 / Streaming Response

对于大型结果集，API 支持流式响应（使用 Apache Arrow 格式）:

```bash
# 获取 Arrow 格式的流式响应
curl -H "Accept: application/vnd.apache.arrow.stream" "http://localhost:8080/api/query?sql=SELECT * FROM large_table" > results.arrow

# 使用 Python 读取流式响应
import pyarrow as pa
import requests

response = requests.get(
    'http://localhost:8080/api/query',
    params={'sql': 'SELECT * FROM large_table'},
    headers={'Accept': 'application/vnd.apache.arrow.stream'},
    stream=True
)

with pa.ipc.open_stream(response.raw) as reader:
    for batch in reader:
        df = batch.to_pandas()
        # 处理批次数据 / Process batch data
        print(df)
```

#### 示例 / Examples

```bash
# 获取未过滤的数据 / Get unfiltered data
curl http://localhost:8080/api/metrics/user_signups

# 获取年龄大于30的用户数据 / Get users older than 30
curl "http://localhost:8080/api/metrics/sample_json_metric?filter=age > 30"

# 获取美国地区用户数据 / Get users from US region
curl "http://localhost:8080/api/metrics/sample_json_metric?filter=region == 'US'"
```

## 项目结构 / Project Structure

```
src/main/java/com/example/unifieddataservice/
├── UnifiedDataApplication.java  # 应用入口 / Application entry point
├── config/             # 配置类 / Configuration classes
│   └── AppConfig.java  # 应用配置 / Application configuration
├── controller/         # Web 控制器 / Web controllers
│   ├── MetricController.java  # 指标API控制器 / Metric API controller
│   ├── QueryController.java   # SQL查询API控制器 / SQL Query API controller
│   └── UnifiedDataTable.java  # 统一数据表 / Unified data table
├── model/              # 数据模型 / Data models
│   ├── TableDefinition.java   # 逻辑表定义 / Logical table definition
│   └── MetricQueryPlan.java   # 查询计划 / Query plan
├── service/            # 业务逻辑 / Business logic
│   ├── SqlQueryService.java   # SQL查询服务 / SQL query service
│   └── TableRegistry.java     # 逻辑表注册中心 / Table registry
└── util/               # 工具类 / Utilities
    └── ArrowJoinUtil.java     # Arrow表连接工具 / Arrow table join utility
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

## 📜 更新日志 / Changelog

### [Unreleased]
#### Added
- 新增配置管理功能，支持配置项的增删改查
- 集成 Element Plus 实现现代化 UI 界面
- 添加股票市场相关配置项（数据源、监控股票列表、技术指标等）
- 添加配置项预览功能，支持 JSON 格式化显示

#### Changed
- 重构前端界面，使用 Element Plus 组件库
- 优化配置项表单验证和错误处理
- 改进配置项值的展示格式

#### Fixed
- 修复前端构建问题
- 修复后端 API 跨域配置

