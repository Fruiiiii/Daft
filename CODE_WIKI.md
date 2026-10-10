# Daft Code Wiki

> 自动生成的 Daft 项目代码维基文档，涵盖项目整体架构、主要模块职责、关键类与函数说明、依赖关系以及项目运行方式。

---

## 目录

- [1. 项目概览](#1-项目概览)
- [2. 整体架构](#2-整体架构)
- [3. Rust 工作区模块（src/）](#3-rust-工作区模块src)
- [4. Python 包模块（daft/）](#4-python-包模块daft)
- [5. 关键类与函数说明](#5-关键类与函数说明)
- [6. 多模态算子详解](#6-多模态算子详解)
- [7. 依赖关系](#7-依赖关系)
- [8. 项目运行方式](#8-项目运行方式)
- [9. 测试体系](#9-测试体系)

---

## 1. 项目概览

**Daft** 是一个面向 AI 与多模态工作负载的高性能数据引擎（Distributed Dataframes for Multimodal Data）。它能够处理图像、音频、视频、嵌入向量以及结构化数据，并支持从本地到分布式集群的无缝扩展。

### 核心特性

| 特性 | 说明 |
| --- | --- |
| 原生多模态处理 | 在单一框架中处理图像、音频、视频与嵌入向量 |
| 内建 AI 操作 | 支持 LLM 提示、嵌入生成、数据分类（OpenAI / Transformers / 自定义模型） |
| Python 原生、Rust 驱动 | Python 作为核心 API 层，Rust 提供底层高性能执行 |
| 无缝扩展 | 本地执行可平滑扩展到 Ray / Kubernetes 分布式集群 |
| 通用数据连接 | 支持 S3、GCS、Iceberg、Delta Lake、Hugging Face、Unity Catalog 等 |
| 开箱即用的可靠性 | 智能内存管理与合理默认值 |

### 基本信息

- **语言/构建**：Rust（edition 2024）+ Python（≥ 3.10），使用 `maturin` 构建为 `cp3xx` abi3 扩展模块
- **版本**：`0.3.0-dev0`
- **许可证**：Apache 2.0
- **入口**：[daft/__init__.py](file:///workspace/daft/__init__.py)（Python 公共 API）、[src/lib.rs](file:///workspace/src/lib.rs)（Rust PyModule 注册）
- **仓库**：https://github.com/Eventual-Inc/Daft
- **文档**：https://docs.daft.ai

---

## 2. 整体架构

Daft 采用**双层混合架构**：上层为 Python API 与编排逻辑，下层为 Rust 实现的高性能执行引擎，两者通过 [PyO3](https://pyo3.rs) 桥接。

```
┌─────────────────────────────────────────────────────────────────────┐
│                     用户层 (Python API)                              │
│   daft.read_parquet / daft.from_pydict / daft.sql / DataFrame API   │
├─────────────────────────────────────────────────────────────────────┤
│  Python 包 daft/                                                     │
│  ┌──────────┐ ┌───────────┐ ┌────────┐ ┌────────┐ ┌──────────────┐ │
│  │dataframe │ │expressions│ │ series │ │ schema │ │ udf / window │ │
│  └────┬─────┘ └─────┬─────┘ └───┬────┘ └───┬────┘ └──────┬───────┘ │
│       │  logical/builder  context/session  runners  io  sql  catalog │
├───────┴───────────────┴───────────────┴────────┴──────────┴────────┤
│              PyO3 桥接层 (daft.daft native module)                   │
├─────────────────────────────────────────────────────────────────────┤
│  Rust 工作区 src/                                                     │
│  ┌──────────┐ ┌──────────┐ ┌─────────────┐ ┌──────────────────────┐  │
│  │daft-core │ │daft-dsl  │ │daft-logical │ │daft-local-execution  │  │
│  │(arrays/  │ │(exprs)   │ │-plan        │ │(pipeline engine)     │  │
│  │ kernels) │ └────┬─────┘ └──────┬──────┘ └──────────┬───────────┘  │
│  └────┬─────┘      │              │                    │            │
│       │     ┌──────┴────────┐    │           ┌────────┴──────────┐ │
│       │     │daft-local-plan│    │           │daft-distributed    │ │
│       │     │(phys ops)     │    │           │(Ray/flight shuffle) │ │
│       │     └──────────────┘    │           └─────────────────────┘ │
│       └──── daft-scan ─── daft-io ─ daft-parquet/csv/json/avro ───┘  │
│              daft-micropartition / daft-recordbatch / daft-functions*│
└─────────────────────────────────────────────────────────────────────┘
```

### 执行流水线

一个查询从用户 API 到结果产出的典型流程：

1. **DataFrame 构建**：用户调用 `daft.read_parquet(...)` / `.select()` / `.where()` 等，构建惰性的 `LogicalPlanBuilder`（逻辑计划）。
2. **逻辑优化**：触发执行（`.collect()` / `.show()`）时，逻辑计划经过 `optimize()` 进行谓词下推、投影裁剪等优化。
3. **物理计划转换**：逻辑计划被翻译为 `LocalPhysicalPlan`（本地）或 `DistributedPhysicalPlan`（分布式）。
4. **执行**：
   - **Native Runner**：[daft-local-execution](file:///workspace/src/daft-local-execution) 流水线引擎在本地多线程执行，产出 `MicroPartition` 流。
   - **Ray Runner**：[daft-distributed](file:///workspace/src/daft-distributed) 将算子调度到 Ray 集群，通过 Flight Shuffle 交换分区。
5. **物化与缓存**：结果以 `PartitionSet` 形式缓存到 `PartitionCacheEntry`，供 `.collect()`、迭代或写回使用。

---

## 3. Rust 工作区模块（src/）

Rust 工作区在 [Cargo.toml](file:///workspace/Cargo.toml) 中定义，包含 50+ 个 crate，分为公共共享库、核心引擎、执行层、IO/格式、函数库、AI 与工具库几大类。根 crate `daft`（`src/lib.rs`）编译为 `cdylib`，通过 PyO3 的 `#[pymodule] fn daft` 注册所有子模块到 Python。

### 3.1 共享公共库（src/common/）

| Crate | 职责 |
| --- | --- |
| `common-daft-config` | 全局配置（execution/planning/event-log config）的 Python 绑定 |
| `common-display` | 通用表格/格式化显示 |
| `common-file-formats` | 文件格式枚举与公共逻辑 |
| `common-error` | 统一错误类型 |
| `common-io-config` | 各云存储（S3/Azure/GCS/HDFS/HTTP 等）IOConfig |
| `common-metrics` | 执行指标统计 |
| `common-partitioning` | 分区抽象 |
| `common-resource-request` | 资源请求（CPU/GPU/内存） |
| `common-runtime` | tokio 运行时管理（计算线程池） |
| `common-system-info` | 系统信息采集 |
| `common-tracing` | OpenTelemetry tracing 初始化 |
| `common-logging` | 全局日志器（pyo3-log 桥接 Python logging） |
| `common-version` | 版本与构建类型 |
| `common-treenode` / `common-macros` / `common-pattern` | 通用工具 |

### 3.2 核心数据与表达式

| Crate | 职责 | 关键导出 |
| --- | --- | --- |
| [daft-core](file:///workspace/src/daft-core) | 核心数据结构：数组、Series、数据类型、计算内核 | `Series`、各种 `ArrayType`、Python 绑定 |
| [daft-schema](file:///workspace/src/daft-schema) | Schema、Field、数据类型定义 | `Schema`、`Field`、`DataType` |
| [daft-dsl](file:///workspace/src/daft-dsl) | 表达式 DSL（Expr）、字面量、列引用、UDF 表达式、函数注册表 | `Expr`、`PyExpr`、`FUNCTION_REGISTRY` |

### 3.3 计划与执行

| Crate | 职责 | 关键导出 |
| --- | --- | --- |
| [daft-logical-plan](file:///workspace/src/daft-logical-plan) | 逻辑计划构造与查询（Scan/Filter/Project/Join/Agg 等） | `LogicalPlan`、`LogicalPlanBuilder`、source/sink info |
| [daft-algebra](file:///workspace/src/daft-algebra) | 计划代数与优化规则 | 优化 pass |
| [daft-local-plan](file:///workspace/src/daft-local-plan) | 本地物理算子（Filter/Project/HashJoin/GlobScan/PhysicalScan） | `LocalPhysicalPlan` 及算子节点 |
| [daft-local-execution](file:///workspace/src/daft-local-execution) | 本地流水线执行引擎 | 执行运行时、pipeline 调度、`NativeExecutor` |
| [daft-distributed](file:///workspace/src/daft-distributed) | 分布式执行（pipeline 节点、调度、统计、Flight Shuffle） | 分布式 plan 执行 |
| [daft-runners](file:///workspace/src/daft-runners) | Runner 抽象与原生/Ray 配置 | `Runner` Python 注册 |

### 3.4 数据扫描与分区

| Crate | 职责 |
| --- | --- |
| [daft-scan](file:///workspace/src/daft-scan) | `ScanOperator` trait、`ScanTask`、`GlobScanOperator`、`AnonymousScanOperator`、`Pushdowns`、`PartitionField`、分区裁剪 |
| [daft-micropartition](file:///workspace/src/daft-micropartition) | `MicroPartition`：执行与分区交换的基本单位 |
| [daft-recordbatch](file:///workspace/src/daft-recordbatch) | `RecordBatch`：列数据访问封装 |
| [daft-shuffles](file:///workspace/src/daft-shuffles) | Shuffle 算法实现 |
| [daft-partition-refs](file:///workspace/src/daft-partition-refs) | 分区引用管理 |

### 3.5 IO 与文件格式

| Crate | 职责 |
| --- | --- |
| [daft-io](file:///workspace/src/daft-io) | 统一 IO：云存储/对象存储/HTTP/本地 FS/重试/范围读，Python IOConfig 注册 |
| [daft-parquet](file:///workspace/src/daft-parquet) | Parquet 读写（基于 arrow/parquet） |
| [daft-csv](file:///workspace/src/daft-csv) | CSV 读取 |
| [daft-json](file:///workspace/src/daft-json) | JSON 读取 |
| [daft-avro](file:///workspace/src/daft-avro) | Avro 读写 |
| [daft-text](file:///workspace/src/daft-text) | 纯文本读取 |
| [daft-warc](file:///workspace/src/daft-warc) | WARC（Web Archive）读取 |
| [daft-mcap](file:///workspace/src/daft-mcap) | MCAP（机器人日志）读取 |
| [daft-file](file:///workspace/src/daft-file) | 多模态文件类型（Image/Video/Audio/HDF5 File）与 `File` 函数 |
| [daft-image](file:///workspace/src/daft-image) | 图像处理函数与内核 |
| [daft-writers](file:///workspace/src/daft-writers) | 写出器抽象 |

### 3.6 函数库

通过 `daft-dsl` 的 `FUNCTION_REGISTRY` 在 [src/lib.rs](file:///workspace/src/lib.rs) 的模块初始化时统一注册：

| Crate | 函数类别 |
| --- | --- |
| [daft-functions](file:///workspace/src/daft-functions) | 数值/浮点/距离/相似度/随机/coalesce/monotonically_increasing_id 等 |
| [daft-functions-utf8](file:///workspace/src/daft-functions-utf8) | UTF8 字符串函数 |
| [daft-functions-list](file:///workspace/src/daft-functions-list) | 列表函数 |
| [daft-functions-json](file:///workspace/src/daft-functions-json) | JSON 函数 |
| [daft-functions-binary](file:///workspace/src/daft-functions-binary) | 二进制函数 |
| [daft-functions-temporal](file:///workspace/src/daft-functions-temporal) | 时间函数 |
| [daft-functions-tokenize](file:///workspace/src/daft-functions-tokenize) | 分词函数 |
| [daft-functions-uri](file:///workspace/src/daft-functions-uri) | URI 函数 |
| [daft-functions-serde](file:///workspace/src/daft-functions-serde) | 序列化函数 |
| [daft-geo](file:///workspace/src/daft-geo) | 空间/地理函数 |
| [daft-hash](file:///workspace/src/daft-hash) / [daft-minhash](file:///workspace/src/daft-minhash) / [hyperloglog](file:///workspace/src/hyperloglog) | 哈希、MinHash、HyperLogLog 概略算法 |
| [daft-sketch](file:///workspace/src/daft-sketch) / [daft-stats](file:///workspace/src/daft-stats) | 草图与统计 |

### 3.7 SQL、Catalog、Session、AI

| Crate | 职责 |
| --- | --- |
| [daft-sql](file:///workspace/src/daft-sql) | SQL 解析（sqlparser）与执行，`sql_exec` / `sql_expr` |
| [daft-catalog](file:///workspace/src/daft-catalog) | Catalog 抽象（Identifier、Table、Catalog trait）与 Python 绑定 |
| [daft-session](file:///workspace/src/daft-session) | 会话状态（PySession、表源、函数注册） |
| [daft-context](file:///workspace/src/daft-context) | 全局上下文 PyDaftContext、订阅者通知 |
| [daft-checkpoint](file:///workspace/src/daft-checkpoint) | 检查点存储与幂等提交（支持 S3） |
| [daft-ai](file:///workspace/src/daft-ai) | AI 操作内核（provider 协议、表达式） |
| [daft-cli](file:///workspace/src/daft-cli) | `daft` CLI 命令行入口 |

### 3.8 Dashboard 与扩展

| Crate | 职责 |
| --- | --- |
| [daft-dashboard](file:///workspace/src/daft-dashboard) | Axum 后端 + Next.js 前端（静态嵌入二进制），SSE 实时查询监控。`DashboardState`（DashMap 内存态） |
| [daft-ext](file:///workspace/src/daft-ext) / [daft-ext-internal](file:///workspace/src/daft-ext-internal) / [daft-ext-macros](file:///workspace/src/daft-ext-macros) | 扩展 SDK，用于构建自定义 Daft 扩展插件 |

---

## 4. Python 包模块（daft/）

Python 包 [daft/](file:///workspace/daft) 是用户直接交互的 API 层。它通过 `from daft.daft import ...` 引用由 Rust 编译出的原生模块（`daft.daft`，即 abi3 `.so`）。所有顶层公共 API 在 [daft/__init__.py](file:///workspace/daft/__init__.py) 中聚合导出。

### 4.1 核心数据 API

| 模块 | 职责 | 关键类/函数 |
| --- | --- | --- |
| [daft/dataframe/](file:///workspace/daft/dataframe) | DataFrame 惰性表 API，所有变换构建逻辑计划，执行时物化 | `DataFrame`、`GroupedDataFrame` |
| [daft/expressions/](file:///workspace/daft/expressions) | 表达式 DSL，列引用/字面量/UDF 表达式 | `Expression`、`ExpressionsProjection`、`col()`、`lit()`、`element()`、`interval()` |
| [daft/series.py](file:///workspace/daft/series.py) | 单列数据抽象，包装原生 Series | `Series` |
| [daft/schema.py](file:///workspace/daft/schema.py) | Schema 定义 | `Schema` |
| [daft/datatype.py](file:///workspace/daft/datatype.py) | 数据类型、媒体类型、时间单位 | `DataType`、`MediaType`、`TimeUnit`、`UnionMode` |
| [daft/convert.py](file:///workspace/daft/convert.py) | 从 Python 对象构造 DataFrame | `from_pylist`、`from_pydict`、`from_arrow`、`from_pandas`、`from_dask_dataframe`、`from_ray_dataset`、`concat` |

### 4.2 计划与执行编排

| 模块 | 职责 |
| --- | --- |
| [daft/logical/](file:///workspace/daft/logical) | `LogicalPlanBuilder` 包装原生逻辑计划构建器，提供数据源入口（`from_in_memory_scan`/`from_tabular_scan`/`from_glob_scan`）及变换方法（select/with_columns/filter/sort/join/agg 等） |
| [daft/execution/](file:///workspace/daft/execution) | 执行编排：`NativeExecutor`（本地执行器，流式产出 LocalMaterializedResult）、Ray UDF actor 池、分布式 limit、key-filtering join |
| [daft/runners/](file:///workspace/daft/runners) | Runner 抽象与实现：`Runner`（抽象基类）、native runner、ray runner、flotilla、分区缓存、进度条、profiler |
| [daft/recordbatch/](file:///workspace/daft/recordbatch) | `MicroPartition`（分区执行/交换基本单位）、`RecordBatch`（列访问封装）、partitioning |
| [daft/context.py](file:///workspace/daft/context.py) | `DaftContext` 全局执行上下文：execution/planning/event-log 配置、订阅者 attach/detach |

### 4.3 IO 与数据源

[daft/io/](file:///workspace/daft/io) 提供丰富的读取与写入 API：

| 子模块 | 函数/职责 |
| --- | --- |
| `_parquet.py` / `_csv.py` / `_json.py` / `_avro.py` / `_orc.py` / `_text.py` / `_warc.py` / `_mcap.py` | 基础格式读取：`read_parquet`/`read_csv`/`read_json`/`read_avro`/`read_orc`/`read_text`/`read_warc`/`read_mcap` |
| `_sql.py` | `read_sql`（PostgreSQL/MySQL/Trino 等） |
| `_kafka.py` / `_blob.py` | Kafka 流读取、Blob 存储 |
| `_files.py` / `file_path.py` | `from_files`、`from_glob_path` |
| `iceberg/` / `delta_lake/` / `hudi/` / `paimon/` / `lance/` | 表格式读取与写入 |
| `huggingface/` / `webdataset/` / `av/` / `bigtable/` / `clickhouse/` / `turbopuffer/` | 数据源/汇集成 |
| `source.py` / `sink.py` / `writer.py` / `scan.py` / `pushdowns.py` / `partitioning.py` | 扫描/写入抽象、下推、分区 |
| `aws_config.py` / `object_store_options.py` / `gravitino_filesystem.py` | 云存储配置 |

### 4.4 SQL、Catalog、Session

| 模块 | 职责 |
| --- | --- |
| [daft/sql/](file:///workspace/daft/sql) | `sql()` 执行 SQL 查询返回 DataFrame；`sql_expr()` 将 SQL 片段解析为 Expression；`sql_connection.py`/`sql_scan.py` 数据库连接与扫描 |
| [daft/catalog/](file:///workspace/daft/catalog) | Catalog 抽象：`Catalog`、`Identifier`、`Table`；后端实现 `__iceberg.py`、`__glue.py`、`__unity`、`__gravitino`、`__s3tables.py`、`__paimon.py`、`__postgres.py` |
| [daft/session.py](file:///workspace/daft/session.py) | `Session` 会话状态管理：catalog/provider/function/table 的 attach/detach、create/drop、`read_table`/`write_table`、`current_session`（基于 ContextVar） |

### 4.5 UDF、窗口、文件、AI

| 模块 | 职责 |
| --- | --- |
| [daft/udf/](file:///workspace/udf) | UDF 体系：`udf`/`udaf`/`func`/`cls`/`method`/`metrics` 装饰器；`Func` 数据类（返回类型/并发/批处理/资源/重试/错误处理）；UDAF 聚合；执行与指标 |
| [daft/window.py](file:///workspace/daft/window.py) | `Window` 窗口函数定义（partition by / order by / rows between） |
| [daft/file/](file:///workspace/daft/file) | 多模态文件：`File`、`ImageFile`、`VideoFile`、`AudioFile`、`Hdf5File`、`open_file` |
| [daft/ai/](file:///workspace/daft/ai) | AI provider 框架：`Provider`、`PROVIDERS`、`load_provider`；OpenAI/Google/Transformers/vLLM/LM Studio 后端；protocols（prompter/text_embedder/image_classifier 等） |
| [daft/functions/](file:///workspace/daft/functions) | Python 层函数封装（numeric/str/list/datetime/image/audio/video/url 等），对接 Rust 注册函数 |

### 4.6 可观测性与扩展

| 模块 | 职责 |
| --- | --- |
| [daft/subscribers/](file:///workspace/daft/subscribers) | 事件订阅：`Subscriber`（ABC，基于 singledispatch 分发 Event）；事件类型（QueryStarted/Finished、Optimization、Execution、Operator、Stats）；`dashboard` 启动器、`event_log` 日志 |
| [daft/viz/](file:///workspace/daft/viz) | 可视化钩子 `register_viz_hook`、HTML 渲染 |
| [daft/checkpoint.py](file:///workspace/daft/checkpoint.py) | 检查点：`CheckpointConfig`、`CheckpointStore`、`IdempotentCommit`、`KeyFilteringSettings` |
| [daft/cli.py](file:///workspace/daft/cli.py) | `daft` CLI 入口 `main` |
| [daft/extension_type.py](file:///workspace/daft/extension_type.py) / [daft/lazy_import.py](file:///workspace/daft/lazy_import.py) | 扩展类型、惰性导入（保持 `import daft` 快速） |

---

## 5. 关键类与函数说明

### 5.1 DataFrame（核心惰性表）

定义于 [daft/dataframe/dataframe.py](file:///workspace/daft/dataframe/dataframe.py#L181)。DataFrame 包装一个 `LogicalPlanBuilder`，所有变换方法返回新的 DataFrame（惰性），仅在 `collect()`/`show()` 时触发执行。

**构造**：`DataFrame(builder: LogicalPlanBuilder)`，用户应使用 `daft.from_*` / `daft.read_*` 工厂函数，而非直接构造。

**主要方法分组**：

| 类别 | 方法 |
| --- | --- |
| 数据源/变换 | `select`、`where`、`with_column`、`with_column_renamed`、`with_columns`、`sort`、`limit`、`distinct`、`repartition`、`explode`、`sample`、`pipe` |
| 连接/集合 | `join`、`union`、`intersect`、`except_` |
| 聚合 | `sum`、`mean`、`min`、`max`、`count`、`agg`、`groupby`（返回 GroupedDataFrame）、`pivot`、`describe`、`summarize` |
| 执行/物化 | `collect`（触发执行并缓存）、`show`、`iter_rows`、`to_*`（to_arrow/pandas/pydict/torch 等） |
| 写出 | `write_parquet`、`write_csv`、`write_json`、`write_deltalake` 等 |
| 调试/元数据 | `explain`、`schema`、`column_names`、`count_rows`、`metrics`、`skipped_corrupt_files` |

### 5.2 Expression（表达式 DSL）

定义于 [daft/expressions/expressions.py](file:///workspace/daft/expressions/expressions.py#L138)。包装原生 `_PyExpr`，表示列引用、字面量、函数调用或 UDF 表达式。

- `col(name)` → 列引用表达式
- `lit(value)` → 字面量表达式
- `element()` → 列表逐元素操作引用
- `interval(...)` → 时间间隔字面量
- `Expression.udf(...)` → 构造 UDF 表达式
- 运算符重载（`+ - * / == > <` 等）与大量方法链（`.sum()`、`.count()`、`.cast()`、`.if_else()` 等）

### 5.3 LogicalPlanBuilder（逻辑计划构建器）

定义于 [daft/logical/builder.py](file:///workspace/daft/logical/builder.py#L55)。包装 Rust `LogicalPlanBuilder`，是 DataFrame 的底层引擎。

- 数据源入口：`from_in_memory_scan`、`from_tabular_scan`（接收 `ScanOperatorHandle`）、`from_glob_scan`
- 变换方法：`select`、`with_columns`、`with_column_renamed`、`filter`、`sort`、`limit`、`join`、`agg`、`distinct`、`repartition`、`explode`、`pivot`、`union` 等
- 查询/优化：`schema()`、`optimize(execution_config)`、`pretty_print`、`repr_json`
- 装饰器 `_apply_daft_planning_config_to_initializer` 自动注入当前 planning config

### 5.4 执行与 Runner

- **`Runner`**（[daft/runners/runner.py](file:///workspace/daft/runners/runner.py#L26)）：抽象基类，定义 `run`、`run_iter`、`run_iter_tables`、分区缓存管理。子类 `name` 为 `"native"` 或 `"ray"`。
- **`NativeExecutor`**（[daft/execution/native_executor.py](file:///workspace/daft/execution/native_executor.py#L28)）：本地执行器，`run()` 接收 `LocalPhysicalPlan`，通过 event loop 异步流式产出 `LocalMaterializedResult`，返回执行统计。
- **Runner 切换**：`daft.set_runner_native()` / `daft.set_runner_ray()`（或环境变量 `DAFT_RUNNER=native|ray`）。`get_or_create_runner()` 首次调用后锁定 runner。

### 5.5 MicroPartition 与 RecordBatch

- **`MicroPartition`**（[daft/recordbatch/micropartition.py](file:///workspace/daft/recordbatch/micropartition.py)）：执行与分区交换的基本单位，提供 schema、列访问、record batch 转换。
- **`RecordBatch`**（[daft/recordbatch/recordbatch.py](file:///workspace/daft/recordbatch/recordbatch.py#L38)）：列数据访问封装，`schema()`、`get_column()`、`columns()`。

### 5.6 上下文、会话、Catalog

- **`DaftContext`**（[daft/context.py](file:///workspace/daft/context.py#L30)）：全局单例，持有 execution/planning/event-log config，管理订阅者，分发查询生命周期事件。通过 `get_context()` 获取。
- **`Session`**（[daft/session.py](file:///workspace/daft/session.py#L86)）：会话状态，基于 `ContextVar` 实现线程安全的当前会话。管理 catalog/provider/function/table 的注册与解析，编排 SQL 与 DataFrame 查询。
- **`Catalog` / `Identifier` / `Table`**（[daft/catalog/__init__.py](file:///workspace/daft/catalog/__init__.py)）：Catalog 抽象，支持多后端（Iceberg/Glue/Unity/Gravitino/S3Tables/Paimon/Postgres）。

### 5.7 Subscriber（可观测性）

定义于 [daft/subscribers/abc.py](file:///workspace/daft/subscribers/abc.py#L28)。基于 `singledispatchmethod` 分发事件：`QueryStarted`、`QueryHeartbeat`、`QueryFinished`、`OptimizationStarted/Completed`、`ExecutionStarted/Finished`、`OperatorStarted/Finished`、`ProcessStats`、`Stats`。通过 `daft.with_subscriber(alias, subscriber)` 上下文管理器临时注册。

### 5.8 UDF 体系

[daft/udf/](file:///workspace/daft/udf) 提供装饰器式 UDF/UDAF 定义：

- `@daft.udf(...)` / `@daft.udaf(...)`：定义标量/聚合 UDF
- `@daft.func(...)`：函数式 UDF（新 V2 API），`Func` 数据类保存返回类型、并发、批处理、资源、重试、错误处理等元信息
- `@daft.cls(...)` / `@daft.method(...)`：类式 UDF
- `daft.metrics`：UDF 指标

### 5.9 顶层工厂函数（daft.\*）

| 函数 | 说明 |
| --- | --- |
| `from_pylist` / `from_pydict` / `from_arrow` / `from_pandas` / `from_dask_dataframe` / `from_ray_dataset` | 从内存数据构造 DataFrame |
| `concat` | 拼接多个 DataFrame |
| `read_parquet` / `read_csv` / `read_json` / `read_avro` / `read_orc` / `read_text` / `read_sql` / `read_iceberg` / `read_deltalake` / `read_hudi` / `read_paimon` / `read_lance` / `read_kafka` / `read_warc` / `read_mcap` / `read_blob` / `read_huggingface` / `read_webdataset` / `read_video_frames` / `from_files` / `from_glob_path` | 数据源读取 |
| `sql` / `sql_expr` | SQL 查询与表达式 |
| `col` / `lit` / `element` / `interval` | 表达式构造 |
| `set_runner_native` / `set_runner_ray` / `get_or_create_runner` | Runner 配置 |
| `set_execution_config` / `set_planning_config` / `execution_config_ctx` / `planning_config_ctx` / `set_event_log_config` / `event_log_ctx` | 配置 |
| `attach` / `attach_catalog` / `attach_table` / `create_table` / `read_table` / `write_table` / `list_tables` / ... | Session/Catalog 操作 |

---

## 6. 多模态算子详解

Daft 的多模态算子横跨 **Rust 内核实现** 与 **Python 函数封装** 两层。用户通过 `daft.functions.*` 调用的算子最终经由 [src/lib.rs](file:///workspace/src/lib.rs) 的 `FUNCTION_REGISTRY` 注册到 Rust 内核执行。下文按模态逐一说明详细位置。

### 6.1 图像算子

#### Rust 内核 — [src/daft-image/src/functions/](file:///workspace/src/daft-image/src/functions)

图像处理内核实现，通过 [daft-image/src/lib.rs](file:///workspace/src/daft-image/src/lib.rs) 暴露并注册为 `ImageFunctions`：

| 算子 | 源文件 |
| --- | --- |
| `decode`（图像字节 → Image 类型） | [decode.rs](file:///workspace/src/daft-image/src/functions/decode.rs) |
| `encode`（Image 类型 → PNG/JPEG/... 字节） | [encode.rs](file:///workspace/src/daft-image/src/functions/encode.rs) |
| `resize`（调整尺寸） | [resize.rs](file:///workspace/src/daft-image/src/functions/resize.rs) |
| `crop`（裁剪） | [crop.rs](file:///workspace/src/daft-image/src/functions/crop.rs) |
| `to_mode`（色彩模式转换，如 RGB↔RGBA） | [to_mode.rs](file:///workspace/src/daft-image/src/functions/to_mode.rs) |
| `to_tensor`（Image → Tensor） | [to_tensor.rs](file:///workspace/src/daft-image/src/functions/to_tensor.rs) |
| `image_hash`（感知哈希，用于近重复检测） | [image_hash.rs](file:///workspace/src/daft-image/src/functions/image_hash.rs) |
| `attribute`（宽度/高度/通道/模式属性） | [attribute.rs](file:///workspace/src/daft-image/src/functions/attribute.rs) |
| `decode_image_file`（从 ImageFile 解码帧） | [decode_image_file.rs](file:///workspace/src/daft-image/src/functions/decode_image_file.rs) |
| `image_file_metadata`（ImageFile 元数据提取） | [image_file_metadata.rs](file:///workspace/src/daft-image/src/functions/image_file_metadata.rs) |

其他相关内核文件：[ops.rs](file:///workspace/src/daft-image/src/ops.rs)（通用算子调度）、[series.rs](file:///workspace/src/daft-image/src/series.rs)（Image Series 实现）、[counting_writer.rs](file:///workspace/src/daft-image/src/counting_writer.rs)、[iters.rs](file:///workspace/src/daft-image/src/iters.rs)。

#### Python 封装

| 模块 | 导出函数 |
| --- | --- |
| [daft/functions/image.py](file:///workspace/daft/functions/image.py) | `resize`、`crop`、`encode_image`、`decode_image`、`convert_image`、`image_to_tensor`、`image_attribute`、`image_width`、`image_height`、`image_channel`、`image_mode`、`image_hash` |
| [daft/functions/image_file_.py](file:///workspace/daft/functions/image_file_.py) | `image_file_metadata`、`decode_image_file` |

#### 调用示例

```python
import daft
from daft.functions import decode_image, encode_image, image_hash, decode_image_file

# 1) 全局管道：发现文件 → 下载字节 → 解码 → 预处理 → 哈希
df = daft.from_glob_path("hf://datasets/datasets-examples/doc-image-3/images")
df = (
    df.with_column("image_bytes", daft.col("path").download())
      # 字节 → Image 类型（出错时返回 null 而不是中断）
      .with_column("image", decode_image(daft.col("image_bytes"), on_error="null"))
      # 色彩模式转换（链式方法等价于 convert_image / resize）
      .with_column("image_resized", daft.col("image").convert_image("RGB").resize(288, 288))
      # 感知哈希，用于近重复检测（返回 FixedSizeBinary）
      .with_column("hash", image_hash(daft.col("image_resized"), method="phash"))
)

# 2) 编码回字节（PNG/JPEG），可写回对象存储
df = df.with_column("png_bytes", encode_image(daft.col("image_resized"), "png"))

# 3) 基于 ImageFile 的惰性解码与元数据
from daft.functions import image_file, image_file_metadata

df = (
    daft.from_glob_path("s3://bucket/images/*")
    .with_column("file", image_file(daft.col("path")))
    .with_column("meta", image_file_metadata(daft.col("file")))   # width/height/format/mode
    .with_column("image", decode_image_file(daft.col("file"), mode="RGB"))
)
```

#### 参数说明

| 函数 | 签名 | 参数说明 |
| --- | --- | --- |
| `decode_image` | `(bytes, on_error="raise", mode=ImageMode.RGB)` | `bytes`：二进制表达式（PNG/JPEG 等编码字节）；`on_error`：解码失败时 `"raise"` 抛错 / `"null"` 记 null；`mode`：解码前转换的色彩模式（如 `"RGB"`），`None` 表示按数据自动推断 |
| `encode_image` | `(image, image_format)` | `image_format`：输出格式（`str` 或 `ImageFormat`，如 `"png"`、`"jpeg"`），返回 Binary 列 |
| `resize` | `(image, w, h)` | `w` / `h`：目标宽度 / 高度（像素，int） |
| `crop` | `(image, bbox)` | `bbox`：`(x, y, width, height)` 四元组，或「每行一个四元素列表」的 List 表达式（按行动态裁剪） |
| `convert_image` | `(image, mode)` | `mode`：目标色彩模式（`str` 或 `ImageMode`，如 `"RGB"`、`"RGBA"`、`"L"`） |
| `image_to_tensor` | `(image)` | 自动推断像素 dtype 与 shape，转为 Tensor 类型（比手动 cast 更安全） |
| `image_width` / `image_height` / `image_channel` / `image_mode` | `(image)` | 无额外参数，返回 UInt32 属性表达式 |
| `image_hash` | `(image, *, method="phash", hash_size=8, binbits=3, segments=3)` | `method`：哈希算法，可选 `"phash"`（默认、最稳健）/ `"phash_simple"` / `"dhash"` / `"dhash_vertical"` / `"ahash"`（最快）/ `"whash"`（`hash_size` 须为 2 的幂）/ `"crop_resistant"`（抗裁剪）/ `"colorhash"`；`hash_size`：空间哈希网格尺寸（输出 `hash_size²` 位，常用 8/16，`colorhash` 忽略）；`binbits`：仅 `colorhash` 生效，输出 `14×binbits` 位；`segments`：仅 `crop_resistant` 生效，图像切分为 `segments×segments` 网格分别哈希 |
| `image_file_metadata` | `(file_expr)` | 从 ImageFile 提取 Struct：`width`(uint32)/`height`(uint32)/`format`(str)/`mode`(str) |
| `decode_image_file` | `(file_expr, mode=None, on_error="raise")` | `mode`：目标模式，`None` 自动推断；`on_error`：同 `decode_image` |

### 6.2 多模态文件算子（File 类型体系）

#### Rust 内核 — [src/daft-file/src/](file:///workspace/src/daft-file/src)

`daft-file` crate 实现多模态文件类型的算子与 Python 绑定：

| 文件 | 职责 |
| --- | --- |
| [functions.rs](file:///workspace/src/daft-file/src/functions.rs) | 注册 `File`、`FilePath`、`VideoFile`、`AudioFile`、`ImageFile`、`Hdf5File`、`GuessMimeType`、`FileExists`（异步）、`Size`（异步）等函数 |
| [python.rs](file:///workspace/src/daft-file/src/python.rs) | Python 类型绑定 |
| [file.rs](file:///workspace/src/daft-file/src/file.rs) | `File` 结构体实现（惰性文件引用、随机访问、read/visit） |
| [meta.rs](file:///workspace/src/daft-file/src/meta.rs) | 文件元数据 |

#### 数据类型核心 — [src/daft-core/src/file/](file:///workspace/src/daft-core/src/file)

[mod.rs](file:///workspace/src/daft-core/src/file/mod.rs) 定义 `File`/`ImageFile`/`VideoFile`/`AudioFile`/`Hdf5File` 的原生数据类型与 Series/Array 实现。

#### Python 文件类型抽象 — [daft/file/](file:///workspace/daft/file)

`daft.File` 及其多模态子类，提供 file-like 惰性引用接口：

| 模块 | 类 | 说明 |
| --- | --- | --- |
| [daft/file/file.py](file:///workspace/daft/file/file.py) | `File` | 基类：惰性文件引用、open/read/visit、随机访问 |
| [daft/file/image.py](file:///workspace/daft/file/image.py) | `ImageFile` | 图像文件，支持元数据提取与按需解码 |
| [daft/file/video.py](file:///workspace/daft/file/video.py) | `VideoFile` | 视频文件，支持元数据、关键帧、帧解码 |
| [daft/file/audio.py](file:///workspace/daft/file/audio.py) | `AudioFile` | 音频文件，支持元数据提取 |
| [daft/file/hdf5.py](file:///workspace/daft/file/hdf5.py) | `Hdf5File` | HDF5 文件，支持 attrs/keys/metadata |
| [daft/file/__init__.py](file:///workspace/daft/file/__init__.py) | — | 导出 `File`、`VideoFile`、`AudioFile`、`ImageFile`、`Hdf5File`、`open_file` |

#### Python 文件函数封装 — [daft/functions/file_.py](file:///workspace/daft/functions/file_.py)

导出 `file`、`file_path`、`file_size`、`file_exists`、`video_file`、`audio_file`、`image_file`、`hdf5_file`、`guess_mime_type`。

#### 调用示例

```python
import daft
from daft.functions import (
    file, video_file, image_file, hdf5_file,
    file_path, file_size, file_exists, guess_mime_type,
)

# 1) URL → 模态化 File 引用（惰性，不下载内容）
df = daft.from_glob_path("s3://bucket/media/*")
df = df.with_column("vfile", video_file(daft.col("path"), verify=True))

# 2) File 列的通用属性
df = (
    df.with_column("path2", file_path(daft.col("vfile")))
      .with_column("size", file_size(daft.col("vfile")))      # UInt64 字节
      .with_column("exists", file_exists(daft.col("vfile")))  # 布尔（异步算子）
)

# 3) 按魔数猜测 MIME 类型（无需解码，无法识别时返回 None）
df = daft.from_pydict({"data": [b"\x89PNG\r\n\x1a\n", b"not a known format"]})
df = df.with_column("mime", guess_mime_type(df["data"]))
# → "image/png" / None
```

#### 参数说明

| 函数 | 签名 | 参数说明 |
| --- | --- | --- |
| `file` | `(url, io_config=None)` | 通用文件引用；`io_config`：可选 IO 配置（S3 凭证、超时等） |
| `video_file` / `audio_file` / `image_file` / `hdf5_file` | `(url, verify=False, io_config=None)` | 将 URL 列转为对应模态的 File 表达式；`verify=True` 时校验文件存在且属于该模态，任一不符即报错 |
| `file_path` | `(file)` | 返回文件 URL 字符串（String） |
| `file_size` | `(file)` | 返回文件字节数（UInt64） |
| `file_exists` | `(file)` | 异步校验文件是否存在（Boolean） |
| `guess_mime_type` | `(bytes_expr)` | 检查二进制魔数，识别 PNG/JPEG/GIF/WEBP/PDF/ZIP/MP3/WAV/OGG/MP4/MPEG/HDF5/HTML，返回 MIME 字符串（如 `"image/png"`）或 `None` |

> 提示：`VideoFile`/`AudioFile`/`Hdf5File` 上还有实例方法（`frames()`、`metadata()`、`resample()`、`keys()`、`attrs()` 等），由下文各模态表达式函数在内部调用。

### 6.3 视频算子

#### Python 封装 — [daft/functions/video.py](file:///workspace/daft/functions/video.py)

导出 `video_metadata`、`video_keyframes`、`video_frames`（从 `VideoFile` 列解码帧）。

#### 视频帧数据源 — [daft/io/av/](file:///workspace/daft/io/av)

| 文件 | 职责 |
| --- | --- |
| [daft/io/av/_read_video_frames.py](file:///workspace/daft/io/av/_read_video_frames.py) | 实现 `daft.read_video_frames`，作为 IO 数据源直接将视频帧读入 DataFrame（区别于 `video_frames` 表达式函数） |
| [daft/io/av/__init__.py](file:///workspace/daft/io/av/__init__.py) | 模块导出 |

#### 调用示例

```python
import daft
from daft.functions import (
    video_file, video_metadata, video_keyframes, video_frames, get_video_frame_by_idx,
)

# 1) 表达式方式：一行一个视频，帧解到 List 列中
df = daft.from_glob_path("hf://datasets/Eventual-Inc/sample-files/videos/*.mp4")
df = (
    df.with_column("file", video_file(daft.col("path")))
      # 元数据 Struct：width / height / fps / frame_count / time_base
      .with_column("metadata", video_metadata(daft.col("file")))
      # 关键帧（List[Image]），可限定时间范围（秒）
      .with_column("keyframes", video_keyframes(daft.col("file"), start_time=0, end_time=10.0))
      # 帧解码（List[Struct]），支持缩放、只取关键帧、按行动态时间范围
      .with_column(
          "frames",
          video_frames(daft.col("file"), width=640, height=480, is_key_frame=True,
                       start_time=daft.col("start"), end_time=daft.col("end")),
      )
      # 按索引取单帧（idx 支持 int 或整数表达式）
      .with_column("first_frame", get_video_frame_by_idx(daft.col("file"), idx=0))
)

# 2) 数据源方式（daft.read_video_frames）：每帧展开为一行，流式读取
df = daft.read_video_frames(
    path="s3://daft-oss-public-data/videos/zoo.mp4",  # 支持 str / list / 通配符 / YouTube URL
    image_height=480,
    image_width=640,
    is_key_frame=True,           # True 仅关键帧 / False 仅非关键帧 / None 全部
    sample_interval_seconds=1.0, # 可选：约每 1 秒采样一帧
)
# 输出列：path, frame_index, frame_time, frame_time_base, frame_pts,
#         frame_dts, frame_duration, is_key_frame, data(Image)
```

#### 参数说明

| 函数 | 签名 | 参数说明 |
| --- | --- | --- |
| `video_metadata` | `(file_expr)` | 无额外参数；返回 Struct：`width`/`height`/`fps`/`frame_count`/`time_base` |
| `video_keyframes` | `(file_expr, *, start_time=0, end_time=None)` | `start_time`/`end_time`：帧提取时间范围（秒，float）；返回 List[Image] |
| `video_frames` | `(file_expr, *, start_time=0, end_time=None, width=None, height=None, is_key_frame=None, sample_interval_seconds=None)` | `start_time`/`end_time`：时间范围（秒），支持 `float` 或表达式（逐行动态）；`width`/`height`：目标缩放尺寸，必须成对提供；`is_key_frame`：`True` 仅关键帧 / `False` 仅非关键帧 / `None` 全部；`sample_interval_seconds`：按 `frame_time` 近似等间隔采样（选取时间戳 ≥ 目标时刻的第一帧），无有效时间戳的帧跳过。返回 List[Struct]，字段：`frame_index`/`frame_time`/`frame_time_base`/`frame_pts`/`frame_dts`/`frame_duration`/`is_key_frame`/`data` |
| `get_video_frame_by_idx` | `(file_expr, idx)` | `idx`：帧索引（`int` 或整数表达式，逐行动态）；返回 Image |
| `daft.read_video_frames` | `(path, image_height, image_width, is_key_frame=None, *, sample_interval_seconds=None, io_config=None)` | `path`：视频路径（`str`/`list[str]`，支持通配符与 YouTube URL，YouTube 需 `pip install yt-dlp`）；`image_height`/`image_width`：所有帧统一缩放到的尺寸（必填）；其余同 `video_frames`；需 PyAV（`pip install av`） |

### 6.4 音频算子

#### Python 封装 — [daft/functions/audio.py](file:///workspace/daft/functions/audio.py)

导出 `audio_metadata`（音频元数据提取）、`resample`（重采样）。

#### 调用示例

```python
import daft
from daft.functions import audio_file, audio_metadata, resample

# 发现音频文件（返回含 path、size 列的 DataFrame）
df = daft.from_glob_path("hf://datasets/Eventual-Inc/sample-files/audio/*.mp3")
df = (
    df.with_column("file", audio_file(daft.col("path")))
      # 元数据 Struct：sample_rate / channels / frames / format / subtype
      .with_column("metadata", audio_metadata(daft.col("file")))
      # 重采样到 16kHz（Tensor[Float64] 波形）
      .with_column("resampled", resample(daft.col("file"), sample_rate=16000))
      .select("path", "size", "metadata", "resampled")
)
# 依赖：pip install 'daft[audio]'（soundfile + librosa）
```

#### 参数说明

| 函数 | 签名 | 参数说明 |
| --- | --- | --- |
| `audio_metadata` | `(file_expr)` | 无额外参数；返回 Struct：`sample_rate`(int)/`channels`(int)/`frames`(float)/`format`(str)/`subtype`(str\|None)；需 `soundfile` |
| `resample` | `(file_expr, sample_rate)` | `sample_rate`：目标采样率（int，如 16000）；返回 Tensor[Float64] 波形；需 `soundfile` + `librosa` |

### 6.5 HDF5 算子

#### Python 封装 — [daft/functions/hdf5.py](file:///workspace/daft/functions/hdf5.py)

导出 `hdf5_attrs`（属性读取）、`hdf5_keys`（键列举）、`hdf5_metadata`（元数据）。

#### 调用示例

```python
import daft
from daft.functions import hdf5_file, hdf5_attrs, hdf5_keys, hdf5_metadata

df = daft.from_glob_path("s3://bucket/data/*.h5")
df = (
    df.with_column("file", hdf5_file(daft.col("path")))
      # 根组（默认 "/"）下的直接成员名称 → List[String]
      .with_column("keys", hdf5_keys(daft.col("file")))
      # 指定组下的成员名称
      .with_column("group_keys", hdf5_keys(daft.col("file"), group="/experiment"))
      # 组/数据集元数据 → List[Struct]：h5path/kind/shape/dtype/chunks/compression
      .with_column("meta", hdf5_metadata(daft.col("file")))
      # 指定对象的属性 → Python dict
      .with_column("attrs", hdf5_attrs(daft.col("file"), h5path="/experiment/dataset"))
)
```

#### 参数说明

| 函数 | 签名 | 参数说明 |
| --- | --- | --- |
| `hdf5_keys` | `(file_expr, group="/")` | `group`：HDF5 组路径，默认根组 `/`；返回该组下直接成员名称列表（List[String]），语义对齐 h5py `Group.keys()` |
| `hdf5_metadata` | `(file_expr, group="/")` | `group`：HDF5 组路径，默认 `/`；返回该组下组与数据集的元数据列表（List[Struct]：`h5path`/`kind`/`shape`/`dtype`/`chunks`/`compression`） |
| `hdf5_attrs` | `(file_expr, h5path="/")` | `h5path`：组或数据集路径，默认 `/`；返回「属性名 → 值」的 Python 字典（Python 类型） |

### 6.6 AI 算子（LLM / 嵌入 / 分类）

#### Python 封装 — [daft/functions/ai/](file:///workspace/daft/functions/ai)

[daft/functions/ai/__init__.py](file:///workspace/daft/functions/ai/__init__.py) 导出 `prompt`（LLM 文本生成）、`embed_text`（文本嵌入）、`embed_image`（图像嵌入）、`classify_text`（零样本文本分类）、`classify_image`（零样本图像分类）。

#### AI Provider 层 — [daft/ai/](file:///workspace/daft/ai)

[daft/ai/provider.py](file:///workspace/daft/ai/provider.py) 定义 `Provider` 抽象与 `PROVIDERS` 注册表，按后端组织：

| 后端 | 目录 | 支持协议 |
| --- | --- | --- |
| OpenAI | [daft/ai/openai/](file:///workspace/daft/ai/openai) | prompter、text_embedder |
| Google | [daft/ai/google/](file:///workspace/daft/ai/google) | prompter |
| Transformers | [daft/ai/transformers/](file:///workspace/daft/ai/transformers) | image_classifier、image_embedder、text_classifier、text_embedder、prompter |
| vLLM | [daft/ai/vllm/](file:///workspace/daft/ai/vllm) | prompter |
| LM Studio | [daft/ai/lm_studio/](file:///workspace/daft/ai/lm_studio) | text_embedder |

公共协议定义于 [daft/ai/protocols.py](file:///workspace/daft/ai/protocols.py)，类型定义于 [daft/ai/typing.py](file:///workspace/daft/ai/typing.py)，表达式于 [daft/ai/_expressions.py](file:///workspace/daft/ai/_expressions.py)。

#### Rust AI 内核 — [src/daft-ai/](file:///workspace/src/daft-ai)

提供 AI provider 协议与表达式内核，通过 [src/lib.rs](file:///workspace/src/lib.rs) 的 `daft_ai::register_modules(m)` 注册到 Python。

#### 调用示例

```python
import daft
from daft.functions import decode_image, embed_text, embed_image, classify_text, classify_image
from daft.functions.ai import prompt

# 1) 文本嵌入 → Embedding[Float32; 384]
df = daft.read_huggingface("togethercomputer/RedPajama-Data-1T")
df = df.with_column(
    "embeddings",
    embed_text(daft.col("text"), provider="transformers",
               model="sentence-transformers/all-MiniLM-L6-v2"),
)

# 2) 图像嵌入：解码 → 预处理 → 嵌入 → Embedding[Float32; 768]
df = (
    daft.from_glob_path("hf://datasets/datasets-examples/doc-image-3/images")
    .with_column("image_bytes", daft.col("path").download())
    .with_column("image_type", decode_image(daft.col("image_bytes")))
    .with_column("image_resized", daft.col("image_type").convert_image("RGB").resize(288, 288))
    .with_column(
        "image_embeddings",
        embed_image(daft.col("image_resized"), provider="transformers",
                    model="apple/aimv2-large-patch14-224-lit"),
    )
)

# 3) 零样本分类（返回最可能标签的字符串）
df = daft.from_pydict({"text": ["Daft is wicked fast!"]})
df = df.with_column(
    "label",
    classify_text(daft.col("text"), labels=["Positive", "Negative"],
                  provider="transformers", model="tabularisai/multilingual-sentiment-analysis"),
)
df = df.with_column(
    "image_label",
    classify_image(daft.col("image_resized"),
                   labels=["bulbasaur", "catapie", "voltorb", "electrode"],
                   provider="transformers", model="openai/clip-vit-base-patch32"),
)

# 4) LLM 提问（需设置 OPENAI_API_KEY）
df = daft.from_pydict({"quote": ["I am going to be the king of the pirates!"]})
df = df.with_column(
    "response",
    prompt(daft.col("quote"),
           system_message="Classify the anime from the quote and return the show, character name, and explanation.",
           provider="openai", model="gpt-5-nano"),
)

# 5) 结构化输出：用 Pydantic 模型约束返回格式
from pydantic import BaseModel, Field

class Anime(BaseModel):
    show: str = Field(description="The name of the anime show")
    character: str = Field(description="The name of the character who says the quote")

df = df.with_column(
    "out",
    prompt(daft.col("quote"), return_format=Anime, provider="openai", model="gpt-5-nano"),
)
```

#### 参数说明

| 函数 | 签名 | 参数说明 |
| --- | --- | --- |
| `embed_text` | `(text, *, provider=None, model=None, dimensions=None, **options)` | `text`：字符串列表达式；`provider`：后端名或 `Provider` 实例（默认 `"transformers"`）；`model`：嵌入模型名（默认用该 provider 的默认模型）；`dimensions`：输出向量维度（provider/模型支持时生效）；`**options`：`EmbedTextOptions` 等其余模型选项。返回 Embedding 列 |
| `embed_image` | `(image, *, provider=None, model=None, **options)` | `image`：Image 列表达式；其余同 `embed_text`（无 `dimensions`） |
| `classify_text` / `classify_image` | `(input, labels, *, provider=None, model=None, **options)` | `input`：文本/图像列表达式；`labels`：单个标签或标签列表（零样本分类候选）；`model`：默认用 `zero-shot-classification` 类模型；返回最可能标签字符串（String） |
| `prompt` | `(messages, return_format=None, *, system_message=None, provider=None, model=None, **options)` | `messages`：单条消息表达式或消息列表；每条可为纯文本、图像数据（ndarray/bytes/File，按 MIME 检测）或文件（PDF/TXT/HTML/音视频等）；`return_format`：Pydantic `BaseModel`，指定后返回结构化输出（Struct 列）；`system_message`：系统提示词；`provider`：默认 `"openai"`（支持 OpenAI/Google/Transformers/vLLM 等）；`model`：模型名；`**options`：`PromptOptions`（如温度等） |

**Provider 解析顺序**（`_resolve_provider`，[daft/functions/ai/__init__.py](file:///workspace/daft/functions/ai/__init__.py)）：显式传入 `Provider` 实例 → 在当前 `Session` 中按名查找 → `load_provider(name)` 直接加载 → 会话当前 provider → 各 API 的默认 provider（嵌入/分类为 `transformers`，prompt 为 `openai`）。

**vLLM 特殊限制**：`provider="vllm"` 时 `prompt` 不支持 `return_format`、`system_message` 与多消息列表，走 Rust 侧前缀缓存内核（`Expression.vllm`）。

### 6.7 函数注册入口 — [src/lib.rs](file:///workspace/src/lib.rs)

所有多模态函数在 PyModule 初始化时统一注册到 `daft_dsl::functions::FUNCTION_REGISTRY`：

```rust
// 图像函数
functions_registry.register::<daft_image::functions::ImageFunctions>();
// 文件函数（含多模态文件类型）
functions_registry.add_fn(daft_file::File);
functions_registry.add_fn(daft_file::FilePath);
functions_registry.add_async_fn(daft_file::FileExists);
functions_registry.add_async_fn(daft_file::Size);
functions_registry.add_fn(daft_file::VideoFile);
functions_registry.add_fn(daft_file::AudioFile);
functions_registry.add_fn(daft_file::ImageFile);
functions_registry.add_fn(daft_file::Hdf5File);
functions_registry.add_fn(daft_file::GuessMimeType);
```

### 6.8 调用链总结

```
用户 API                          Python 封装              Rust 内核
─────────────────────────────────────────────────────────────────────
daft.functions.image.resize()  → daft/functions/image.py → daft-image::functions::ImageFunctions (resize.rs)
daft.functions.video_frames() → daft/functions/video.py → daft-file::functions (VideoFile)
daft.functions.audio_metadata()→ daft/functions/audio.py→ daft-file::functions (AudioFile)
daft.functions.hdf5_attrs()   → daft/functions/hdf5.py → daft-file::functions (Hdf5File)
daft.functions.file()          → daft/functions/file_.py→ daft-file::functions (File)
daft.functions.ai.prompt()    → daft/functions/ai/     → daft-ai (provider 内核)
daft.read_video_frames()       → daft/io/av/            → daft-scan (ScanOperator)
daft.ImageFile / VideoFile... → daft/file/*.py          → daft-core::file (数据类型)
```

### 6.9 多模态文档参考

用户侧多模态使用文档位于 [docs/modalities/](file:///workspace/docs/modalities)：

| 文档 | 内容 |
| --- | --- |
| [overview.md](file:///workspace/docs/modalities/overview.md) | 多模态总览 |
| [images.md](file:///workspace/docs/modalities/images.md) | 图像处理指南（下载/解码/哈希/嵌入/分类） |
| [audio.md](file:///workspace/docs/modalities/audio.md) | 音频处理指南 |
| [videos.md](file:///workspace/docs/modalities/videos.md) | 视频处理指南 |
| [files.md](file:///workspace/docs/modalities/files.md) | File 数据类型与 URL 处理 |
| [tensors.md](file:///workspace/docs/modalities/tensors.md) | 张量与 HDF5 |

AI 函数文档位于 [docs/ai-functions/](file:///workspace/docs/ai-functions)：[overview.md](file:///workspace/docs/ai-functions/overview.md)、[embed.md](file:///workspace/docs/ai-functions/embed.md)、[classify.md](file:///workspace/docs/ai-functions/classify.md)、[prompt.md](file:///workspace/docs/ai-functions/prompt.md)。

---

## 7. 依赖关系

### 7.1 Rust 工作区依赖（[Cargo.toml](file:///workspace/Cargo.toml)）

**核心外部依赖**（在 `[workspace.dependencies]` 统一版本管理）：

| 依赖 | 用途 |
| --- | --- |
| `arrow` (59.0.0) / `arrow-array` / `arrow-buffer` / `arrow-schema` / `arrow-select` / `arrow-csv` / `arrow-json` / `arrow-ipc` / `arrow-flight` | Arrow 列式内存格式生态 |
| `parquet` (59.0.0) | Parquet 读写 |
| `pyo3` (0.29, abi3-py310) / `pyo3-async-runtimes` / `pyo3-log` | Python 互操作 |
| `tokio` (1.48) / `tokio-stream` / `tokio-util` / `tonic` | 异步运行时与 gRPC |
| `rayon` | 数据并行 |
| `serde` / `serde_json` / `bincode` / `prost` | 序列化 |
| `snafu` / `thiserror` | 错误处理 |
| `tracing` / `tracing-subscriber` / `opentelemetry` / `opentelemetry-otlp` | 可观测性 |
| `sqlparser` (0.59) | SQL 解析 |
| `reqwest` / `url` | HTTP |
| `image` / `ndarray` / `numpy` | 图像/数值 |
| `tikv-jemallocator` | 全局分配器（非 MSVC 平台） |
| `dashmap` / `parking_lot` | 并发同步 |
| `xxhash-rust` / `sha1` / `uuid` | 哈希与标识 |
| `mcap` | MCAP 格式 |

**crate 间依赖**（daft-* 内部路径依赖，见 Cargo.toml `[dependencies]`）形成分层：

```
common-* (基础)
   ↓
daft-core / daft-schema / daft-dsl / daft-io
   ↓
daft-recordbatch / daft-micropartition / daft-parquet / daft-csv / daft-json / daft-avro / daft-functions-*
   ↓
daft-scan / daft-logical-plan
   ↓
daft-local-plan / daft-local-execution / daft-distributed
   ↓
daft (根 crate, cdylib, 汇总注册)
```

### 7.2 Python 依赖（[pyproject.toml](file:///workspace/pyproject.toml)）

**必需依赖**：

| 依赖 | 用途 |
| --- | --- |
| `pyarrow` (≥16,<26) | Arrow 内存互操作 |
| `tqdm` (<4.68) | 进度条 |
| `packaging` | 版本解析 |
| `typing-extensions` (Python<3.12) | 类型扩展 |

**可选依赖（extras）**：`all` 聚合以下，按需安装：

`audio`、`aws`、`azure`、`clickhouse`、`deltalake`、`google`、`gravitino`、`hdf5`、`http`、`huggingface`、`iceberg`、`jev`、`kafka`、`lance`、`mcap`、`numpy`、`openai`、`pandas`、`postgres`、`ray`、`sql`、`transformers`、`turbopuffer`、`unity`、`video`、`viz`

**开发依赖（[dependency-groups] dev）**：`maturin`、`ruff`、`mypy`、`pre-commit`、`pytest`/`pytest-xdist`/`pytest-benchmark`/`hypothesis`、`ray`、`torch`、`duckdb`、`moto`、`pyiceberg`、`deltalake`、`vllm`、`openai`、`google-genai` 等。

**文档依赖（docs）**：`mkdocs-material`、`mkdocstrings-python`、`mkdocs-jupyter` 等。

### 7.3 构建工具

- **构建后端**：`maturin`（`[build-system]`），启用 `python` feature 编译为 abi3 扩展模块
- **包管理**：`uv`（`[tool.uv]`，required-version `~=0.12.0`），含 workspace（`daft_dashboard`）
- **Rust 工具链**：[rust-toolchain.toml](file:///workspace/rust-toolchain.toml)
- **Lint**：`ruff`（Python）、`clippy`（Rust，pedantic+nursery+perf+style deny）
- **格式化**：`ruff-format` + `rustfmt`

---

## 8. 项目运行方式

### 8.1 环境准备

> 详见 [AGENTS.md](file:///workspace/AGENTS.md) 与 [CONTRIBUTING.md](file:///workspace/CONTRIBUTING.md)

```bash
# 1. 创建 Python 虚拟环境并安装依赖（需先安装 uv）
make .venv

# 2. （可选）激活虚拟环境
source .venv/bin/activate
```

### 8.2 编译构建

由于核心引擎是 Rust，修改 Rust 代码后需重新编译：

```bash
# 开发构建（编译并安装到 .venv）
make build

# Release 构建（更快但编译更慢）
make build-release

# 仅生成 wheel 包（不安装）
make build-whl

# 交叉编译 wheel（实验性，可选 --target/--zig）
make build-whl-cross BUILD_WHL_TARGET=... BUILD_WHL_USE_ZIG=1
```

构建产物为 `daft/daft.abi3.so`（原生扩展模块），由 Python 包 `daft.daft` 导入。

### 8.3 安装

```bash
# 从 PyPI 安装（终端用户）
pip install daft

# 安装全部可选依赖
pip install "daft[all]"

# 从源码开发安装
make .venv && make build
```

### 8.4 选择执行 Runner

```python
import daft

# 本地多线程执行（默认）
daft.set_runner_native()

# Ray 分布式执行
daft.set_runner_ray(address="auto")
```

或通过环境变量：`DAFT_RUNNER=native` / `DAFT_RUNNER=ray`。

首次调用 `get_or_create_runner()` 后，runner 在进程生命周期内锁定。

### 8.5 运行示例

```python
import daft

# 从内存构造
df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})

# 惰性变换
df = df.with_column("z", daft.col("x") + daft.col("y"))
df = df.where("z > 5")
df = df.select("x", "z")

# 触发执行
df.show()
df.collect()
```

### 8.6 代码质量与文档

```bash
make format       # 格式化 Python 与 Rust
make lint         # Lint（ruff + clippy）
make check-format # 检查格式（pre-commit --all-files）
make hooks        # 安装 pre-commit 钩子

make docs         # 构 mkdocs 文档
make docs-serve   # 开发服务器预览文档
```

### 8.7 清理

```bash
make clean        # 删除 .venv、target、site、.so
```

---

## 9. 测试体系

### 9.1 测试命令

测试位于 [tests/](file:///workspace/tests) 目录，使用 `pytest`：

```bash
# 运行测试（默认排除 integration/benchmark/hypothesis）
make test

# 指定 runner（必须设置）
DAFT_RUNNER=native make test
DAFT_RUNNER=ray make test

# 运行单个测试文件/方法
make test EXTRA_ARGS="-v tests/dataframe/test_select.py"
make test EXTRA_ARGS="-v tests/dataframe/test_select.py::test_select_dataframe"

# 运行 doctest（daft/ 目录下的 docstring）
make doctests
```

`make test` 默认通过 `pytest -n auto` 并行执行，并设置 `--hypothesis-seed`。

### 9.2 测试目录结构

[tests/](file:///workspace/tests) 按功能域组织：

| 目录 | 覆盖范围 |
| --- | --- |
| `dataframe/` | DataFrame API 全量测试（select/filter/join/agg/sort/window 等） |
| `expressions/` | 表达式与 UDF 表达式 |
| `series/` | Series 列操作 |
| `recordbatch/` | MicroPartition/RecordBatch 内核 |
| `io/` | 文件读取/写入、分区、下推、云存储（含 mock AWS） |
| `sql/` | SQL 查询与表达式 |
| `catalog/` | Catalog 后端（Iceberg/Glue/Unity/Gravitino/S3Tables） |
| `udf/` | UDF/UDAF 执行 |
| `window/` | 窗口函数 |
| `functions/` | 内置函数 |
| `integrations/` | 集成测试（dashboard、TPCH、文件 UDF） |
| `ray/` | Ray runner 专项 |
| `benchmarks/` | 性能基准（ClickBench/TPCH/Parquet/JSON） |
| `property_based_testing/` | Hypothesis 属性测试 |
| `microbenchmarks/` | 微基准（codspeed） |
| `observability/` | OpenTelemetry、进度条 |
| `checkpoint/` | 检查点配置 |
| `cookbook/` | 用法样例测试 |

### 9.3 测试配置

[pyproject.toml](file:///workspace/pyproject.toml) 的 `[tool.pytest.ini_options]`：

- `addopts = "-m 'not (integration or benchmark or hypothesis)'"`：默认禁用集成/基准/属性测试（建议在 CI 运行）
- `testpaths = ["tests"]`
- `minversion = "6.0"`

### 9.4 PR 规范

- 标题：Conventional Commits 格式（由 `.github/workflows/pr-labeller.yml` 强制）
- 描述：遵循 [.github/pull_request_template.md](file:///workspace/.github/pull_request_template.md)
- 非平凡 PR 需关联已批准的 issue、包含测试、保持小型

---

## 附录：关键文件速查

| 文件 | 说明 |
| --- | --- |
| [daft/__init__.py](file:///workspace/daft/__init__.py) | Python 顶层公共 API 聚合导出 |
| [src/lib.rs](file:///workspace/src/lib.rs) | Rust PyModule 入口，注册所有子模块与函数注册表 |
| [Cargo.toml](file:///workspace/Cargo.toml) | Rust 工作区定义与依赖版本管理 |
| [pyproject.toml](file:///workspace/pyproject.toml) | Python 项目元数据、依赖、构建（maturin）、工具配置 |
| [Makefile](file:///workspace/Makefile) | 构建/测试/格式化/文档等开发命令 |
| [AGENTS.md](file:///workspace/AGENTS.md) | 开发工作流与测试说明 |
| [CONTRIBUTING.md](file:///workspace/CONTRIBUTING.md) | 贡献指南与治理 |
| [daft/dataframe/dataframe.py](file:///workspace/daft/dataframe/dataframe.py) | DataFrame 类实现 |
| [daft/expressions/expressions.py](file:///workspace/daft/expressions/expressions.py) | Expression 类与 col/lit |
| [daft/logical/builder.py](file:///workspace/daft/logical/builder.py) | LogicalPlanBuilder |
| [daft/context.py](file:///workspace/daft/context.py) | DaftContext 全局上下文 |
| [daft/runners/runner.py](file:///workspace/daft/runners/runner.py) | Runner 抽象基类 |
| [daft/execution/native_executor.py](file:///workspace/daft/execution/native_executor.py) | NativeExecutor 本地执行器 |
