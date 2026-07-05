# Delta Live Tables (DLT)

## Overview

**Delta Live Tables (DLT)** is a declarative data pipeline framework on Databricks that simplifies building and managing reliable data processing pipelines. DLT abstracts away the operational complexity of Spark — cluster management, task orchestration, checkpointing, error handling, and data quality enforcement — allowing engineers to focus on the **transformation logic** rather than infrastructure.

DLT is the recommended approach for building **Bronze → Silver → Gold** medallion pipelines on Databricks. It handles:
- **Dependency management** — automatically resolves table dependencies
- **Incremental processing** — streaming by default with checkpoint management
- **Data quality** — built-in expectations with quarantine support
- **Error recovery** — automatic retries and state management
- **Observability** — rich UI with data flow DAG, metrics, and lineage

## Key Concepts

### Datasets

DLT uses two primary dataset types:

| Type                    | Syntax                          | Description                                        |
|-------------------------|---------------------------------|----------------------------------------------------|
| **Streaming Live Table** | `STREAMING LIVE TABLE`         | Processes only new data per update; append or merge |
| **Live Table**           | `LIVE TABLE`                   | Recomputes fully each update; materialized view      |
| **Streaming Live View**  | `STREAMING LIVE VIEW`          | Non-materialized; evaluated on query                |
| **Live View**            | `LIVE VIEW`                    | Non-materialized; evaluated on query                |

**When to use each:**
- **Streaming table**: Append-only or merge-based incremental loads (Bronze, Silver CDC)
- **Live table**: Full refresh aggregations, dimension loads (Gold, some Silver)
- **Views**: Intermediate transformations, debugging, shared logic

### Pipeline

A DLT pipeline is a deployment unit that contains all datasets and their dependencies. It defines:
- **Source** (notebook or SQL file)
- **Target** (catalog/schema for materialized tables)
- **Compute** (cluster configuration)
- **Schedule** (trigger type)
- **Configuration** (parameters, environment)

### Update

An update is a single execution of a pipeline. DLT processes all datasets in dependency order, processing only new data for streaming tables and recomputing live tables.

## SQL vs. Python Interface

DLT supports both **SQL** and **Python** for defining pipelines. Both can coexist in the same pipeline.

### SQL Syntax

```sql
-- Bronze: Raw ingestion from cloud files
CREATE OR REFRESH STREAMING LIVE TABLE bronze_clickstream
COMMENT "Raw clickstream events from landing zone"
AS SELECT
    *,
    current_timestamp() AS _ingest_ts,
    _metadata.file_path AS _source_file
FROM cloud_files(
    "/mnt/landing/clickstream/",
    "json",
    map("cloudFiles.inferColumnTypes", "true")
);

-- Silver: Cleansed and deduplicated
CREATE OR REFRESH STREAMING LIVE TABLE silver_clickstream
COMMENT "Cleansed clickstream events"
AS SELECT
    event_id,
    user_id,
    event_type,
    to_timestamp(event_ts) AS event_time,
    to_date(event_ts) AS event_date,
    page_url,
    device_type,
    current_timestamp() AS _processed_ts
FROM STREAM(LIVE.bronze_clickstream)
WHERE event_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_time DESC) = 1;

-- Gold: Daily user activity summary
CREATE OR REFRESH LIVE TABLE gold_daily_activity
COMMENT "Daily user activity aggregation"
AS SELECT
    event_date,
    user_id,
    count(*) AS event_count,
    count(DISTINCT event_type) AS unique_event_types,
    count(DISTINCT page_url) AS unique_pages,
    min(event_time) AS first_event_time,
    max(event_time) AS last_event_time
FROM LIVE.silver_clickstream
GROUP BY event_date, user_id;
```

### Python Syntax

```python
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Bronze: Raw ingestion
@dlt.table(
    comment="Raw clickstream events from landing zone",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
def bronze_clickstream():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/mnt/landing/clickstream/")
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# Silver: Cleansed with data quality expectations
@dlt.table(
    comment="Cleansed clickstream events",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_event_id", "event_id IS NOT NULL")
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
@dlt.expect_or_quarantine("valid_timestamp", "event_ts IS NOT NULL")
def silver_clickstream():
    return (
        dlt.read_stream("bronze_clickstream")
        .select(
            "event_id",
            "user_id",
            "event_type",
            to_timestamp("event_ts").alias("event_time"),
            to_date("event_ts").alias("event_date"),
            "page_url",
            "device_type",
            current_timestamp().alias("_processed_ts")
        )
    )

# Gold: Aggregation
@dlt.table(
    comment="Daily user activity aggregation",
    table_properties={"quality": "gold"}
)
def gold_daily_activity():
    return (
        dlt.read("silver_clickstream")
        .groupBy("event_date", "user_id")
        .agg(
            count("*").alias("event_count"),
            countDistinct("event_type").alias("unique_event_types"),
            countDistinct("page_url").alias("unique_pages"),
            min("event_time").alias("first_event_time"),
            max("event_time").alias("last_event_time"),
        )
    )
```

## Data Quality with Expectations

DLT's **expectations** are the core data quality mechanism. They define rules that validate data during pipeline execution.

### Expectation Types

| Decorator / Clause        | Behavior on Failure                         |
|---------------------------|---------------------------------------------|
| `@dlt.expect`              | Log violation, keep record (metric tracking) |
| `@dlt.expect_or_drop`      | Drop violating record, log count             |
| `@dlt.expect_or_quarantine`| Route to quarantine table, log count         |
| `@dlt.expect_or_fail`      | Fail the pipeline update                     |

### SQL Expectations

```sql
CONSTRAINT valid_event_id EXPECT (event_id IS NOT NULL) ON VIOLATION DROP ROW;
CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION QUARANTINE ROW;
CONSTRAINT valid_email EXPECT (email RLIKE '^[^@]+@[^@]+\.[^@]+$') ON VIOLATION FAIL UPDATE;
```

### Python Expectations

```python
@dlt.table
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "amount > 0")
@dlt.expect_or_quarantine("valid_email", "email RLIKE '^[^@]+@[^@]+\\.[^@]+$'")
@dlt.expect_or_fail("non_null_customer", "customer_id IS NOT NULL")
def silver_orders():
    return dlt.read_stream("bronze_orders")
```

### Quarantine Tables

When using `expect_or_quarantine`, DLT automatically creates a companion table named `<table_name>_quarantine` containing the dropped records:

```python
# Access quarantined records
quarantined = spark.read.table("silver.orders_quarantine")

# The quarantine table includes:
# - Original data columns
# - __expectation_violations: array of failed expectation names
```

### Custom Data Quality with Python

```python
@dlt.table(comment="Orders with quality scores")
def silver_orders_scored():
    df = dlt.read("bronze_orders")

    # Custom quality scoring
    from pyspark.sql.functions import when, col, lit

    scored = df.withColumn(
        "quality_score",
        when(col("order_id").isNull(), lit(0))
        .when(col("amount") <= 0, lit(1))
        .when(col("customer_id").isNull(), lit(2))
        .otherwise(lit(100))
    )

    # Split into valid and invalid
    return scored.filter(col("quality_score") == 100)

@dlt.table(comment="Quarantined orders")
def silver_orders_invalid():
    df = dlt.read("bronze_orders")
    # ... same logic, filter for low quality_score
    return scored.filter(col("quality_score") < 100)
```

## Change Data Capture (CDC) with APPLY CHANGES

DLT provides `APPLY CHANGES` for processing CDC streams and maintaining SCD Type 1 or Type 2 tables.

### SCD Type 1 (Overwrite)

```sql
-- Bronze: Raw CDC events
CREATE OR REFRESH STREAMING LIVE TABLE bronze_cdc_customers
AS SELECT * FROM cloud_files("/mnt/cdc/customers/", "json");

-- Silver: SCD1 target (latest value wins)
CREATE OR REFRESH STREAMING LIVE TABLE silver.dim_customer;

APPLY CHANGES INTO live.silver.dim_customer
FROM STREAM(live.bronze_cdc_customers)
KEYS (customer_id)
SEQUENCE BY _change_ts
COLUMNS * EXCEPT (_change_ts, _change_type, _op)
STORED AS SCD TYPE 1;
```

### SCD Type 2 (Historical Tracking)

```sql
CREATE OR REFRESH STREAMING LIVE TABLE silver.dim_customer_scd2;

APPLY CHANGES INTO live.silver.dim_customer_scd2
FROM STREAM(live.bronze_cdc_customers)
KEYS (customer_id)
SEQUENCE BY _change_ts
COLUMNS * EXCEPT (_change_ts, _change_type, _op)
STORED AS SCD TYPE 2
TRACK HISTORY ON (customer_name, email, phone, address, region);
```

### Python APPLY CHANGES

```python
import dlt
from pyspark.sql.functions import *

@dlt.table
def bronze_cdc_customers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/mnt/cdc/customers/")
    )

dlt.create_streaming_table("silver.dim_customer_scd2")

dlt.apply_changes(
    target="silver.dim_customer_scd2",
    source="bronze_cdc_customers",
    keys=["customer_id"],
    sequence_by=col("_change_ts"),
    stored_as_scd_type=2,
    track_history_except_column_names=["_change_ts", "_change_type", "_op"]
)
```

## Auto Loader (Cloud Files)

DLT integrates with **Auto Loader** (`cloud_files`) for incrementally processing new files from cloud storage:

```sql
-- JSON files with schema inference
CREATE OR REFRESH STREAMING LIVE TABLE bronze_events
AS SELECT * FROM cloud_files(
    "/mnt/landing/events/",
    "json",
    map(
        "cloudFiles.inferColumnTypes", "true",
        "cloudFiles.schemaLocation", "/mnt/landing/events/_schema",
        "cloudFiles.maxFilesPerTrigger", "1000"
    )
);

-- CSV with explicit schema
CREATE OR REFRESH STREAMING LIVE TABLE bronze_sales_csv
AS SELECT * FROM cloud_files(
    "/mnt/landing/sales/",
    "csv",
    map(
        "cloudFiles.inferColumnTypes", "false",
        "header", "true",
        "cloudFiles.schemaLocation", "/mnt/landing/sales/_schema"
    )
);
```

**Supported file formats**: JSON, CSV, Parquet, Avro, Text, BinaryFile

## Pipeline Configuration

### Pipeline Settings (UI / JSON)

```json
{
  "name": "Sales Medallion Pipeline",
  "edition": "ADVANCED",
  "channel": "CURRENT",
  "target": "enterprise_prod.silver",
  "development": false,
  "photon": true,
  "configuration": {
    "pipelines.trigger.interval": "10 minutes",
    "pipelines.reset.allowed": "false",
    "pipelines.autoOptimize.managed": "true",
    "env": "prod",
    "source_root": "/mnt/landing"
  },
  "clusters": [
    {
      "label": "default",
      "autoscale": {
        "mode": "ENHANCED",
        "min_workers": 1,
        "max_workers": 8
      },
      "driver_node_type_id": "Standard_DS3_v2",
      "node_type_id": "Standard_DS3_v2",
      "spark_conf": {
        "spark.sql.shuffle.partitions": "200"
      }
    }
  ],
  "libraries": [
    {
      "notebook": {
        "path": "/pipelines/sales_medallion"
      }
    }
  ],
  "continuous": false
}
```

### Key Configuration Parameters

| Parameter                        | Description                                           |
|----------------------------------|-------------------------------------------------------|
| `pipelines.trigger.interval`     | How often the pipeline checks for new data           |
| `pipelines.reset.allowed`        | Allow full refresh of streaming tables (dangerous)   |
| `pipelines.autoOptimize.managed` | Let DLT manage file compaction                        |
| `edition`                        | `CORE` (basic), `PRO` (expectations), `ADVANCED` (CDC, DQ) |
| `channel`                        | `CURRENT` (stable) or `RELEASE` (latest features)    |
| `continuous`                     | `true` for continuous streaming, `false` for triggered |
| `photon`                         | Enable Photon engine for faster execution             |
| `development`                    | `true` for dev mode (no auto-restart, full logs)      |

### Unity Catalog Integration

```json
{
  "catalog": "enterprise_prod",
  "target": "silver",
  "configuration": {
    "pipelines.channel": "CURRENT"
  }
}
```

With Unity Catalog, DLT tables are materialized as:
- `<catalog>.<schema>.<table_name>` for materialized tables
- `<catalog>.<schema>.<table_name>_quarantine` for quarantine tables
- `<catalog>.<schema>.event_log_<pipeline_id>` for event logs

## Environment and Parameterization

### Using Spark Config in DLT

```python
import dlt

# Access configuration values
env = spark.conf.get("env", "dev")
source_root = spark.conf.get("source_root", "/mnt/dev/landing")

@dlt.table(comment=f"Bronze orders from {env}")
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{source_root}/orders/")
    )
```

### Dynamic Pipeline (Python)

```python
import dlt

# Define source configurations dynamically
SOURCES = [
    {"name": "customers", "path": "/mnt/landing/customers/", "format": "json"},
    {"name": "orders", "path": "/mnt/landing/orders/", "format": "csv"},
    {"name": "products", "path": "/mnt/landing/products/", "format": "parquet"},
]

# Dynamically create Bronze tables
for source in SOURCES:
    @dlt.table(name=f"bronze_{source['name']}", comment=f"Raw {source['name']} data")
    def bronze_table(src=source):
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", src["format"])
            .load(src["path"])
        )
```

## Monitoring and Observability

### Event Log

DLT records all pipeline events to an event log table. Query it for insights:

```sql
-- Access the event log (Unity Catalog)
SELECT * FROM event_log(TABLE("enterprise_prod.silver.silver_orders"));

-- Get pipeline metrics
SELECT
    event_type,
    origin.update_id,
    timestamp,
    details
FROM event_log(TABLE("enterprise_prod.silver.silver_orders"))
WHERE event_type IN ('flow_progress', 'update_progress', 'create_update')
ORDER BY timestamp DESC
LIMIT 100;
```

### Key Event Types

| Event Type              | Description                                    |
|------------------------|------------------------------------------------|
| `create_update`         | Pipeline update started                        |
| `update_progress`       | Overall update progress                        |
| `flow_progress`         | Individual dataset (flow) progress             |
| `definition_update`     | Dataset definitions changed                    |
| `flow_definition_logged`| Dataset metadata logged                        |
| `maintenance_progress`  | OPTIMIZE/VACUUM progress                       |
| `dataset_definition`    | Schema and properties of a dataset              |

### Data Quality Metrics from Event Log

```sql
SELECT
    timestamp,
    details:flow_definition.output_dataset AS dataset,
    details:flow_progress.metrics.num_output_rows AS output_rows,
    details:flow_progress.metrics.num_dropped_rows AS dropped_rows,
    details:flow_progress.metrics.num_quarantined_rows AS quarantined_rows
FROM event_log(TABLE("enterprise_prod.silver.silver_orders"))
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC;
```

### Alerting

```python
# Alert on data quality failures
# Use Databricks SQL alerts on DLT event log queries

# Example SQL alert query:
"""
SELECT
    timestamp,
    details:flow_definition.output_dataset AS dataset,
    CAST(details:flow_progress.metrics.num_dropped_rows AS INT) AS dropped_rows
FROM event_log(TABLE('enterprise_prod.silver.silver_orders'))
WHERE event_type = 'flow_progress'
  AND CAST(details:flow_progress.metrics.num_dropped_rows AS INT) > 1000
ORDER BY timestamp DESC
"""
```

## Full Example: E-Commerce Medallion Pipeline

### SQL Version

```sql
-- ============================================
-- BRONZE LAYER: Raw Ingestion
-- ============================================

CREATE OR REFRESH STREAMING LIVE TABLE bronze_customers
COMMENT "Raw customer data from CRM source"
AS SELECT
    *,
    current_timestamp() AS _ingest_ts,
    _metadata.file_path AS _source_file
FROM cloud_files("/mnt/landing/crm/customers/", "csv",
    map("header", "true", "cloudFiles.inferColumnTypes", "true"));

CREATE OR REFRESH STREAMING LIVE TABLE bronze_orders
COMMENT "Raw order data from ERP source"
AS SELECT
    *,
    current_timestamp() AS _ingest_ts,
    _metadata.file_path AS _source_file
FROM cloud_files("/mnt/landing/erp/orders/", "json",
    map("cloudFiles.inferColumnTypes", "true"));

CREATE OR REFRESH STREAMING LIVE TABLE bronze_events
COMMENT "Raw clickstream events from web/app"
AS SELECT
    *,
    current_timestamp() AS _ingest_ts
FROM cloud_files("/mnt/landing/clickstream/", "json",
    map("cloudFiles.inferColumnTypes", "true"));

-- ============================================
-- SILVER LAYER: Cleansed and Conformed
-- ============================================

CREATE OR REFRESH STREAMING LIVE TABLE silver_dim_customer
COMMENT "Cleansed customer dimension"
CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_email EXPECT (email RLIKE '^[^@]+@[^@]+\\.[^@]+$') ON VIOLATION QUARANTINE ROW
AS SELECT
    customer_id,
    trim(customer_name) AS customer_name,
    lower(email) AS email,
    phone,
    address,
    region,
    customer_segment,
    current_timestamp() AS _processed_ts
FROM STREAM(LIVE.bronze_customers)
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ingest_ts DESC) = 1;

CREATE OR REFRESH STREAMING LIVE TABLE silver_fact_orders
COMMENT "Cleansed orders fact table"
CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW
CONSTRAINT valid_customer EXPECT (customer_id IS NOT NULL) ON VIOLATION QUARANTINE ROW
AS SELECT
    order_id,
    customer_id,
    product_id,
    to_date(order_date) AS order_date,
    cast(amount AS DECIMAL(18,2)) AS amount,
    cast(quantity AS INT) AS quantity,
    order_status,
    current_timestamp() AS _processed_ts
FROM STREAM(LIVE.bronze_orders)
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingest_ts DESC) = 1;

-- ============================================
-- GOLD LAYER: Curated Analytics
-- ============================================

CREATE OR REFRESH LIVE TABLE gold_customer_360
COMMENT "360-degree customer view for analytics"
AS SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.region,
    c.customer_segment,
    count(DISTINCT o.order_id) AS total_orders,
    sum(o.amount) AS total_revenue,
    avg(o.amount) AS avg_order_value,
    max(o.order_date) AS last_order_date,
    datediff(current_date(), max(o.order_date)) AS days_since_last_order,
    count(DISTINCT e.event_id) AS total_events
FROM LIVE.silver_dim_customer c
LEFT JOIN LIVE.silver_fact_orders o ON c.customer_id = o.customer_id
LEFT JOIN (
    SELECT user_id, count(*) AS event_count, count(DISTINCT event_id) AS event_id
    FROM LIVE.bronze_events
    GROUP BY user_id
) e ON c.customer_id = e.user_id
GROUP BY
    c.customer_id, c.customer_name, c.email, c.region, c.customer_segment;

CREATE OR REFRESH LIVE TABLE gold_monthly_revenue_by_region
COMMENT "Monthly revenue aggregated by region"
AS SELECT
    date_trunc('month', order_date) AS month,
    region,
    sum(amount) AS total_revenue,
    count(DISTINCT order_id) AS order_count,
    count(DISTINCT customer_id) AS unique_customers
FROM LIVE.silver_fact_orders o
JOIN LIVE.silver_dim_customer c ON o.customer_id = c.customer_id
GROUP BY date_trunc('month', order_date), region;
```

## Best Practices

### Development

| Practice                          | Rationale                                        |
|-----------------------------------|--------------------------------------------------|
| Use `development: true` in dev    | Prevents auto-restart, shows full logs            |
| Start with SQL, switch to Python for complex logic | SQL is simpler for declarative pipelines |
| Use views for intermediate steps  | Avoid materializing unnecessary tables            |
| Test expectations in dev          | Verify data quality rules before prod             |
| Use `pipelines.reset.allowed: false` in prod | Prevents accidental full refreshes      |

### Performance

| Practice                          | Rationale                                        |
|-----------------------------------|--------------------------------------------------|
| Use Enhanced Autoscaling          | Scales compute dynamically per workload           |
| Enable Photon                     | Vectorized execution for faster processing        |
| Set `cloudFiles.maxFilesPerTrigger` | Control ingestion rate and resource usage      |
| Use streaming tables for append sources | Efficient incremental processing           |
| Use live tables for full-refresh gold | Avoids streaming complexity for aggregations  |

### Production

| Practice                          | Rationale                                        |
|-----------------------------------|--------------------------------------------------|
| Pin to `CURRENT` channel          | Stability over latest features                    |
| Use Unity Catalog                 | Governance, lineage, access control               |
| Monitor event log                 | Track data quality, freshness, failures           |
| Set up alerts                     | Proactive notification on failures/delays         |
| Version control notebooks         | Track changes, enable rollback                    |
| Use DABs for deployment           | CI/CD across environments                         |

### Common Pitfalls

1. **Using live tables where streaming is needed** — causes full reprocessing every run
2. **Overusing expectations** — too many `expect_or_fail` can block pipelines; use sparingly
3. **Ignoring schema evolution** — Auto Loader schema inference can break if source changes
4. **No retention policy** — event logs grow indefinitely; set retention
5. **Not testing in dev** — always test pipeline changes before promoting to prod

## DLT vs. Traditional Databricks Jobs

| Aspect                | DLT                                  | Traditional Jobs                         |
|-----------------------|--------------------------------------|------------------------------------------|
| Dependency management | Automatic (DAG)                      | Manual (task dependencies)               |
| Checkpointing         | Automatic                             | Manual (`checkpointLocation`)            |
| Data quality          | Built-in expectations                 | Custom code                              |
| Error recovery        | Automatic retries, state management   | Manual retry logic                       |
| Monitoring            | Rich UI + event log                   | Custom logging                           |
| Schema evolution      | Auto Loader handles                    | Manual schema management                 |
| Code complexity       | Declarative (less code)               | Imperative (more code)                   |
| Flexibility           | Constrained to DLT patterns           | Full Spark API access                    |
| Best for              | ETL pipelines (Bronze→Silver→Gold)    | ML training, ad-hoc processing           |

## Summary

Delta Live Tables represents the **modern, declarative approach** to building data pipelines on Databricks. By abstracting infrastructure management and providing built-in data quality, CDC, and observability, DLT enables data engineers to:

- **Build pipelines faster** with less boilerplate code
- **Ensure data quality** with expectations and quarantine
- **Handle CDC naturally** with APPLY CHANGES and SCD support
- **Scale automatically** with Enhanced Autoscaling
- **Monitor effectively** through the event log and rich UI

For new medallion architecture implementations on Databricks, DLT should be the **default choice** for ETL pipelines, reserving traditional Databricks Jobs for ML workloads, custom processing, and scenarios requiring fine-grained control beyond DLT's declarative model.
