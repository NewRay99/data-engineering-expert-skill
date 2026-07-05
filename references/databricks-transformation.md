# Databricks Transformation Patterns

## Overview

**Databricks** provides the compute and transformation engine for the lakehouse architecture. After Azure Data Factory ingests raw data into the Bronze layer, Databricks handles the heavy lifting: cleansing, conforming, aggregating, and enriching data as it flows through Silver and Gold layers.

This document covers the key transformation patterns, APIs, and best practices for building production-grade data pipelines on Databricks using **PySpark**, **Spark SQL**, and **Delta Lake**.

## Databricks Compute Options

| Compute Type          | Use Case                                          | Auto-Terminate |
|-----------------------|---------------------------------------------------|----------------|
| **All-Purpose Cluster** | Interactive development, notebooks              | Yes (configurable) |
| **Job Cluster**         | Scheduled pipelines, production workloads      | Yes (always)      |
| **SQL Warehouse**       | BI queries, ad-hoc SQL on Delta tables         | Yes (serverless available) |
| **Delta Live Tables**   | Declarative ETL pipelines                       | Managed           |

## Spark APIs for Transformation

### 1. PySpark DataFrame API

The most flexible approach for complex transformations:

```python
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

# Read from Bronze
bronze_orders = spark.read.table("bronze.sap_orders")

# Transform: cleanse, conform, enrich
silver_orders = (
    bronze_orders
    # Drop duplicates
    .dropDuplicates(["order_id", "order_line_id"])
    # Type casting
    .withColumn("order_date", F.to_date("order_date_raw", "yyyy-MM-dd"))
    .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    # Data quality filters
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("amount") > 0)
    # Enrichment
    .withColumn("year", F.year("order_date"))
    .withColumn("month", F.month("order_date"))
    .withColumn("quarter", F.concat(F.lit("Q"), F.quarter("order_date")))
    .withColumn("is_weekend", F.dayofweek("order_date").isin([1, 7]))
    # Audit columns
    .withColumn("_processed_ts", F.current_timestamp())
    .withColumn("_source", F.lit("SAP_ERP"))
)

# Write to Silver (upsert via MERGE)
delta_table = DeltaTable.forName(spark, "silver.orders")

delta_table.alias("target").merge(
    silver_orders.alias("source"),
    """
    target.order_id = source.order_id AND
    target.order_line_id = source.order_line_id
    """
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

### 2. Spark SQL

For analysts and SQL-first engineers. Equivalent transformations:

```sql
-- Silver: Cleansed orders from Bronze
CREATE OR REFRESH STREAMING TABLE silver.orders;

MERGE INTO silver.orders AS target
USING (
  SELECT
    order_id,
    order_line_id,
    TO_DATE(order_date_raw, 'yyyy-MM-dd') AS order_date,
    CAST(amount AS DECIMAL(18,2)) AS amount,
    CAST(quantity AS INT) AS quantity,
    YEAR(order_date) AS order_year,
    MONTH(order_date) AS order_month,
    CURRENT_TIMESTAMP() AS _processed_ts,
    'SAP_ERP' AS _source
  FROM bronze.sap_orders
  WHERE order_id IS NOT NULL AND amount > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, order_line_id ORDER BY _ingest_ts DESC) = 1
) AS source
ON target.order_id = source.order_id AND target.order_line_id = source.order_line_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

### 3. Python UDFs and Pandas UDFs

For transformations not expressible in SQL or built-in functions:

```python
# Standard UDF (slower — JVM serialization overhead)
@udf(returnType=StringType())
def parse_category(code):
    if code is None:
        return "UNKNOWN"
    prefix = code[:2]
    mapping = {"01": "Electronics", "02": "Apparel", "03": "Home"}
    return mapping.get(prefix, "Other")

# Pandas UDF (vectorized — much faster)
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf(StringType())
def parse_category_vectorized(codes: pd.Series) -> pd.Series:
    mapping = {"01": "Electronics", "02": "Apparel", "03": "Home"}
    return codes.str[:2].map(mapping).fillna("Other")

# Apply
silver_df = bronze_df.withColumn("category", parse_category_vectorized(F.col("product_code")))
```

## Core Transformation Patterns

### Pattern 1: UPSERT (MERGE) for Incremental Loading

Delta Lake's `MERGE` operation is the backbone of incremental pipelines:

```python
def upsert_to_delta(micro_batch_df, batch_id):
    if micro_batch_df.isEmpty():
        return

    delta_table = DeltaTable.forName(spark, "silver.orders")

    delta_table.alias("target").merge(
        micro_batch_df.alias("source"),
        "target.order_id = source.order_id AND target.order_line_id = source.order_line_id"
    ).whenMatchedUpdate(
        condition="source._ingest_ts > target._processed_ts",
        set={
            "amount": "source.amount",
            "quantity": "source.quantity",
            "status": "source.status",
            "_processed_ts": "source._ingest_ts"
        }
    ).whenNotMatchedInsertAll() \
     .execute()

# Use in streaming
(spark.readStream
    .option("ignoreChanges", True)
    .table("bronze.sap_orders")
    .writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", "/checkpoints/silver_orders_upsert")
    .trigger(processingTime="5 minutes")
    .start())
```

### Pattern 2: SCD Type 2 (Slowly Changing Dimensions)

Track historical changes to dimension attributes:

```sql
-- Silver: Customer dimension with SCD2
CREATE TABLE silver.dim_customer_scd2 (
    customer_key    BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_id     STRING NOT NULL,
    customer_name   STRING,
    email           STRING,
    phone           STRING,
    address         STRING,
    region          STRING,
    segment         STRING,
    effective_from  TIMESTAMP NOT NULL,
    effective_to    TIMESTAMP,
    is_current      BOOLEAN NOT NULL,
    _processed_ts   TIMESTAMP
)
USING DELTA
PARTITIONED BY (is_current);

-- MERGE logic for SCD2
MERGE INTO silver.dim_customer_scd2 AS target
USING (
  SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.phone,
    c.address,
    c.region,
    c.segment,
    c._ingest_ts AS effective_from
  FROM bronze.crm_customers c
  WHERE c._ingest_date = CURRENT_DATE()
) AS source
ON target.customer_id = source.customer_id AND target.is_current = true
WHEN MATCHED AND
    target.customer_name != source.customer_name OR
    target.email != source.email OR
    target.phone != source.phone OR
    target.address != source.address OR
    target.region != source.region OR
    target.segment != source.segment
THEN UPDATE SET
    effective_to = source.effective_from,
    is_current = false
WHEN NOT MATCHED THEN INSERT (
    customer_id, customer_name, email, phone, address, region, segment,
    effective_from, effective_to, is_current, _processed_ts
) VALUES (
    source.customer_id, source.customer_name, source.email, source.phone,
    source.address, source.region, source.segment,
    source.effective_from, NULL, true, CURRENT_TIMESTAMP()
);
```

### Pattern 3: Change Data Capture (CDC) with CDF

Delta Lake's **Change Data Feed (CDF)** enables efficient CDC:

```python
# Enable CDF on a table
spark.sql("""
    ALTER TABLE bronze.orders
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Read changes since a specific version
changes_df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 10) \
    .table("bronze.orders")

# changes_df contains: _change_type (insert/update_preimage/update_postimage/delete), _commit_version, _commit_timestamp

# Filter for updates and inserts only
inserts_and_updates = changes_df.filter("_change_type IN ('insert', 'update_postimage')")

# Apply to Silver
inserts_and_updates.write.format("delta").mode("append").saveAsTable("silver.orders")
```

### Pattern 4: Aggregation and Window Functions

Building Gold-layer analytics tables:

```python
from pyspark.sql import Window

# Gold: Monthly revenue by region and product category
gold_monthly_revenue = (
    spark.read.table("silver.orders")
    .join(spark.read.table("silver.dim_product"), "product_id", "left")
    .join(spark.read.table("silver.dim_customer"), "customer_id", "left")
    .filter(F.col("order_date").between("2024-01-01", "2024-12-31"))
    .groupBy(
        F.date_trunc("month", "order_date").alias("month"),
        "region",
        "product_category"
    )
    .agg(
        F.sum("amount").alias("total_revenue"),
        F.countDistinct("order_id").alias("order_count"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.avg("amount").alias("avg_order_value"),
        F.sum(F.when(F.col("is_weekend"), F.col("amount")).otherwise(0)).alias("weekend_revenue")
    )
    .withColumn("revenue_rank",
        F.rank().over(Window.partitionBy("month").orderBy(F.desc("total_revenue")))
    )
    .withColumn("mom_growth",
        (F.col("total_revenue") - F.lag("total_revenue").over(
            Window.partitionBy("region", "product_category").orderBy("month")
        )) / F.lag("total_revenue").over(
            Window.partitionBy("region", "product_category").orderBy("month")
        )
    )
)

# Write to Gold
(gold_monthly_revenue.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.monthly_revenue"))
```

### Pattern 5: Pivot and Unpivot

```python
# Pivot: Rows to columns
pivoted = (
    spark.read.table("silver.orders")
    .groupBy("customer_id")
    .pivot("product_category")
    .agg(F.sum("amount"))
)
# Result: customer_id | Electronics | Apparel | Home | ...

# Unpivot: Columns to rows
from pyspark.sql import functions as F

unpivoted = (
    spark.read.table("gold.wide_metrics")
    .select(
        "date",
        F.expr("stack(3, 'revenue', revenue, 'orders', orders, 'customers', customers)").alias("metric", "value")
    )
)
```

### Pattern 6: Handling Late-Arriving Data

```python
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Handle late-arriving events in Silver
def handle_late_arrivals(table_name, watermark_column, expected_date):
    delta_table = DeltaTable.forName(spark, table_name)

    # Find records that arrived late (ingested after expected date)
    late_records = (
        spark.read.table(table_name)
        .filter(
            (F.col(watermark_column) < F.lit(expected_date)) &
            (F.col("_processed_ts") > F.lit(expected_date))
        )
    )

    if late_records.count() > 0:
        print(f"Found {late_records.count()} late-arriving records")
        # Re-process affected Gold aggregates
        # ... trigger Gold refresh for affected partitions

    return late_records
```

## Streaming Transformations

### Structured Streaming Basics

```python
# Read stream from Bronze
stream_df = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 100)
    .table("bronze.clickstream")
)

# Transform in flight
transformed = (
    stream_df
    .withWatermark("event_time", "10 minutes")
    .filter(F.col("event_type").isNotNull())
    .withColumn("session_window",
        F.window("event_time", "30 minutes", "10 minutes"))
    .groupBy("session_window", "user_id", "event_type")
    .count()
    .withColumnRenamed("count", "event_count")
)

# Write stream to Silver
query = (
    transformed.writeStream
    .format("delta")
    .outputMode("update")  # 'append' for non-aggregations
    .option("checkpointLocation", "/checkpoints/silver_clickstream_agg")
    .trigger(processingTime="2 minutes")
    .toTable("silver.clickstream_session_agg")
)

query.awaitTermination()
```

### Stream-Static Joins

Join streaming data with static dimension tables:

```python
# Stream-static join: enrich events with customer info
enriched_stream = (
    spark.readStream.table("bronze.events")
    .join(
        spark.read.table("silver.dim_customer"),  # static
        "customer_id",
        "left"
    )
    .select(
        "event_id",
        "customer_id",
        "customer_name",
        "region",
        "event_type",
        "event_time"
    )
)

(enriched_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/silver_enriched_events")
    .toTable("silver.events_enriched"))
```

### Stream-Stream Joins

```python
# Join two streams with watermarks
impressions = (
    spark.readStream.table("bronze.impressions")
    .withWatermark("imp_time", "2 hours")
)

clicks = (
    spark.readStream.table("bronze.clicks")
    .withWatermark("click_time", "3 hours")
)

joined = impressions.join(
    clicks,
    expr("""
        imp_id = click_imp_id AND
        click_time >= imp_time AND
        click_time <= imp_time + interval 1 hour
    """),
    "leftOuter"
)
```

## Delta Lake Optimization

### Z-ORDER

Optimize data layout for frequently filtered columns:

```sql
-- Optimize Gold table for common filter patterns
OPTIMIZE gold.sales_monthly_revenue
ZORDER BY (region, product_category, month);

-- Schedule after data loads
-- Use ALTER TABLE to set ZORDER columns for auto-optimization
ALTER TABLE gold.sales_monthly_revenue
SET TBLPROPERTIES (
    'delta.dataSkippingNumIndexedCols' = 10,
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
);
```

### Liquid Clustering

The next-generation clustering (replaces Z-ORDER + partitioning):

```sql
CREATE TABLE gold.customer_analytics
USING DELTA
CLUSTER BY (customer_id, activity_date);

-- Liquid clustering is self-tuning — no manual OPTIMIZE needed
-- Writes automatically cluster data
```

### OPTIMIZE and VACUUM

```python
# Schedule regular maintenance
def maintain_delta_table(table_name):
    spark.sql(f"OPTIMIZE {table_name}")  # Compact small files
    spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS")  # Remove old versions (7 days)
    print(f"Maintenance complete for {table_name}")

# Run weekly via Databricks Jobs
maintain_delta_table("silver.orders")
maintain_delta_table("gold.monthly_revenue")
```

### Performance Tuning

```python
# Spark configuration for production pipelines
spark.conf.set("spark.sql.shuffle.partitions", 200)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.sql.files.maxPartitionBytes", "128MB")
spark.conf.set("spark.sql.streaming.stopTimeout", "60000")
```

## Error Handling and Data Quality

### Data Quality with Delta Expectations

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col

# Define quality checks
def validate_and_load(df, table_name, rules):
    """
    rules: list of (name, condition, action)
    action: 'fail', 'drop', or 'quarantine'
    """
    valid_df = df
    quarantine_conditions = []
    for name, condition, action in rules:
        if action == "fail":
            # Count violations
            violations = df.filter(~F.expr(condition))
            if violations.count() > 0:
                raise ValueError(f"Data quality rule '{name}' failed: {violations.count()} violations")
        elif action == "drop":
            valid_df = valid_df.filter(F.expr(condition))
        elif action == "quarantine":
            valid_df = valid_df.filter(F.expr(condition))
            quarantine_conditions.append(condition)

    # Write valid records
    valid_df.write.format("delta").mode("append").saveAsTable(table_name)

    # Write quarantined records
    if quarantine_conditions:
        quarantine_df = df.filter(~reduce(lambda a, b: a | b, [F.expr(c) for c in quarantine_conditions]))
        quarantine_df.write.format("delta").mode("append").saveAsTable(f"{table_name}_quarantine")

    return valid_df.count()

# Usage
rules = [
    ("order_id_not_null", "order_id IS NOT NULL", "fail"),
    ("amount_positive", "amount > 0", "drop"),
    ("valid_email", "email RLIKE '^[^@]+@[^@]+\\.[^@]+$'", "quarantine"),
]

validate_and_load(silver_df, "silver.orders_validated", rules)
```

### Try-Except Pattern for Pipeline Resilience

```python
import traceback

def run_transformation(step_name, func, *args, **kwargs):
    """Execute a transformation step with error handling and logging."""
    try:
        print(f"[START] {step_name}")
        result = func(*args, **kwargs)
        print(f"[SUCCESS] {step_name}: {result}")
        return {"status": "success", "step": step_name, "result": result}
    except Exception as e:
        error_msg = traceback.format_exc()
        print(f"[FAILED] {step_name}: {error_msg}")
        # Send alert
        # send_alert(step_name, error_msg)
        raise

# Usage
run_transformation("bronze_to_silver_orders", transform_bronze_to_silver, "orders")
run_transformation("silver_to_gold_revenue", build_gold_revenue)
```

## Unity Catalog Integration

```python
# Use Unity Catalog for governance
spark.sql("USE CATALOG enterprise_prod")
spark.sql("USE SCHEMA silver")

# Apply column-level masking
spark.sql("""
    CREATE FUNCTION mask_email(email STRING)
    RETURNS STRING
    RETURN CONCAT(SUBSTRING(email, 1, 2), '****', '@', SPLIT_PART(email, '@', 2));

    ALTER TABLE silver.dim_customer
    ALTER COLUMN email SET MASK mask_email;
""")

# Apply row-level security
spark.sql("""
    CREATE FUNCTION region_filter(region STRING)
    RETURNS BOOLEAN
    RETURN is_member('region_' || region) OR is_member('admin_group');

    ALTER TABLE gold.sales_monthly_revenue
    SET ROW FILTER region_filter ON (region);
""")

# Tag PII columns
spark.sql("ALTER TABLE silver.dim_customer ALTER COLUMN email SET TAGS ('PII' = 'true')")
spark.sql("ALTER TABLE silver.dim_customer ALTER COLUMN phone SET TAGS ('PII' = 'true')")
```

## Testing and CI/CD

### Unit Testing Transformations

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("tests").getOrCreate()

def test_deduplication(spark):
    from transformations import deduplicate_orders

    test_data = [
        ("ORD001", 1, 100.00, "2024-01-01"),
        ("ORD001", 1, 100.00, "2024-01-01"),  # duplicate
        ("ORD002", 1, 200.00, "2024-01-02"),
    ]
    schema = "order_id STRING, order_line_id INT, amount DOUBLE, order_date STRING"
    df = spark.createDataFrame(test_data, schema)

    result = deduplicate_orders(df)
    assert result.count() == 2  # One duplicate removed

def test_amount_validation(spark):
    from transformations import filter_invalid_amounts

    test_data = [
        ("ORD001", 100.00),
        ("ORD002", -50.00),   # invalid
        ("ORD003", 0.00),     # invalid
        ("ORD004", None),     # invalid
    ]
    schema = "order_id STRING, amount DOUBLE"
    df = spark.createDataFrame(test_data, schema)

    result = filter_invalid_amounts(df)
    assert result.count() == 1  # Only ORD001 is valid
```

### Databricks Asset Bundles (DABs) for Deployment

```yaml
# databricks.yml
bundle:
  name: sales_pipeline

variables:
  env:
    description: Environment
    default: dev

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-dev.azuredatabricks.net
    variables:
      env: dev

  prod:
    mode: production
    workspace:
      host: https://adb-prod.azuredatabricks.net
    variables:
      env: prod

resources:
  jobs:
    sales_medallion_pipeline:
      name: "Sales Medallion Pipeline (${var.env})"
      tasks:
        - task_key: bronze_to_silver
          notebook_task:
            notebook_path: /pipelines/bronze_to_silver
            base_parameters:
              env: ${var.env}
          job_cluster:
            new_cluster:
              spark_version: "14.3.x-scala2.12"
              node_type_id: "Standard_DS3_v2"
              num_workers: 4
              autoscale:
                min_workers: 2
                max_workers: 8
        - task_key: silver_to_gold
          depends_on:
            - task_key: bronze_to_silver
          notebook_task:
            notebook_path: /pipelines/silver_to_gold
            base_parameters:
              env: ${var.env}
          job_cluster:
            new_cluster:
              spark_version: "14.3.x-scala2.12"
              node_type_id: "Standard_DS3_v2"
              num_workers: 4
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: UTC
```

## Best Practices Summary

| Area                | Best Practice                                          |
|---------------------|-------------------------------------------------------|
| **Idempotency**     | Always use MERGE, never blind append for Silver/Gold  |
| **Partitioning**    | Partition by date, avoid over-partitioning (>10k parts)|
| **Small files**     | Enable auto-compaction, run OPTIMIZE regularly         |
| **Schema**          | Enforce at Silver, lock at Gold                        |
| **Streaming**       | Always set checkpoint locations                        |
| **Watermarks**      | Set appropriate watermarks to manage state size         |
| **UDFs**            | Prefer built-in functions; use Pandas UDFs if needed   |
| **Joins**           | Broadcast small tables, enable AQE                     |
| **Testing**         | Unit test transformations, integration test pipelines  |
| **Monitoring**      | Log to audit tables, use Databricks SQL alerts         |
| **Security**         | Unity Catalog, column masking, row filters             |

## Summary

Databricks provides a comprehensive platform for transforming data within the medallion architecture. The combination of PySpark, Spark SQL, Delta Lake, and Unity Catalog enables:
- **Flexible transformations** from simple SQL to complex Python logic
- **Scalable streaming and batch** processing within a single engine
- **ACID compliance and time travel** via Delta Lake
- **Governance and security** through Unity Catalog
- **CI/CD** via Databricks Asset Bundles and Git integration

The key to success is choosing the right abstraction (SQL vs. PySpark vs. DLT) for each layer and leveraging Delta Lake's optimization features (Z-ORDER, Liquid Clustering, CDF) to ensure performant, production-grade pipelines.
