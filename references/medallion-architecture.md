# Medallion Architecture

## Overview

The **Medallion Architecture** (also known as "Multi-Hop Architecture") is a data design pattern used in Databricks and Azure Databricks platforms to logically organize data in a lakehouse. The architecture uses a layered, progressive approach to data transformation, where data quality and structure improve as it flows through each layer — from raw ingestion to refined, analytics-ready datasets.

The name "medallion" derives from the Olympic medal hierarchy: **Bronze**, **Silver**, and **Gold**, each representing increasing data quality and refinement.

## The Three Layers

### Bronze Layer (Raw Data)

The Bronze layer is the entry point for all data into the lakehouse. It stores raw, unprocessed data ingested from various source systems — operational databases, APIs, streaming sources, flat files, logs, etc.

**Key characteristics:**
- **Schema-on-read** — data is stored as-is from the source
- **Append-only** — full history is preserved; no updates or deletes
- **Delta format** — stored as Delta Lake tables for ACID compliance
- **Minimal transformation** — only basic metadata (ingestion timestamp, source file name) is added
- **Schema enforcement** optional; schema evolution enabled
- **Near 1:1 mapping** with source system structures

**Bronze layer objectives:**
- Capture data exactly as it arrives from source
- Maintain a complete historical record
- Enable reprocessing if downstream logic changes
- Support incremental (CDC) and batch ingestion patterns
- Serve as a recovery/replay point

**Typical table naming convention:**
```
bronze.<source_system>_<entity>
```
Examples: `bronze.sap_customers`, `bronze.crm_orders_raw`, `bronze.kafka_clickstream`

### Silver Layer (Cleansed and Conformed Data)

The Silver layer represents cleansed, filtered, and conformed data. Here, raw Bronze data is transformed into a more structured, normalized form suitable for analytics and data science.

**Key characteristics:**
- **Schema enforcement** — strict schema applied
- **Cleansed** — duplicates removed, nulls handled, data types standardized
- **Conformed** — common business keys and dimensions applied
- **Filtered** — only relevant data retained
- **Normalized** — data modeled to 3NF or star schema principles
- **Deduplicated** — surrogate keys assigned
- **SCD Type 2** — slowly changing dimension tracking applied

**Silver layer objectives:**
- Provide a single source of truth for enterprise data
- Enable cross-source joins and integration
- Support governance, data quality, and lineage
- Feed both Gold layer and direct ad-hoc analysis
- Serve as the "conformed" enterprise data model

**Typical table naming convention:**
```
silver.<domain>_<entity>
```
Examples: `silver.sales_orders`, `silver.dim_customer`, `silver.fact_transaction`

### Gold Layer (Curated Analytics Layer)

The Gold layer is the final, presentation-level tier optimized for reporting, BI dashboards, ML models, and business consumption. Data here is aggregated, enriched, and shaped for specific business use cases.

**Key characteristics:**
- **Project/department-level** — organized by business domain or use case
- **Aggregated** — pre-computed KPIs and metrics
- **Denormalized** — star schema or flat tables for query performance
- **Optimized** — tuned for specific query patterns (Z-ORDER, liquid clustering)
- **Access-controlled** — row/column-level security applied
- **Consumption-ready** — exposed via SQL warehouses, BI tools, APIs

**Gold layer objectives:**
- Power BI dashboards and enterprise reporting
- Feed ML feature stores and training pipelines
- Provide data marts for specific departments
- Support real-time and batch consumption
- Enforce data masking and access policies

**Typical table naming convention:**
```
gold.<project>_<aggregate>
```
Examples: `gold.sales_monthly_revenue`, `gold.exec_dashboard_summary`, `gold.churn_model_features`

## Architecture Diagram (Conceptual)

```
  Source Systems          Bronze Layer          Silver Layer          Gold Layer
  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
  │  SAP ERP     │─────▶│  bronze.sap_ │─────▶│  silver.dim_ │─────▶│  gold.sales_ │
  │              │      │  materials   │      │  product     │      │  dashboard   │
  ├──────────────┤      ├──────────────┤      ├──────────────┤      ├──────────────┤
  │  Salesforce  │─────▶│  bronze.crm_ │─────▶│  silver.dim_ │─────▶│  gold.crm_   │
  │              │      │  accounts    │      │  customer    │      │  exec_kpis   │
  ├──────────────┤      ├──────────────┤      ├──────────────┤      ├──────────────┤
  │  Kafka Stream│─────▶│  bronze.kafka│─────▶│  silver.fact_│─────▶│  gold.ml_    │
  │              │      │  _events     │      │  events      │      │  features    │
  └──────────────┘      └──────────────┘      └──────────────┘      └──────────────┘
         │                     │                     │                     │
    Ingestion              Raw Storage         Cleansed/Conformed     Curated/Aggregated
```

## Design Principles

### 1. Idempotency

Every pipeline run must produce the same result regardless of how many times it executes. Use MERGE operations and checkpointing to ensure reprocessing does not duplicate data.

### 2. Incremental Processing

Use Delta Lake's CDC (Change Data Capture) and structured streaming to process only new or changed data rather than full reloads:

```python
# Bronze: Append-only incremental load from streaming source
(df.writeStream
    .format("delta")
    .option("checkpointLocation", "/delta/events/_checkpoints/bronze")
    .outputMode("append")
    .trigger(processingTime="1 minute")
    .toTable("bronze.kafka_clickstream"))

# Silver: Upsert using MERGE with micro-batches
@dlt.table
def silver_events():
    return (
        spark.readStream
        .option("ignoreChanges", True)
        .table("bronze.kafka_clickstream")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn("event_date", F.to_date("event_time"))
        .dropDuplicates(["event_id"])
    )
```

### 3. Schema Management

- **Bronze**: `mergeSchema` enabled for flexibility
- **Silver**: Schema enforced (`AUTO MERGE` disabled)
- **Gold**: Locked schema with explicit DDL

### 4. Partitioning Strategy

Avoid over-partitioning. Use partition pruning on high-cardinality-but-low-value-count columns, or rely on **Liquid Clustering** (Databricks recommendation for new tables):

```sql
-- Bronze: Partition by ingestion date
CREATE TABLE bronze.sales_orders (
  order_id STRING,
  order_data STRING,
  _ingest_ts TIMESTAMP,
  _source_file STRING
)
USING DELTA
PARTITIONED BY (DATE(_ingest_ts));

-- Gold: Use Liquid Clustering (no manual partitioning needed)
CREATE TABLE gold.sales_monthly_revenue
USING DELTA
CLUSTER BY (region, product_category);
```

### 5. Data Quality Enforcement

| Layer  | Quality Level        | Mechanism                                  |
|--------|---------------------|--------------------------------------------|
| Bronze | Pass-through         | Schema on read, capture all                |
| Silver | Validated + Cleansed | Constraints, expectations, deduplication   |
| Gold   | Certified            | Data quality rules, freshness SLAs, review |

## Implementation Patterns

### Pattern 1: Batch Medallion with Azure Data Factory + Databricks

```
ADF Pipeline (trigger)
  → Databricks Notebook Activity (Bronze load)
  → Databricks Notebook Activity (Bronze → Silver)
  → Databricks Notebook Activity (Silver → Gold)
  → Data Quality Checks
  → Notification
```

### Pattern 2: Streaming Medallion with Delta Live Tables (DLT)

```sql
-- DLT pipeline definition
CREATE OR REFRESH STREAMING LIVE TABLE bronze_clickstream
COMMENT "Raw clickstream events from Kafka"
AS SELECT * FROM cloud_files("/mnt/kafka-events/", "json");

CREATE OR REFRESH STREAMING LIVE TABLE silver_clickstream
COMMENT "Cleansed and deduplicated events"
AS
  SELECT
    event_id,
    user_id,
    event_type,
    to_timestamp(event_ts) AS event_time,
    current_timestamp() AS _processed_ts
  FROM STREAM(live.bronze_clickstream)
  WHERE event_id IS NOT NULL;

CREATE OR REFRESH LIVE TABLE gold_daily_user_activity
COMMENT "Daily user activity aggregation"
AS
  SELECT
    date(event_time) AS activity_date,
    user_id,
    count(*) AS event_count,
    count(DISTINCT event_type) AS unique_event_types
  FROM live.silver_clickstream
  GROUP BY date(event_time), user_id;
```

### Pattern 3: Multi-Source Convergence

When the same entity exists across multiple source systems (e.g., customer in SAP, Salesforce, and a legacy DB), the Silver layer provides a unified, conformed dimension:

```sql
-- Silver: Unified customer dimension with SCD2
CREATE OR REFRESH STREAMING TABLE silver.dim_customer;

APPLY CHANGES INTO live.silver.dim_customer
FROM STREAM(live.bronze.sap_customers)
KEYS (customer_id)
SEQUENCE BY _ingest_ts
COLUMNS * EXCEPT (_ingest_ts, _source_file)
STORED AS SCD TYPE 2;
```

## Best Practices

### Storage and Performance

- **Use Unity Catalog** for centralized governance across all three layers
- **Optimize Gold tables** with Z-ORDER or Liquid Clustering on common filter columns
- **Vacuum regularly** but conservatively (retain at least 7 days of history)
- **Use OPTIMIZE** to compact small files — schedule after peak ingestion windows
- **Monitor Delta transaction logs** — large logs degrade performance

### Governance

- **Catalog structure**: `bronze_catalog`, `silver_catalog`, `gold_catalog` (or single catalog with schemas)
- **Access control**: Bronze is restricted to data engineers; Gold is available to analysts
- **Lineage tracking**: Unity Catalog automatically tracks column-level lineage
- **Tagging**: Apply PII tags at Silver; mask at Gold

### Naming and Organization

```
Catalog: enterprise_prod
  Schema: bronze
    Tables: sap_<entity>, crm_<entity>, kafka_<topic>, api_<source>
  Schema: silver
    Tables: dim_<entity>, fact_<event>, bridge_<relation>
  Schema: gold
    Tables: <domain>_<aggregate>, <project>_<view>, <department>_<report>
```

### Error Handling

- **Quarantine tables** for records that fail quality checks in Silver
- **Dead letter queues** for streaming pipelines
- **Retry policies** in ADF / DLT with exponential backoff
- **Alerting** via Databricks SQL alerts or Azure Monitor

```python
# Example: Quarantine pattern in Silver layer
from pyspark.sql.functions import col

silver_df = spark.readStream.table("bronze.orders")

valid_df = silver_df.filter(col("order_id").isNotNull() & col("customer_id").isNotNull())
invalid_df = silver_df.filter(col("order_id").isNull() | col("customer_id").isNull())

# Write valid records to Silver
(valid_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/silver_orders_valid")
    .toTable("silver.orders"))

# Write invalid records to quarantine
(invalid_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/silver_orders_quarantine")
    .toTable("silver.orders_quarantine"))
```

## Comparison: Medallion vs. Traditional Data Warehouse

| Aspect                | Medallion (Lakehouse)              | Traditional DW                    |
|----------------------|-------------------------------------|-----------------------------------|
| Storage format       | Delta (open format)                | Proprietary (vendor-specific)     |
| Compute               | Spark / SQL Warehouse (decoupled)  | Tightly coupled to storage         |
| Schema flexibility    | Schema-on-read at Bronze           | Schema-on-write, rigid             |
| Cost model           | Pay-per-use compute                | Provisioned, expensive             |
| AI/ML integration    | Native (MLflow, feature store)     | Requires export to ML platform     |
| Streaming support    | Native (structured streaming, DLT) | Bolted-on                          |
| Open format           | Yes (Parquet + transaction log)    | No                                 |

## Common Pitfalls

1. **Skipping Bronze** — going directly to Silver loses auditability and replay capability
2. **Over-transforming at Bronze** — adds coupling and reduces flexibility
3. **Under-transforming at Silver** — pushes complexity to Gold, creating multiple "truths"
4. **Ignoring partition strategy** — leads to small file problems and slow queries
5. **No data quality checks** — trust erodes; always enforce expectations
6. **Tight coupling between Gold and specific BI tools** — Gold should be tool-agnostic

## Integration with Databricks Tools

| Tool                  | Role in Medallion Architecture                     |
|----------------------|-----------------------------------------------------|
| Delta Live Tables     | Declarative Bronze→Silver→Gold pipelines           |
| Databricks SQL        | Gold layer consumption, ad-hoc analysis            |
| MLflow                | Feature engineering on Gold, model tracking         |
| Unity Catalog         | Governance, lineage, access control across layers  |
| Delta Sharing         | Externally share Gold tables without copying        |
| Workflows / Jobs      | Orchestrate batch pipelines between layers          |
| Photon Engine         | Accelerate Gold-layer queries                       |

## Summary

The Medallion Architecture provides a pragmatic, scalable framework for building lakehouse data platforms on Databricks. Its layered approach balances:
- **Raw fidelity** (Bronze) for auditability and replay
- **Conformed integration** (Silver) for enterprise consistency
- **Curated delivery** (Gold) for business value

By leveraging Delta Lake's ACID transactions, schema enforcement, and time travel, combined with Unity Catalog governance and DLT orchestration, organizations can build production-grade data platforms that serve both analytics and AI/ML workloads from a single copy of data.
