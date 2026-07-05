# Metadata-Driven Solutions

## Overview

Metadata-driven architecture (MDA) is the practice of externalising pipeline configuration — source connections, column mappings, transformation logic, and data-quality rules — into structured metadata tables rather than hard-coding them in pipeline code. The runtime engine reads the metadata, interprets it, and executes the appropriate operations dynamically. The result: **adding a new data source becomes a metadata-insert exercise, not a code-change exercise.**

This document covers:
- Metadata table schemas and DDL
- Azure Data Factory parameterisation patterns
- Databricks parameterisation patterns
- YAML configuration format for source-controlled metadata
- Benefits, trade-offs, and governance considerations

---

## 1. Metadata Table Schemas

The metadata layer consists of four core tables. Each table serves a single responsibility and is designed for both human readability and programmatic consumption.

### 1.1 `source_config`

Stores one row per data source (e.g., a source table in an ERP database, an API endpoint, a file drop location).

```sql
CREATE TABLE metadata.source_config (
    source_id              INT IDENTITY(1,1) PRIMARY KEY,
    source_name            NVARCHAR(200)   NOT NULL,
    source_type            NVARCHAR(50)    NOT NULL,   -- 'Database', 'API', 'File', 'CDC'
    source_system          NVARCHAR(100)   NOT NULL,   -- 'SAP', 'Salesforce', 'Kafka', etc.
    connection_string_key  NVARCHAR(200)   NOT NULL,   -- Key Vault reference name
    schema_name            NVARCHAR(100),
    object_name            NVARCHAR(200)   NOT NULL,   -- Table name, endpoint path, file pattern
    ingestion_mode         NVARCHAR(50)    NOT NULL,   -- 'FullLoad', 'Incremental', 'CDC'
    incremental_column     NVARCHAR(100),              -- For incremental loads
    watermark_value        NVARCHAR(100),              -- Last processed value
    target_layer           NVARCHAR(50)    NOT NULL,   -- 'bronze', 'silver', 'gold'
    target_table           NVARCHAR(200)   NOT NULL,
    is_active              BIT             DEFAULT 1,
    load_frequency         NVARCHAR(50),               -- 'Hourly', 'Daily', 'Weekly'
    priority               INT             DEFAULT 5,  -- 1=high, 10=low
    created_date           DATETIME2       DEFAULT SYSUTCDATETIME(),
    updated_date           DATETIME2       DEFAULT SYSUTCDATETIME(),
    updated_by             NVARCHAR(100),
    CONSTRAINT uq_source_name UNIQUE (source_name)
);
```

### 1.2 `column_mapping`

Stores one row per column mapping between source and target. Multiple rows share the same `source_id` foreign key.

```sql
CREATE TABLE metadata.column_mapping (
    mapping_id          INT IDENTITY(1,1) PRIMARY KEY,
    source_id           INT             NOT NULL,
    source_column       NVARCHAR(200)   NOT NULL,
    source_data_type    NVARCHAR(100)   NOT NULL,
    target_column       NVARCHAR(200)   NOT NULL,
    target_data_type    NVARCHAR(100)   NOT NULL,
    is_nullable         BIT             DEFAULT 1,
    default_value       NVARCHAR(500),
    transformation_id   INT,                        -- FK to transformation_rules
    column_order        INT             NOT NULL,
    is_active           BIT             DEFAULT 1,
    CONSTRAINT fk_colmap_source FOREIGN KEY (source_id)
        REFERENCES metadata.source_config(source_id),
    CONSTRAINT fk_colmap_transform FOREIGN KEY (transformation_id)
        REFERENCES metadata.transformation_rules(transformation_id)
);

CREATE INDEX ix_colmap_source_id ON metadata.column_mapping(source_id)
    WHERE is_active = 1;
```

### 1.3 `dq_rules`

Stores data-quality rules evaluated against ingested data. Rules can be column-level or table-level.

```sql
CREATE TABLE metadata.dq_rules (
    rule_id             INT IDENTITY(1,1) PRIMARY KEY,
    source_id           INT             NOT NULL,
    rule_name           NVARCHAR(200)   NOT NULL,
    rule_type           NVARCHAR(50)    NOT NULL,   -- 'NOT_NULL', 'UNIQUE', 'RANGE',
                                                   -- 'REGEX', 'REFERENTIAL', 'CUSTOM_SQL'
    rule_expression     NVARCHAR(MAX)   NOT NULL,   -- SQL fragment or regex pattern
    column_name         NVARCHAR(200),              -- NULL for table-level rules
    severity            NVARCHAR(20)    NOT NULL,   -- 'ERROR', 'WARN', 'INFO'
    action_on_failure   NVARCHAR(50)    NOT NULL,   -- 'FAIL_PIPELINE', 'LOG_ONLY',
                                                   -- 'QUARANTINE', 'ALERT'
    threshold_pct       DECIMAL(5,2)    DEFAULT 0,  -- Allowable failure % (e.g., 0.01)
    is_active           BIT             DEFAULT 1,
    CONSTRAINT fk_dq_source FOREIGN KEY (source_id)
        REFERENCES metadata.source_config(source_id)
);
```

### 1.4 `transformation_rules`

Stores named transformation logic that can be referenced by column mappings or applied at the pipeline level.

```sql
CREATE TABLE metadata.transformation_rules (
    transformation_id       INT IDENTITY(1,1) PRIMARY KEY,
    transformation_name     NVARCHAR(200)   NOT NULL,
    transformation_type     NVARCHAR(50)    NOT NULL,   -- 'CAST', 'DERIVED', 'LOOKUP',
                                                       -- 'SPLIT', 'CONCAT', 'DATE_FORMAT',
                                                       -- 'CUSTOM_SQL'
    expression_template     NVARCHAR(MAX)   NOT NULL,   -- Templated with {source_column}
    description             NVARCHAR(500),
    is_active               BIT             DEFAULT 1,
    CONSTRAINT uq_transform_name UNIQUE (transformation_name)
);
```

**Example transformation rows:**

```sql
INSERT INTO metadata.transformation_rules
    (transformation_name, transformation_type, expression_template, description)
VALUES
    ('ToUpper', 'CAST', 'UPPER({source_column})', 'Convert string to uppercase'),
    ('TrimSpaces', 'DERIVED', 'TRIM({source_column})', 'Trim leading/trailing whitespace'),
    ('EpochToTimestamp', 'DATE_FORMAT',
     'TIMESTAMP_MILLIS(CAST({source_column} AS BIGINT))',
     'Convert epoch milliseconds to timestamp'),
    ('HashKey', 'DERIVED',
     'SHA2(CONCAT_WS(||, {source_column}, COALESCE({source_column}, '''')), 256)',
     'Generate SHA-256 hash surrogate key');
```

---

## 2. Azure Data Factory Parameterisation

### 2.1 Architecture Pattern

The standard metadata-driven ADF pipeline follows this flow:

```
[Lookup: Get Active Sources]
        │
        ▼
[ForEach: source]
   ├── Lookup: Get Column Mappings for source
   ├── Lookup: Get DQ Rules for source
   ├── Copy Activity: Source → Sink (dynamic datasets)
   ├── Databricks Notebook: Apply transformations + DQ
   └── Sql StoredProcedure: Update Watermark
```

### 2.2 Lookup Activity — Get Active Sources

```json
{
  "name": "GetActiveSources",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "AzureSqlSource",
      "sqlReaderQuery": {
        "value": "SELECT * FROM metadata.source_config WHERE is_active = 1 AND load_frequency = '@{pipeline().parameters.LoadFrequency}' ORDER BY priority",
        "type": "Expression"
      }
    },
    "dataset": {
      "referenceName": "Ds_MetadataDb",
      "type": "DatasetReference"
    },
    "firstRowOnly": false
  }
}
```

### 2.3 ForEach Activity

```json
{
  "name": "ForEachSource",
  "type": "ForEach",
  "typeProperties": {
    "items": {
      "value": "@activity('GetActiveSources').output.value",
      "type": "Expression"
    },
    "isSequential": false,
    "batchCount": 5,
    "activities": [
      {
        "name": "CopySourceToBronze",
        "type": "Copy",
        "typeProperties": {
          "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": {
              "value": "SELECT @{item().column_list} FROM @{item().schema_name}.@{item().object_name} WHERE @{item().incremental_column} > '@{item().watermark_value}'",
              "type": "Expression"
            }
          },
          "sink": {
            "type": "ParquetSink",
            "storeSettings": {
              "type": "AzureBlobFSWriteSettings",
              "fileName": {
                "value": "@{item().target_table}_@{formatDateTime(utcnow(), 'yyyyMMddHHmmss')}.parquet",
                "type": "Expression"
              },
              "folderPath": {
                "value": "@{pipeline().parameters.BronzeContainer}/@{item().target_layer}/@{item().target_table}/",
                "type": "Expression"
              }
            }
          }
        },
        "inputs": [
          { "referenceName": "Ds_DynamicSource", "type": "DatasetReference",
            "parameters": {
              "ConnectionStringKey": "@item().connection_string_key",
              "SchemaName": "@item().schema_name",
              "TableName": "@item().object_name"
            }
          }
        ],
        "outputs": [
          { "referenceName": "Ds_DynamicParquetSink", "type": "DatasetReference",
            "parameters": {
              "FolderPath": "@{pipeline().parameters.BronzeContainer}/@{item().target_layer}/@{item().target_table}/",
              "FileName": "@{item().target_table}_@{formatDateTime(utcnow(), 'yyyyMMddHHmmss')}.parquet"
            }
          }
        ]
      }
    ]
  }
}
```

### 2.4 Dynamic Datasets

The key to metadata-driven ADF is **parameterised datasets**. Instead of creating one dataset per source table, you create a single generic dataset that accepts parameters.

**Generic Azure SQL Source Dataset:**

```json
{
  "name": "Ds_DynamicSource",
  "properties": {
    "type": "AzureSqlTable",
    "typeProperties": {
      "schema": { "value": "@dataset().SchemaName", "type": "Expression" },
      "table": { "value": "@dataset().TableName", "type": "Expression" }
    },
    "parameters": {
      "SchemaName": { "type": "String" },
      "TableName": { "type": "String" },
      "ConnectionStringKey": { "type": "String" }
    },
    "linkedService": {
      "referenceName": "Ls_KeyVaultSql",
      "type": "LinkedServiceReference",
      "parameters": {
        "ConnectionStringSecret": "@dataset().ConnectionStringKey"
      }
    }
  }
}
```

**Generic Parquet Sink Dataset:**

```json
{
  "name": "Ds_DynamicParquetSink",
  "properties": {
    "type": "Parquet",
    "typeProperties": {
      "location": {
        "type": "AzureBlobFSLocation",
        "fileName": { "value": "@dataset().FileName", "type": "Expression" },
        "folderPath": { "value": "@dataset().FolderPath", "type": "Expression" }
      }
    },
    "parameters": {
      "FolderPath": { "type": "String" },
      "FileName": { "type": "String" }
    },
    "linkedService": { "referenceName": "Ls_AdlsGen2", "type": "LinkedServiceReference" }
  }
}
```

### 2.5 Watermark Update

```json
{
  "name": "UpdateWatermark",
  "type": "SqlServerStoredProcedure",
  "typeProperties": {
    "storedProcedureName": "[metadata].[usp_UpdateWatermark]",
    "storedProcedureParameters": {
      "SourceId": { "value": "@item().source_id", "type": "Int" },
      "NewWatermark": { "value": "@{formatDateTime(utcnow(), 'yyyy-MM-ddTHH:mm:ssZ')}", "type": "String" }
    }
  }
}
```

---

## 3. Databricks Parameterisation

### 3.1 Notebook Receiving Metadata Parameters

```python
# %run /Shared/metadata_utils/get_source_metadata
import json

# Widgets are populated by the ADF Databricks Notebook activity
dbutils.widgets.text("source_id", "")
dbutils.widgets.text("source_name", "")
dbutils.widgets.text("target_table", "")
dbutils.widgets.text("watermark_value", "")

source_id       = dbutils.widgets.get("source_id")
source_name     = dbutils.widgets.get("source_name")
target_table    = dbutils.widgets.get("target_table")
watermark_value = dbutils.widgets.get("watermark_value")

# Fetch column mappings from metadata database
column_mappings = spark.read \
    .format("jdbc") \
    .option("url", METADATA_DB_URL) \
    .option("dbtable", f"(SELECT * FROM metadata.column_mapping WHERE source_id = {source_id} AND is_active = 1 ORDER BY column_order)") \
    .option("user", METADATA_USER) \
    .option("password", METADATA_PASSWORD) \
    .load() \
    .collect()

# Fetch transformation rules
transformations = {}
for row in column_mappings:
    if row["transformation_id"]:
        t_rule = spark.read.format("jdbc") \
            .option("url", METADATA_DB_URL) \
            .option("dbtable", f"(SELECT * FROM metadata.transformation_rules WHERE transformation_id = {row['transformation_id']})") \
            .option("user", METADATA_USER) \
            .option("password", METADATA_PASSWORD) \
            .load() \
            .collect()[0]
        transformations[row["source_column"]] = {
            "template": t_rule["expression_template"],
            "type": t_rule["transformation_type"]
        }
```

### 3.2 Dynamic SQL Generation

```python
def build_select_clause(mappings, transformations, watermark_column=None, watermark_value=None):
    """Build a SELECT clause with inline transformations from metadata."""
    select_parts = []
    for m in mappings:
        src_col = m["source_column"]
        tgt_col = m["target_column"]
        if src_col in transformations:
            expr = transformations[src_col]["template"].replace("{source_column}", src_col)
            select_parts.append(f"{expr} AS {tgt_col}")
        else:
            select_parts.append(f"{src_col} AS {tgt_col}")
    select_clause = ",\n    ".join(select_parts)

    where_clause = ""
    if watermark_column and watermark_value:
        where_clause = f"\nWHERE {watermark_column} > '{watermark_value}'"

    return f"SELECT\n    {select_clause}\nFROM {mappings[0]['schema_name']}.{mappings[0]['object_name']}{where_clause}"


def build_merge_statement(target_table, mappings, merge_keys):
    """Generate a MERGE INTO statement for SCD-type upserts."""
    src_alias = "src"
    tgt_alias = "tgt"

    on_clause = " AND ".join([f"{tgt_alias}.{k} = {src_alias}.{k}" for k in merge_keys])

    set_clause = ",\n        ".join([
        f"{tgt_alias}.{m['target_column']} = {src_alias}.{m['target_column']}"
        for m in mappings
        if m['target_column'] not in merge_keys
    ])

    insert_cols = ", ".join([m["target_column"] for m in mappings])
    insert_vals = ", ".join([f"{src_alias}.{m['target_column']}" for m in mappings])

    return f"""
MERGE INTO {target_table} AS {tgt_alias}
USING ({source_query}) AS {src_alias}
ON {on_clause}
WHEN MATCHED THEN
    UPDATE SET
        {set_clause}
WHEN NOT MATCHED THEN
    INSERT ({insert_cols})
    VALUES ({insert_vals})
"""
```

### 3.3 Data Quality Execution

```python
def execute_dq_rules(df, source_id, spark):
    """Execute DQ rules from metadata against a DataFrame."""
    dq_rules = spark.read.format("jdbc") \
        .option("url", METADATA_DB_URL) \
        .option("dbtable", f"(SELECT * FROM metadata.dq_rules WHERE source_id = {source_id} AND is_active = 1)") \
        .option("user", METADATA_USER) \
        .option("password", METADATA_PASSWORD) \
        .load() \
        .collect()

    results = []
    for rule in dq_rules:
        if rule["rule_type"] == "NOT_NULL":
            violations = df.filter(col(rule["column_name"]).isNull()).count()
        elif rule["rule_type"] == "UNIQUE":
            violations = df.groupBy(rule["column_name"]).count().filter("count > 1").count()
        elif rule["rule_type"] == "RANGE":
            # rule_expression like "amount BETWEEN 0 AND 1000000"
            violations = df.filter(f"NOT ({rule['rule_expression']})").count()
        elif rule["rule_type"] == "REGEX":
            violations = df.filter(f"NOT {rule['column_name']} RLIKE '{rule['rule_expression']}'").count()
        elif rule["rule_type"] == "CUSTOM_SQL":
            # Register temp view and run the SQL
            df.createOrReplaceTempView("dq_check_view")
            violations = spark.sql(rule["rule_expression"]).collect()[0][0]

        total = df.count()
        failure_pct = (violations / total * 100) if total > 0 else 100
        passed = failure_pct <= rule["threshold_pct"]

        results.append({
            "rule_name": rule["rule_name"],
            "severity": rule["severity"],
            "violations": violations,
            "failure_pct": failure_pct,
            "threshold_pct": rule["threshold_pct"],
            "passed": passed,
            "action": rule["action_on_failure"] if not passed else "NONE"
        })

        if not passed and rule["action_on_failure"] == "FAIL_PIPELINE":
            raise Exception(f"DQ Rule '{rule['rule_name']}' failed: {violations} violations ({failure_pct:.2f}%)")

    return results
```

---

## 4. YAML Configuration Format

In addition to database-stored metadata, source-controlled YAML files provide a human-readable, reviewable format for metadata. The YAML is loaded into the metadata tables via a bootstrap pipeline.

Reference template: `templates/metadata-config-template.yaml`

```yaml
# metadata-config-template.yaml
# Source-controlled metadata configuration for data engineering pipelines
# This file is ingested by the Metadata Bootstrap pipeline into metadata tables.

version: "1.0"
environment: dev  # dev | test | prod

sources:
  - source_name: sap_customers
    source_type: Database
    source_system: SAP
    connection_string_key: kv-sap-erp-connection
    schema_name: SAPCRM
    object_name: CUSTOMERS
    ingestion_mode: Incremental
    incremental_column: LAST_MODIFIED
    target_layer: bronze
    target_table: sap_customers
    load_frequency: Hourly
    priority: 1
    column_mappings:
      - source_column: KUNNR
        source_data_type: NVARCHAR(10)
        target_column: customer_id
        target_data_type: STRING
        is_nullable: false
        column_order: 1
        transformation: null
      - source_column: NAME1
        source_data_type: NVARCHAR(35)
        target_column: customer_name
        target_data_type: STRING
        is_nullable: true
        column_order: 2
        transformation: TrimSpaces
      - source_column: ERDAT
        source_data_type: NVARCHAR(8)
        target_column: created_date
        target_data_type: TIMESTAMP
        is_nullable: true
        column_order: 3
        transformation: null
    dq_rules:
      - rule_name: customer_id_not_null
        rule_type: NOT_NULL
        column_name: customer_id
        severity: ERROR
        action_on_failure: FAIL_PIPELINE
        threshold_pct: 0
      - rule_name: customer_id_unique
        rule_type: UNIQUE
        column_name: customer_id
        severity: ERROR
        action_on_failure: FAIL_PIPELINE
        threshold_pct: 0

transformations:
  - name: TrimSpaces
    type: DERIVED
    expression_template: "TRIM({source_column})"
    description: "Trim leading/trailing whitespace"
  - name: ToUpper
    type: CAST
    expression_template: "UPPER({source_column})"
    description: "Convert to uppercase"
  - name: EpochToTimestamp
    type: DATE_FORMAT
    expression_template: "TIMESTAMP_MILLIS(CAST({source_column} AS BIGINT))"
    description: "Convert epoch ms to timestamp"
```

---

## 5. Benefits

| Benefit | Description |
|---|---|
| **No code change for new sources** | Insert a row into `source_config` + `column_mapping`, and the pipeline picks it up automatically. |
| **Centralised governance** | All source definitions live in one place. Auditors can query metadata tables instead of reading pipeline JSON. |
| **Consistent DQ enforcement** | Every source goes through the same DQ framework. No source is "forgotten." |
| **Self-service enablement** | Analysts can request new sources via a metadata form without engineering involvement. |
| **Environment portability** | YAML metadata files can be promoted through dev → test → prod with environment-specific connection strings. |
| **Reduced deployment risk** | Adding metadata rows doesn't require deploying new pipeline code. Rollback = deactivate the row. |

### Trade-offs

- **Initial complexity**: The framework requires upfront investment in the metadata schema and the generic pipeline.
- **Debugging difficulty**: Errors in metadata (wrong column name, bad transformation template) can be harder to trace than errors in explicit code.
- **Performance overhead**: Dynamic SQL generation and metadata lookups add latency to each pipeline run.
- **Schema drift**: Source schema changes (new columns, renamed columns) require metadata updates. Consider automating schema-drift detection.

---

## 6. Governance Considerations

- **Versioning**: Store YAML metadata in Git. Every metadata change goes through a PR review.
- **Audit trail**: Maintain a `metadata.source_config_audit` table with `INSERT`/`UPDATE`/`DELETE` triggers recording who changed what and when.
- **Environment isolation**: Use separate metadata databases per environment (dev, test, prod). Never share production metadata with lower environments.
- **Access control**: Grant write access to metadata tables only to pipeline service principals and authorised data engineers. Analysts get read-only access.
- **Validation**: Implement a metadata validation stored procedure that checks referential integrity (e.g., every `transformation_id` in `column_mapping` exists in `transformation_rules`) before activating a source.

---

## 7. Complete Metadata-Driven Pipeline (Python)

Below is a simplified end-to-end Python implementation that can run in Databricks:

```python
"""
Metadata-Driven Ingestion Pipeline
Reads source_config, column_mapping, and dq_rules from metadata database,
then ingests, transforms, validates, and writes to the target layer.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import *
import logging

logger = logging.getLogger(__name__)

METADATA_CONFIG = {
    "url": dbutils.secrets.get(scope="kv-dataeng", key="metadata-db-url"),
    "user": dbutils.secrets.get(scope="kv-dataeng", key="metadata-db-user"),
    "password": dbutils.secrets.get(scope="kv-dataeng", key="metadata-db-password")
}


def load_metadata(spark, source_name):
    """Load all metadata for a given source."""
    jdbc_opts = {
        "url": METADATA_CONFIG["url"],
        "user": METADATA_CONFIG["user"],
        "password": METADATA_CONFIG["password"],
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    source_config = spark.read.format("jdbc") \
        .option(**jdbc_opts) \
        .option("dbtable", f"(SELECT * FROM metadata.source_config WHERE source_name = '{source_name}' AND is_active = 1)") \
        .load() \
        .collect()[0]

    column_mappings = spark.read.format("jdbc") \
        .option(**jdbc_opts) \
        .option("dbtable", f"(SELECT cm.*, tr.expression_template, tr.transformation_type FROM metadata.column_mapping cm LEFT JOIN metadata.transformation_rules tr ON cm.transformation_id = tr.transformation_id WHERE cm.source_id = {source_config['source_id']} AND cm.is_active = 1 ORDER BY cm.column_order)") \
        .load() \
        .collect()

    dq_rules = spark.read.format("jdbc") \
        .option(**jdbc_opts) \
        .option("dbtable", f"(SELECT * FROM metadata.dq_rules WHERE source_id = {source_config['source_id']} AND is_active = 1)") \
        .load() \
        .collect()

    return source_config, column_mappings, dq_rules


def apply_transformations(df, column_mappings):
    """Apply transformations defined in metadata to the DataFrame."""
    for m in column_mappings:
        src_col = m["source_column"]
        tgt_col = m["target_column"]

        if m["expression_template"]:
            expr = m["expression_template"].replace("{source_column}", src_col)
            df = df.withColumn(tgt_col, F.expr(expr))
        else:
            df = df.withColumnRenamed(src_col, tgt_col)

    # Select only mapped columns in order
    select_cols = [m["target_column"] for m in column_mappings]
    return df.select(*select_cols)


def run_pipeline(source_name):
    spark = SparkSession.builder.getOrCreate()

    logger.info(f"Loading metadata for source: {source_name}")
    source_config, column_mappings, dq_rules = load_metadata(spark, source_name)

    logger.info(f"Source type: {source_config['source_type']}")
    logger.info(f"Ingestion mode: {source_config['ingestion_mode']}")

    # --- Read source data ---
    if source_config["source_type"] == "Database":
        source_df = spark.read.format("jdbc") \
            .option("url", dbutils.secrets.get(scope="kv-dataeng", key=source_config["connection_string_key"])) \
            .option("dbtable", f"{source_config['schema_name']}.{source_config['object_name']}") \
            .load()

        if source_config["ingestion_mode"] == "Incremental":
            watermark = source_config["watermark_value"]
            inc_col = source_config["incremental_column"]
            source_df = source_df.filter(col(inc_col) > lit(watermark))

    elif source_config["source_type"] == "File":
        source_df = spark.read.format("parquet") \
            .load(f"abfss://landing@storage.dfs.core.windows.net/{source_config['object_name']}")

    # --- Apply transformations ---
    logger.info("Applying transformations from metadata")
    transformed_df = apply_transformations(source_df, column_mappings)

    # --- Execute DQ rules ---
    logger.info(f"Executing {len(dq_rules)} DQ rules")
    dq_results = execute_dq_rules(transformed_df, source_config["source_id"], spark)

    for r in dq_results:
        status = "PASS" if r["passed"] else "FAIL"
        logger.info(f"  DQ [{status}] {r['rule_name']}: {r['violations']} violations ({r['failure_pct']:.2f}%)")

    # --- Write to target layer ---
    target_path = f"abfss://{source_config['target_layer']}@storage.dfs.core.windows.net/{source_config['target_table']}"
    logger.info(f"Writing to {target_path}")

    transformed_df.write.format("delta") \
        .mode("merge") \
        .option("mergeSchema", "true") \
        .save(target_path)

    logger.info(f"Pipeline complete for {source_name}. Rows written: {transformed_df.count()}")


# Entry point
run_pipeline("sap_customers")
```

---

## 8. Summary

Metadata-driven solutions transform data pipeline development from a **code-first** discipline to a **configuration-first** discipline. The core investment is in:

1. A well-designed metadata schema (4 tables: source config, column mapping, DQ rules, transformation rules)
2. A generic, parameterised pipeline (ADF or Databricks) that interprets metadata at runtime
3. YAML-based source control for metadata, with a bootstrap loader to sync YAML → database
4. Governance: audit trails, validation, environment isolation

The payoff is exponential: the Nth source costs the same as the 1st to onboard, and every source benefits from consistent DQ enforcement, auditability, and environment portability.
