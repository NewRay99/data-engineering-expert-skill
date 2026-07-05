# Azure Data Factory Ingestion Patterns

## Overview

**Azure Data Factory (ADF)** is Microsoft's cloud-based data integration service that orchestrates data movement and transformation at scale. In a Databricks lakehouse architecture, ADF serves as the primary **ingestion and orchestration layer**, moving data from diverse source systems into the Bronze layer and triggering Databricks transformation jobs for downstream processing.

ADF complements Databricks by handling:
- **Scheduled and event-driven data movement** from 100+ connectors
- **Copy activities** for efficient bulk data transfer
- **Pipeline orchestration** with dependencies, branching, and error handling
- **Metadata-driven ingestion** using parameters and lookup activities
- **Monitoring and alerting** through Azure Monitor integration

## Core ADF Concepts

### Key Components

| Component        | Description                                                        |
|-----------------|--------------------------------------------------------------------|
| **Pipeline**     | Logical grouping of activities that perform a unit of work         |
| **Activity**     | A processing step (copy, databricks notebook, stored proc, etc.)  |
| **Dataset**      | Named view of data pointing to a specific data structure           |
| **Linked Service** | Connection definition to a source or destination (connection string) |
| **Integration Runtime (IR)** | Compute infrastructure that executes activities                |
| **Trigger**      | Defines when a pipeline runs (schedule, tumbling window, event)  |
| **Data Flow**    | Visual data transformation using Spark clusters managed by ADF   |
| **Parameter**    | Dynamic value passed to pipeline/dataset for reusability           |

### Integration Runtime Types

| IR Type             | Use Case                                                    |
|---------------------|-------------------------------------------------------------|
| **Azure IR**         | Cloud-to-cloud data movement, data flows                   |
| **Self-hosted IR**   | On-premises or VNet-secured data sources                   |
| **Azure-SSIS IR**    | Lift-and-shift SSIS packages                                |

## Ingestion Patterns

### Pattern 1: Batch File Ingestion to Bronze (Delta)

The most common pattern: copy files from a source (e.g., SFTP, Azure Blob, ADLS) into ADLS Gen2, then use Databricks to register as a Delta table.

```json
{
  "name": "IngestCSVToBronze",
  "properties": {
    "activities": [
      {
        "name": "CopyFilesToADLS",
        "type": "Copy",
        "inputs": [
          { "referenceName": "SourceSftpCSV", "type": "DatasetReference" }
        ],
        "outputs": [
          { "referenceName": "BronzeADLSRaw", "type": "DatasetReference" }
        ],
        "typeProperties": {
          "source": { "type": "DelimitedTextSource" },
          "sink": {
            "type": "DelimitedTextSink",
            "storeSettings": {
              "type": "AzureBlobFSWriteSettings"
            }
          }
        }
      },
      {
        "name": "RegisterBronzeTable",
        "type": "DatabricksNotebook",
        "dependsOn": [
          { "activity": "CopyFilesToADLS", "dependencyConditions": ["Succeeded"] }
        ],
        "typeProperties": {
          "notebookPath": "/ingestion/register_bronze_table",
          "baseParameters": {
            "source_name": { "value": "@pipeline().parameters.source_name", "type": "Expression" },
            "file_format": "csv",
            "target_table": { "value": "@concat('bronze.', pipeline().parameters.source_name)", "type": "Expression" }
          }
        },
        "linkedServiceName": { "referenceName": "AzureDatabricks_LS", "type": "LinkedServiceReference" }
      }
    ],
    "parameters": {
      "source_name": { "type": "string" }
    }
  }
}
```

### Pattern 2: Database Incremental Extraction (CDC)

Use ADF's **Copy Activity with watermark** pattern to extract only changed rows from source databases.

**Pipeline structure:**
1. **Lookup** activity — get last watermark (max `modified_at` from control table)
2. **Copy** activity — query source with `WHERE modified_at > @watermark`
3. **Stored Procedure** activity — update watermark in control table
4. **Databricks Notebook** activity — process incremental load into Bronze/Silver

```json
{
  "name": "IncrementalExtractFromSQL",
  "activities": [
    {
      "name": "GetLastWatermark",
      "type": "Lookup",
      "typeProperties": {
        "source": {
          "type": "AzureSqlSource",
          "sqlQuery": "SELECT MAX(last_modified) AS watermark FROM metadata.ingestion_control WHERE source_table = '@{pipeline().parameters.source_table}'"
        },
        "dataset": { "referenceName": "ControlTableDS", "type": "DatasetReference" }
      }
    },
    {
      "name": "CopyChangedRows",
      "type": "Copy",
      "dependsOn": [{ "activity": "GetLastWatermark", "dependencyConditions": ["Succeeded"] }],
      "typeProperties": {
        "source": {
          "type": "AzureSqlSource",
          "sqlQuery": "SELECT * FROM @{pipeline().parameters.source_table} WHERE modified_at > '@{activity('GetLastWatermark').output.firstRow.watermark}'"
        },
        "sink": {
          "type": "DelimitedTextSink",
          "storeSettings": { "type": "AzureBlobFSWriteSettings" }
        }
      }
    },
    {
      "name": "UpdateWatermark",
      "type": "SqlServerStoredProcedure",
      "dependsOn": [{ "activity": "CopyChangedRows", "dependencyConditions": ["Succeeded"] }],
      "typeProperties": {
        "storedProcedureName": "[metadata].[update_watermark]",
        "storedProcedureParameters": {
          "source_table": { "value": "@pipeline().parameters.source_table", "type": "String" },
          "new_watermark": { "value": "@{utcnow()}", "type": "DateTime" }
        }
      }
    }
  ]
}
```

### Pattern 3: Metadata-Driven Ingestion Framework

For enterprise scale (100+ source tables), build a **metadata-driven framework** where a single parameterized pipeline handles all sources, driven by a configuration table:

**Metadata table structure (stored in Azure SQL):**

```sql
CREATE TABLE metadata.ingestion_config (
    source_id           INT IDENTITY(1,1) PRIMARY KEY,
    source_system       VARCHAR(100)  NOT NULL,
    source_type         VARCHAR(50)   NOT NULL,  -- 'AzureSQL', 'Oracle', 'SFTP', 'API', 'CosmosDB'
    source_table        VARCHAR(200)  NOT NULL,
    source_query        NVARCHAR(MAX),           -- optional custom query
    target_adls_path    VARCHAR(500)  NOT NULL,
    target_bronze_table VARCHAR(200)  NOT NULL,
    file_format         VARCHAR(20)   DEFAULT 'parquet',
    incremental_column  VARCHAR(100),            -- for CDC: 'modified_at', 'id', etc.
    load_frequency      VARCHAR(20)   DEFAULT 'daily', -- 'hourly', 'daily', 'weekly'
    is_active           BIT           DEFAULT 1,
    priority            INT           DEFAULT 5,
    retry_count         INT           DEFAULT 3,
    created_at          DATETIME2     DEFAULT SYSDATETIME(),
    updated_at          DATETIME2     DEFAULT SYSDATETIME()
);
```

**Master pipeline:**

1. **Lookup** all active sources from `metadata.ingestion_config` where `load_frequency = @trigger().frequency`
2. **ForEach** loop over each source
3. Inside the loop, call a **child pipeline** with parameters (`source_table`, `target_path`, `incremental_column`, etc.)
4. Child pipeline performs: Copy → Databricks notebook → Validation → Audit logging

```json
{
  "name": "MasterIngestionPipeline",
  "activities": [
    {
      "name": "GetSourceList",
      "type": "Lookup",
      "typeProperties": {
        "source": {
          "type": "AzureSqlSource",
          "sqlQuery": "SELECT * FROM metadata.ingestion_config WHERE is_active = 1 AND load_frequency = '@{pipeline().parameters.frequency}'"
        },
        "dataset": { "referenceName": "MetadataConfigDS", "type": "DatasetReference" },
        "firstRowOnly": false
      }
    },
    {
      "name": "ForEachSource",
      "type": "ForEach",
      "dependsOn": [{ "activity": "GetSourceList", "dependencyConditions": ["Succeeded"] }],
      "typeProperties": {
        "items": { "value": "@activity('GetSourceList').output.value", "type": "Expression" },
        "isSequential": false,
        "batchCount": 5,
        "activities": [
          {
            "name": "ExecuteChildIngestion",
            "type": "ExecutePipeline",
            "typeProperties": {
              "pipeline": { "referenceName": "ChildIngestionPipeline", "type": "PipelineReference" },
              "waitOnCompletion": true,
              "parameters": {
                "source_system": "@item().source_system",
                "source_table": "@item().source_table",
                "target_path": "@item().target_adls_path",
                "bronze_table": "@item().target_bronze_table",
                "incremental_column": "@item().incremental_column",
                "source_type": "@item().source_type"
              }
            }
          }
        ]
      }
    }
  ]
}
```

### Pattern 4: Event-Driven Ingestion

Use **Event Grid triggers** to ingest data as soon as a file lands in ADLS Gen2:

1. File uploaded to `adls://raw/incoming/sap/customers_20250101.csv`
2. Event Grid fires → triggers ADF pipeline
3. Pipeline reads the file path from trigger payload
4. Copy to Bronze landing zone → Databricks notebook processes → Delta table

```json
{
  "name": "EventDrivenBronzeLoad",
  "properties": {
    "parameters": {
      "fileName": { "type": "String" },
      "folderPath": { "type": "String" }
    },
    "activities": [
      {
        "name": "TriggerDatabricksLoad",
        "type": "DatabricksNotebook",
        "typeProperties": {
          "notebookPath": "/ingestion/event_bronze_loader",
          "baseParameters": {
            "file_path": { "value": "@concat(pipeline().parameters.folderPath, '/', pipeline().parameters.fileName)", "type": "Expression" }
          }
        }
      }
    ]
  }
}
```

### Pattern 5: API Ingestion

For REST API sources, use ADF's **Web Activity** or **Copy Activity with REST source**:

```json
{
  "name": "RestApiIngestion",
  "type": "Copy",
  "typeProperties": {
    "source": {
      "type": "RestSource",
      "httpRequestTimeout": "00:05:00",
      "requestMethod": "GET",
      "additionalHeaders": {
        "Authorization": "Bearer @{pipeline().parameters.api_token}"
      },
      "paginationRules": {
        "supportRFC5988": true
      }
    },
    "sink": {
      "type": "JsonSink",
      "storeSettings": { "type": "AzureBlobFSWriteSettings" }
    }
  }
}
```

## Linked Services Configuration

### Databricks Linked Service

```json
{
  "name": "AzureDatabricks_LS",
  "type": "Microsoft.DataFactory/factories/linkedservices",
  "properties": {
    "type": "AzureDatabricks",
    "typeProperties": {
      "domain": "https://adb-1234567890.1.azuredatabricks.net",
      "authentication": "MSI",
      "workspaceResourceId": "/subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.Databricks/workspaces/xxx",
      "newClusterNodeType": "Standard_DS3_v2",
      "newClusterNumOfWorker": "4",
      "newClusterSparkEnv": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
      },
      "newClusterVersion": "14.3.x-scala2.12"
    }
  }
}
```

### Best Practice: Use Existing Interactive Clusters vs. Job Clusters

| Approach                     | Pros                                    | Cons                                     |
|-----------------------------|-----------------------------------------|------------------------------------------|
| **New job cluster per activity** | Isolated, auto-terminates          | Cold start (~5 min), higher latency      |
| **Existing interactive cluster** | No cold start, shared              | Cost when idle, resource contention      |
| **Job cluster (shared)**    | Balanced for batch                      | Requires orchestration                    |

**Recommendation**: For production, use **job clusters** (new or existing) with auto-termination. Avoid interactive clusters for scheduled pipelines.

## Orchestration: ADF + Databricks Integration

### End-to-End Medallion Pipeline

```
ADF Pipeline: "Daily_Sales_Medallion_Pipeline"
│
├── [1] Copy SAP Orders → ADLS Raw Zone
├── [2] Copy CRM Accounts → ADLS Raw Zone          (parallel with [1])
├── [3] Databricks Notebook: Bronze → Silver       (depends on [1], [2])
├── [4] Databricks Notebook: Silver → Gold         (depends on [3])
├── [5] Databricks Notebook: Data Quality Checks   (depends on [4])
├── [6] If quality PASS → Power BI Dataset Refresh
├── [7] If quality FAIL → Send Alert (Teams/Email)
└── [8] Log pipeline status to audit table
```

### Databricks Notebook Activity Parameters

When calling Databricks from ADF, pass parameters using `baseParameters`:

```json
{
  "name": "TransformBronzeToSilver",
  "type": "DatabricksNotebook",
  "typeProperties": {
    "notebookPath": "/pipelines/bronze_to_silver",
    "baseParameters": {
      "source_table": "@pipeline().parameters.source_table",
      "target_table": "@pipeline().parameters.target_table",
      "load_date": "@formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd')",
      "batch_id": "@pipeline().RunId"
    }
  }
}
```

### Databricks Python Notebook (Bronze → Silver)

```python
# /pipelines/bronze_to_silver
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# ADF parameters via dbutils.widgets
source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")
load_date = dbutils.widgets.get("load_date")
batch_id = dbutils.widgets.get("batch_id")

print(f"Processing: {source_table} → {target_table} for {load_date}")

# Read Bronze data
bronze_df = spark.read.table(f"bronze.{source_table}") \
    .filter(F.col("_ingest_date") == load_date)

# Cleansing and conformance
silver_df = (bronze_df
    .dropDuplicates(["id"])
    .withColumn("processed_ts", F.current_timestamp())
    .withColumn("batch_id", F.lit(batch_id))
    .drop("_source_file", "_ingest_ts"))

# Merge into Silver (upsert)
if spark.catalog.tableExists(f"silver.{target_table}"):
    delta_table = DeltaTable.forName(spark, f"silver.{target_table}")
    delta_table.alias("target").merge(
        silver_df.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    silver_df.write.format("delta").saveAsTable(f"silver.{target_table}")

print(f"Completed: {silver_df.count()} rows merged into silver.{target_table}")
```

## Error Handling and Retry

### Retry Policies

```json
{
  "typeProperties": {
    "policy": {
      "timeout": "02:00:00",
      "retry": 3,
      "retryIntervalInSeconds": 300,
      "secureOutput": false,
      "secureInput": false
    }
  }
}
```

### Conditional Logic and Alerting

```json
{
  "name": "CheckPipelineStatus",
  "type": "IfCondition",
  "typeProperties": {
    "expression": {
      "value": "@equals(activity('DataQualityChecks').output.status, 'FAIL')",
      "type": "Expression"
    },
    "ifTrueActivities": [
      {
        "name": "SendAlert",
        "type": "WebActivity",
        "typeProperties": {
          "url": "https://outlook.office.com/webhook/xxx",
          "method": "POST",
          "body": {
            "message": "Pipeline FAILED: @{pipeline().PipelineName} at @{utcnow()}"
          }
        }
      }
    ],
    "ifFalseActivities": [
      {
        "name": "LogSuccess",
        "type": "SqlServerStoredProcedure",
        "typeProperties": {
          "storedProcedureName": "metadata.log_pipeline_success"
        }
      }
    ]
  }
}
```

## Monitoring and Observability

### ADF Built-in Monitoring

- **Pipeline runs** — visual status, duration, error messages
- **Activity runs** — per-step execution details
- **Integration runtime monitoring** — queue times, throughput
- **SSIS integration runtime** — package-level execution logs

### Azure Monitor Integration

```json
{
  "diagnosticSettings": {
    "logs": [
      { "category": "PipelineRuns", "enabled": true },
      { "category": "ActivityRuns", "enabled": true },
      { "category": "TriggerRuns", "enabled": true },
      { "category": "SandboxPipelineRuns", "enabled": true }
    ],
    "workspaceId": "/subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.OperationalInsights/workspaces/LogAnalyticsWS"
  }
}
```

### Audit Logging Table

```sql
CREATE TABLE metadata.pipeline_audit (
    audit_id        BIGINT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name   VARCHAR(200) NOT NULL,
    pipeline_run_id VARCHAR(100) NOT NULL,
    trigger_type    VARCHAR(50),
    start_time      DATETIME2 NOT NULL,
    end_time        DATETIME2,
    status          VARCHAR(20) NOT NULL,  -- 'Succeeded', 'Failed', 'Running'
    error_message   NVARCHAR(MAX),
    rows_processed  BIGINT,
    parameters_json NVARCHAR(MAX),
    created_at      DATETIME2 DEFAULT SYSDATETIME()
);
```

## Best Practices

### Performance

- **Use Parquet format** for staging — better compression than CSV, schema preserved
- **Set batchCount** on ForEach loops (3-5) to parallelize without overwhelming source
- **Tune Copy Activity DIU (Data Integration Units)** — default is auto, increase for large datasets
- **Use Self-hosted IR** for on-prem sources — place near the data
- **Pre-copy scripts** to clean staging — avoid accumulation of stale files

### Security

- **Use Managed Identity** (MSI) authentication for ADF → Azure services (ADLS, SQL, Databricks)
- **Store secrets in Azure Key Vault** — reference from Linked Services
- **Enable Private Endpoints** for ADF and linked services
- **Apply RBAC** — developers get Data Factory Contributor on dev; only operators on prod
- **Encrypt linked service credentials** at rest

### Governance

- **Naming convention**: `PL_{domain}_{frequency}_{action}` → `PL_Sales_Daily_BronzeToGold`
- **Tag resources**: Environment, Owner, CostCenter, DataClassification
- **Version control**: Export ARM templates to Git (Azure DevOps / GitHub)
- **CI/CD**: Use Azure DevOps pipelines to deploy ADF ARM templates across environments

### Cost Optimization

| Strategy                    | Impact                                         |
|-----------------------------|------------------------------------------------|
| Auto-pause Self-hosted IR   | Reduces idle compute cost                     |
| Batch multiple sources      | Fewer cluster starts, better utilization      |
| Use ADF Data Flows sparingly | Databricks is often cheaper for transforms   |
| Schedule off-peak           | Leverage lower-cost windows                   |
| Monitor DIU usage           | Over-provisioning DIU wastes money            |

## Comparison: ADF vs. Databricks Workflows vs. Airflow

| Feature              | ADF                              | Databricks Workflows              | Apache Airflow                   |
|---------------------|----------------------------------|-----------------------------------|----------------------------------|
| Source connectors   | 100+ (enterprise-grade)          | Limited (file/DB)                | Via operators (extensible)       |
| On-premises         | Self-hosted IR (excellent)       | Network peering required          | Via plugins                       |
| Visual designer     | Yes                              | Yes (notebook + UI)              | No (code-first)                   |
| Databricks integration | Native activity type          | Native                            | Via `DatabricksSubmitRunOperator` |
| Cost model          | Per-activity + DIU               | Databricks DBU                    | Compute (Kubernetes/Cloud)        |
| CI/CD               | ARM templates                    | DABs (Databricks Asset Bundles)   | Python DAGs in Git                |
| Best for            | Heterogeneous ingestion + orchestration | Databricks-native pipelines | Complex dependency graphs         |

## Summary

Azure Data Factory excels as the **ingestion and orchestration layer** in a Databricks lakehouse architecture. Its strengths include:
- **Broad connectivity** to enterprise source systems (100+ connectors)
- **Metadata-driven frameworks** that scale to hundreds of source tables
- **Seamless Databricks integration** via native notebook/job activities
- **Enterprise-grade monitoring** and CI/CD via ARM templates

For transformation-heavy workloads, delegate to Databricks (notebooks, DLT, Spark SQL) and use ADF strictly for orchestration. For heterogeneous ingestion (SFTP, APIs, on-prem databases), ADF's Copy Activity and Self-hosted IR remain the best-in-class approach in the Azure ecosystem.
