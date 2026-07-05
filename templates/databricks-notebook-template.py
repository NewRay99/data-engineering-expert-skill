# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Transformation: <TABLE_NAME>
# MAGIC
# MAGIC | Field | Value |
# MAGIC|-------|-------|
# MAGIC | **Pipeline** | <pipeline_name> |
# MAGIC | **Layer** | Silver |
# MAGIC | **Source** | Bronze.<bronze_table> |
# MAGIC | **Target** | Silver.<silver_table> |
# MAGIC | **Author** | <author> |
# MAGIC | **Created** | YYYY-MM-DD |
# MAGIC | **Last Modified** | YYYY-MM-DD |
# MAGIC | **Version** | 1.0.0 |
# MAGIC | **Schedule** | <cron_or_trigger> |
# MAGIC | **SLA** | <sla_minutes> minutes |
# MAGIC
# MAGIC ## Change Log
# MAGIC | Date | Author | Change |
# MAGIC|------|--------|--------|
# MAGIC | YYYY-MM-DD | <author> | Initial creation |
# MAGIC
# MAGIC ## Dependencies
# MAGIC - Bronze table: `<catalog>.<schema>.<bronze_table>`
# MAGIC - Metadata config: `<metadata_table>`
# MAGIC - DQ rules: `<dq_rules_table>`

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# dbutils.widgets
dbutils.widgets.text("source_system", "", "Source System")
dbutils.widgets.text("target_table", "", "Target Table (Silver)")
dbutils.widgets.text("load_date", "", "Load Date (YYYY-MM-DD)")
dbutils.widgets.text("batch_id", "", "Batch ID")
dbutils.widgets.text("env", "dev", "Environment (dev/test/prod)")
dbutils.widgets.text("full_refresh", "false", "Full Refresh (true/false)")

# Read parameters
source_system = dbutils.widgets.get("source_system")
target_table = dbutils.widgets.get("target_table")
load_date = dbutils.widgets.get("load_date")
batch_id = dbutils.widgets.get("batch_id")
env = dbutils.widgets.get("env")
full_refresh = dbutils.widgets.get("full_refresh").lower() == "true"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Imports and Configuration

# COMMAND ----------

import json
import logging
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Catalog and schema (environment-aware)
catalog = f"{env}_catalog"
bronze_schema = f"{catalog}.bronze"
silver_schema = f"{catalog}.silver"
metadata_schema = f"{catalog}.metadata"

bronze_table_name = f"{bronze_schema}.{target_table.replace('silver_', 'bronze_')}"
silver_table_name = f"{silver_schema}.{target_table}"

logger.info(f"Starting Silver transformation: {bronze_table_name} -> {silver_table_name}")
logger.info(f"Parameters: source_system={source_system}, load_date={load_date}, batch_id={batch_id}, full_refresh={full_refresh}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load Metadata Configuration

# COMMAND ----------

# Load metadata for this table from the metadata table
metadata_df = spark.sql(f"""
    SELECT *
    FROM {metadata_schema}.pipeline_metadata
    WHERE source_system = '{source_system}'
      AND target_table = '{target_table}'
""")

metadata = metadata_df.collect()[0].asDict() if metadata_df.count() > 0 else {}
if not metadata:
    raise ValueError(f"No metadata configuration found for source_system={source_system}, target_table={target_table}")

logger.info(f"Metadata loaded: {json.dumps({k: str(v) for k, v in metadata.items()}, indent=2)}")

# Extract column mapping and transformation rules
column_mappings = json.loads(metadata.get("column_mapping", "[]"))
transformation_rules = json.loads(metadata.get("transformation_rules", "[]"))
watermark_column = metadata.get("watermark_column", "load_timestamp")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read Bronze Data

# COMMAND ----------

# Read Bronze table
bronze_df = spark.table(bronze_table_name)
logger.info(f"Bronze table schema: {bronze_df.schema.simpleString()}")

# Apply incremental filter unless full refresh
if not full_refresh and watermark_column:
    # Get last successful watermark from control table
    last_watermark_row = spark.sql(f"""
        SELECT MAX({watermark_column}) AS last_watermark
        FROM {metadata_schema}.pipeline_control
        WHERE pipeline_name = '{source_system}_{target_table}'
          AND status = 'SUCCESS'
    """).collect()

    last_watermark = last_watermark_row[0]["last_watermark"] if last_watermark_row else None

    if last_watermark:
        bronze_df = bronze_df.filter(F.col(watermark_column) > F.lit(last_watermark))
        logger.info(f"Incremental load: filtering {watermark_column} > {last_watermark}")
    else:
        logger.info("No previous watermark found — loading full snapshot")
else:
    logger.info("Full refresh requested — loading entire Bronze table")

bronze_count = bronze_df.count()
logger.info(f"Bronze records to process: {bronze_count}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Transformation Logic

# COMMAND ----------

# Start with Bronze DataFrame
silver_df = bronze_df

# --- 1. Apply column mappings (rename + select) ---
if column_mappings:
    for mapping in column_mappings:
        source_col = mapping["source_column"]
        target_col = mapping["target_column"]
        transformation = mapping.get("transformation", None)

        if transformation:
            # Apply transformation expression
            silver_df = silver_df.withColumn(target_col, F.expr(transformation))
        else:
            # Direct rename
            silver_df = silver_df.withColumnRenamed(source_col, target_col)

    # Select only mapped columns + audit columns
    target_cols = [m["target_column"] for m in column_mappings]
    audit_cols = ["_ingestion_date", "_source_file"]
    available_audit = [c for c in audit_cols if c in silver_df.columns]
    silver_df = silver_df.select(*target_cols, *available_audit)

logger.info(f"Silver schema after column mapping: {silver_df.schema.simpleString()}")

# --- 2. Standardize string columns (trim + upper for codes) ---
string_std_columns = ["status", "country_code", "currency_code"]
for col_name in string_std_columns:
    if col_name in silver_df.columns:
        silver_df = silver_df.withColumn(col_name, F.trim(F.upper(F.col(col_name))))

# --- 3. Handle nulls and defaults ---
null_defaults = {
    "status": F.lit("unknown"),
    "is_active": F.lit(False),
    "email": F.lit(""),
}
for col_name, default in null_defaults.items():
    if col_name in silver_df.columns:
        silver_df = silver_df.fillna({col_name: default})

# --- 4. Add audit columns ---
silver_df = (
    silver_df
    .withColumn("_silver_processed_date", F.current_timestamp())
    .withColumn("_batch_id", F.lit(batch_id))
    .withColumn("_source_system", F.lit(source_system))
)

# --- 5. Deduplicate (keep latest by watermark) ---
if watermark_column and watermark_column in silver_df.columns:
    primary_key = metadata.get("primary_key", "id")
    window = Window.partitionBy(primary_key).orderBy(F.col(watermark_column).desc())
    silver_df = (
        silver_df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    logger.info("Deduplication applied (latest record per primary key)")

silver_count = silver_df.count()
logger.info(f"Silver records after transformation: {silver_count}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# Load DQ rules for this table
dq_rules = spark.sql(f"""
    SELECT *
    FROM {metadata_schema}.dq_rules
    WHERE table_name = '{target_table}'
      AND enabled = true
""").collect()

dq_results = []
dq_fail_count = 0
dq_warn_count = 0

for rule in dq_rules:
    rule_dict = rule.asDict()
    rule_name = rule_dict.get("rule_name", rule_dict.get("column_name", "unknown"))
    check_type = rule_dict["check_type"]
    expression = rule_dict["rule_expression"]
    severity = rule_dict["severity"]
    action = rule_dict.get("action", "log")

    # Execute the DQ check
    violations = silver_df.filter(F.NOT(F.expr(expression))).count()

    result = {
        "rule_name": rule_name,
        "check_type": check_type,
        "expression": expression,
        "severity": severity,
        "action": action,
        "violations": violations,
        "status": "PASS" if violations == 0 else ("FAIL" if severity == "error" else "WARN"),
    }
    dq_results.append(result)

    if violations > 0:
        if severity == "error":
            dq_fail_count += violations
            logger.error(f"DQ FAIL [{check_type}] {rule_name}: {violations} violations — action: {action}")
        else:
            dq_warn_count += violations
            logger.warning(f"DQ WARN [{check_type}] {rule_name}: {violations} violations — action: {action}")
    else:
        logger.info(f"DQ PASS [{check_type}] {rule_name}: 0 violations")

# Quarantine logic: if action is 'quarantine', move violating rows to quarantine table
quarantine_rules = [r for r in dq_results if r["action"] == "quarantine" and r["violations"] > 0]
if quarantine_rules:
    quarantine_df = silver_df.filter(F.lit(False))  # Start with empty
    for rule in quarantine_rules:
        quarantine_df = quarantine_df.union(silver_df.filter(F.NOT(F.expr(rule["expression"]))))
    quarantine_table = f"{silver_schema}.{target_table}_quarantine"
    quarantine_df.write.format("delta").mode("append").saveAsTable(quarantine_table)
    logger.warning(f"Quarantined {quarantine_df.count()} rows to {quarantine_table}")

# Fail pipeline if any error-severity DQ rules failed
if dq_fail_count > 0:
    raise RuntimeError(
        f"Data quality checks failed: {dq_fail_count} error-severity violations, "
        f"{dq_warn_count} warning-severity violations. "
        f"Check quarantine table for details."
    )

logger.info(f"DQ summary: {len(dq_results)} rules executed, {dq_fail_count} errors, {dq_warn_count} warnings")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write to Silver (MERGE Pattern)

# COMMAND ----------

# Check if Silver table exists
silver_exists = spark.catalog.tableExists(silver_table_name)

if not silver_exists:
    # First load — create table
    silver_df.write.format("delta").mode("overwrite").saveAsTable(silver_table_name)
    logger.info(f"Silver table created: {silver_table_name} ({silver_count} rows)")
else:
    # MERGE (upsert) pattern
    primary_key = metadata.get("primary_key", "id")
    silver_table = DeltaTable.forName(spark, silver_table_name)

    # Build merge condition
    merge_condition = " AND ".join(
        f"target.{pk} = source.{pk}" for pk in primary_key.split(",")
    )

    # Build set clause for matched updates
    update_cols = {c: f"source.{c}" for c in silver_df.columns if c != primary_key}
    insert_cols = {c: f"source.{c}" for c in silver_df.columns}

    (
        silver_table.alias("target")
        .merge(silver_df.alias("source"), merge_condition)
        .whenMatchedUpdate(set=update_cols)
        .whenNotMatchedInsert(values=insert_cols)
        .execute()
    )
    logger.info(f"Silver table merged: {silver_table_name} ({silver_count} rows processed)")

# Optimize (Z-order by primary key for query performance)
spark.sql(f"OPTIMIZE {silver_table_name} ZORDER BY ({metadata.get('primary_key', 'id')})")
logger.info(f"Optimized Silver table: {silver_table_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Update Control Table and Logging

# COMMAND ----------

# Update pipeline control table with run status
control_entry = {
    "pipeline_name": f"{source_system}_{target_table}",
    "batch_id": batch_id,
    "load_date": load_date,
    "source_system": source_system,
    "target_table": target_table,
    "status": "SUCCESS",
    "bronze_count": bronze_count,
    "silver_count": silver_count,
    "dq_errors": dq_fail_count,
    "dq_warnings": dq_warn_count,
    "watermark_value": str(datetime.now()),
    "run_timestamp": datetime.now().isoformat(),
}

control_df = spark.createDataFrame([control_entry])
control_df.write.format("delta").mode("append").saveAsTable(f"{metadata_schema}.pipeline_control")

logger.info(f"Pipeline control updated: {json.dumps(control_entry, indent=2)}")
logger.info(f"Silver transformation complete: {silver_table_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Error Handling

# COMMAND ----------

# NOTE: This cell is a catch-all for unexpected errors.
# In Databricks, unhandled exceptions will fail the notebook run.
# The job orchestration layer (ADF/Airflow) should handle retries.

try:
    assert silver_count > 0 or full_refresh, "Unexpected: 0 records processed in non-full-refresh mode"
    logger.info("All assertions passed — notebook execution successful")
except AssertionError as e:
    logger.error(f"Post-write assertion failed: {e}")
    raise
except Exception as e:
    logger.error(f"Unexpected error: {e}", exc_info=True)

    # Log failure to control table
    fail_entry = {
        "pipeline_name": f"{source_system}_{target_table}",
        "batch_id": batch_id,
        "load_date": load_date,
        "source_system": source_system,
        "target_table": target_table,
        "status": "FAILED",
        "error_message": str(e)[:500],
        "run_timestamp": datetime.now().isoformat(),
    }
    fail_df = spark.createDataFrame([fail_entry])
    fail_df.write.format("delta").mode("append").saveAsTable(f"{metadata_schema}.pipeline_control")
    raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## End of Notebook
