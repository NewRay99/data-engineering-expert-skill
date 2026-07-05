# Testing and Test Plans

## Overview

Testing in data engineering is fundamentally different from application testing. Data pipelines deal with **volume, schema drift, data quality, and idempotency** — concerns that traditional unit testing frameworks don't natively address. This document defines a comprehensive testing strategy covering unit tests, integration tests, data quality tests, end-to-end tests, and performance tests across Azure Data Factory, Databricks, and the broader medallion architecture.

---

## 1. Testing Pyramid for Data Engineering

```
            ┌──────────┐
            │   E2E    │   ← 5% (few, slow, full pipeline)
            ├──────────┤
            │ Integration │  ← 15% (ADF pipeline + Databricks notebook)
            ├──────────┤
            │   DQ     │   ← 30% (data quality assertions on real data)
            ├──────────┤
            │   Unit   │   ← 50% (fast, isolated, DAX/SQL/Python functions)
            └──────────┘
```

| Layer | What | Where | Speed | Count |
|---|---|---|---|---|
| **Unit** | Individual functions, SQL snippets, DAX measures | pytest / Databricks unit testing | < 1s each | Many |
| **Data Quality** | Assertions on actual data (nullability, uniqueness, ranges) | Great Expectations / DQ framework in pipeline | Seconds | Per-source |
| **Integration** | Pipeline activities chained together with real services | ADF test pipeline runs, Databricks job runs | Minutes | Few per pipeline |
| **End-to-End** | Source → Bronze → Silver → Gold → Semantic Model | Triggered test pipeline in test environment | 10–30 min | 1–2 per major release |
| **Performance** | Load testing with production-scale data volumes | Dedicated performance test environment | 30+ min | Quarterly |

---

## 2. Unit Testing

### 2.1 Python (Databricks Notebooks)

Use `pytest` with the `chispa` library for DataFrame comparisons.

```python
# test_transformations.py
import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from src.transformations import apply_hash_key, trim_and_upper, epoch_to_timestamp

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("unit-tests") \
        .getOrCreate()


def test_apply_hash_key(spark):
    """Hash key should produce consistent SHA-256 output."""
    input_data = [("C001", "ACME Corp"), ("C002", "Globex Inc")]
    input_df = spark.createDataFrame(input_data, ["customer_id", "customer_name"])

    result_df = apply_hash_key(input_df, key_columns=["customer_id"])

    expected_data = [("C001", "ACME Corp", "a1b2c3d4..."), ("C002", "Globex Inc", "e5f6g7h8...")]
    expected_df = spark.createDataFrame(expected_data, ["customer_id", "customer_name", "hash_key"])

    assert_df_equality(result_df, expected_df, ignore_nullable=True)


def test_trim_and_upper(spark):
    """Should trim whitespace and convert to uppercase."""
    input_data = [("  acme corp  "), ("  Globex  ")]
    input_df = spark.createDataFrame(input_data, ["raw_name"])

    result_df = trim_and_upper(input_df, "raw_name", "clean_name")

    expected_data = [("  acme corp  ", "ACME CORP"), ("  Globex  ", "GLOBEX")]
    expected_df = spark.createDataFrame(expected_data, ["raw_name", "clean_name"])

    assert_df_equality(result_df, expected_df)


def test_epoch_to_timestamp(spark):
    """Should convert epoch milliseconds to timestamp."""
    input_data = [(1609459200000,), (1609545600000,)]
    input_df = spark.createDataFrame(input_data, ["epoch_ms"])

    result_df = epoch_to_timestamp(input_df, "epoch_ms", "ts")

    assert result_df.count() == 2
    assert result_df.filter("ts IS NULL").count() == 0
    assert str(result_df.collect()[0]["ts"]).startswith("2021-01-01")
```

### 2.2 SQL (Databricks SQL / Azure SQL)

Use `sqltest` or a pytest-based SQL test runner.

```sql
-- test_dq_rules.sql
-- Test: NOT_NULL rule should flag null values

CREATE OR REPLACE TEMP VIEW test_dq_input AS
SELECT 'C001' AS customer_id, 'Active' AS status UNION ALL
SELECT NULL  AS customer_id, 'Active' AS status UNION ALL
SELECT 'C003' AS customer_id, NULL     AS status;

-- Expected: 1 violation (NULL customer_id in row 2)
SELECT COUNT(*) AS violation_count
FROM test_dq_input
WHERE customer_id IS NULL;
-- Assertion: violation_count = 1
```

### 2.3 DAX (Power BI Semantic Model)

Use DAX Query operations via the modeling MCP tools to validate measures.

```dax
// Test: Total Revenue should equal sum of line items
EVALUATE
{
    ("Total Revenue Check",
        [Total Revenue],
        SUMX(Sales, Sales[Quantity] * Sales[UnitPrice])
    )
}
// Assertion: [Total Revenue] == SUMX(Sales, Sales[Quantity] * Sales[UnitPrice])
```

Validate via DAX query execution:

```dax
EVALUATE
FILTER(
    SUMMARIZE(Sales, Sales[CustomerKey]),
    [Total Revenue] < 0
)
// Assertion: should return 0 rows (no negative revenue)
```

---

## 3. Data Quality Testing

### 3.1 Great Expectations Integration

```python
# dq_suite.py
import great_expectations as gx
from great_expectations.core.expectation_configuration import ExpectationConfiguration

def build_dq_suite(source_name, column_mappings, dq_rules):
    """Build a Great Expectations suite from metadata."""
    suite = gx.core.expectation_suite.ExpectationSuite(name=f"{source_name}_suite")

    # Always expect table row count > 0
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_greater_than",
            kwargs={"value": 0}
        )
    )

    for rule in dq_rules:
        if rule["rule_type"] == "NOT_NULL":
            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_not_be_null",
                    kwargs={"column": rule["column_name"]}
                )
            )
        elif rule["rule_type"] == "UNIQUE":
            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_unique",
                    kwargs={"column": rule["column_name"]}
                )
            )
        elif rule["rule_type"] == "RANGE":
            # Parse "amount BETWEEN 0 AND 1000000"
            parts = rule["rule_expression"].split("BETWEEN")
            min_val = float(parts[1].split("AND")[0].strip())
            max_val = float(parts[1].split("AND")[1].strip())
            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_between",
                    kwargs={
                        "column": rule["column_name"],
                        "min_value": min_val,
                        "max_value": max_val
                    }
                )
            )
        elif rule["rule_type"] == "REGEX":
            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_match_regex",
                    kwargs={
                        "column": rule["column_name"],
                        "regex": rule["rule_expression"]
                    }
                )
            )

    return suite
```

### 3.2 DQ Test Categories

| Category | Example Rule | Severity | Action |
|---|---|---|---|
| **Completeness** | `customer_id IS NOT NULL` | ERROR | Fail pipeline |
| **Uniqueness** | `transaction_id is unique` | ERROR | Fail pipeline |
| **Validity** | `email RLIKE '^[^@]+@[^@]+\.[^@]+$'` | WARN | Log + quarantine |
| **Consistency** | `order_total = SUM(line_total)` | ERROR | Fail pipeline |
| **Accuracy** | `age BETWEEN 0 AND 150` | WARN | Log only |
| **Timeliness** | `load_time < NOW() - INTERVAL 1 HOUR` | INFO | Alert |
| **Referential** | `customer_id EXISTS IN dim_customer` | ERROR | Quarantine |

---

## 4. Integration Testing

### 4.1 ADF Pipeline Integration Tests

Integration tests verify that pipeline activities work together correctly with real (test-environment) services.

```python
# test_adf_pipeline.py
"""
Integration test: Trigger an ADF pipeline run and validate outputs.
Requires: Azure CLI authenticated, test resource group deployed.
"""
import pytest
import time
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

ADF_SUBSCRIPTION_ID = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
ADF_RESOURCE_GROUP = "rg-dataeng-test"
ADF_FACTORY_NAME = "adf-dataeng-test"

credential = DefaultAzureCredential()
adf_client = DataFactoryManagementClient(credential, ADF_SUBSCRIPTION_ID)


@pytest.fixture(scope="module")
def pipeline_run():
    """Trigger the metadata-driven ingestion pipeline in test environment."""
    run_response = adf_client.pipelines.create_run(
        resource_group_name=ADF_RESOURCE_GROUP,
        factory_name=ADF_FACTORY_NAME,
        pipeline_name="pl_metadata_driven_ingestion",
        parameters={
            "LoadFrequency": "Daily",
            "BronzeContainer": "bronze-test"
        }
    )
    run_id = run_response.run_id

    # Poll for completion (max 10 minutes)
    for _ in range(60):
        run = adf_client.pipeline_runs.get(
            resource_group_name=ADF_RESOURCE_GROUP,
            factory_name=ADF_FACTORY_NAME,
            run_id=run_id
        )
        if run.status in ("Succeeded", "Failed", "Cancelled"):
            break
        time.sleep(10)

    return run


def test_pipeline_succeeded(pipeline_run):
    assert pipeline_run.status == "Succeeded", \
        f"Pipeline failed with status: {pipeline_run.status}"


def test_pipeline_activity_count(pipeline_run):
    """Verify expected number of activities executed."""
    activity_runs = list(adf_client.activity_runs.list_by_pipeline_run(
        resource_group_name=ADF_RESOURCE_GROUP,
        factory_name=ADF_FACTORY_NAME,
        run_id=pipeline_run.run_id
    ))
    assert len(activity_runs) > 0, "No activities executed"
    # Verify no activity failed
    failed = [a for a in activity_runs if a.status == "Failed"]
    assert len(failed) == 0, f"{len(failed)} activities failed"


def test_bronze_files_created(pipeline_run):
    """Verify Bronze layer files were created in ADLS Gen2."""
    from azure.storage.filedatalake import DataLakeServiceClient

    service = DataLakeServiceClient(
        account_url="https://stdatatest.dfs.core.windows.net",
        credential=credential
    )
    fs_client = service.get_file_system_client("bronze-test")

    files = list(fs_client.get_paths(recursive=True))
    parquet_files = [f for f in files if f.name.endswith(".parquet")]
    assert len(parquet_files) > 0, "No parquet files found in Bronze layer"
```

### 4.2 Databricks Notebook Integration Tests

```python
# test_databricks_notebook.py
"""
Integration test: Execute a Databricks notebook via Jobs API and validate output.
"""
import pytest
import requests
import time

DATABRICKS_HOST = "https://adb-1234567890123456.7.azuredatabricks.net"
TOKEN = "<databricks-token>"  # Use Azure Key Vault in production


def run_notebook(notebook_path, params):
    """Submit a notebook run via Databricks Jobs API."""
    response = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/runs/submit",
        headers={"Authorization": f"Bearer {TOKEN}"},
        json={
            "run_name": "integration-test",
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": params
            },
            "clusters": {
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "node_type_id": "Standard_DS3_v2",
                    "num_workers": 1
                }
            }
        }
    )
    run_id = response.json()["run_id"]

    # Poll for completion
    for _ in range(120):
        status_response = requests.get(
            f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get?run_id={run_id}",
            headers={"Authorization": f"Bearer {TOKEN}"}
        )
        state = status_response.json()["state"]["life_cycle_state"]
        if state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        time.sleep(10)

    result_state = status_response.json()["state"].get("result_state")
    return run_id, result_state


def test_silver_transformation_notebook():
    run_id, result_state = run_notebook(
        "/Shared/silver/transform_customers",
        {"source_name": "sap_customers", "environment": "test"}
    )
    assert result_state == "SUCCESS", f"Notebook run {run_id} failed with state: {result_state}"
```

---

## 5. End-to-End Testing

### 5.1 Test Plan Template

```
Test ID: E2E-001
Test Name: Full Customer 360 Pipeline (Source → Gold)
Description: Verify the complete pipeline from SAP source extraction through
             Bronze, Silver, Gold layers to Power BI semantic model refresh.

Prerequisites:
  - Test environment deployed with test data fixtures
  - SAP mock database populated with 10,000 customer records
  - ADLS Gen2 test containers provisioned
  - Databricks test cluster running
  - Power BI test workspace accessible

Steps:
  1. Trigger ADF pipeline "pl_customer_360_full" with test parameters
  2. Wait for pipeline completion (timeout: 30 minutes)
  3. Verify Bronze layer:
     - File count matches expected source table count
     - Row count in parquet == source row count
  4. Verify Silver layer:
     - Delta table exists with correct schema
     - No NULL values in primary key columns
     - Referential integrity: all FK values exist in dimension tables
  5. Verify Gold layer:
     - Aggregated measures match expected values
     - Row count in fact table <= Silver row count (post-dedup)
  6. Trigger Power BI dataset refresh
  7. Execute DAX query to verify measure returns expected value
  8. Verify DQ results table has all rules marked as PASS

Expected Results:
  - Pipeline status: Succeeded
  - Bronze: 10,000 rows, 15 parquet files
  - Silver: 9,985 rows (15 duplicates removed)
  - Gold: 9,985 rows, Total Revenue = $45,231,789.00
  - DQ: All 12 rules PASS
  - Power BI: [Total Revenue] = $45,231,789.00

Pass Criteria: All steps pass without manual intervention.
```

### 5.2 Automated E2E Test

```python
# test_e2e_customer_360.py
"""
End-to-end test: Full pipeline from source to semantic model.
This is the most expensive test — run only on major releases.
"""
import pytest
import time

@pytest.mark.e2e
@pytest.mark.slow
class TestCustomer360E2E:

    def test_full_pipeline(self, adf_client, spark, pbi_connection):
        # Step 1: Trigger pipeline
        run = adf_client.pipelines.create_run(
            resource_group_name=ADF_RESOURCE_GROUP,
            factory_name=ADF_FACTORY_NAME,
            pipeline_name="pl_customer_360_full",
            parameters={"Environment": "test"}
        )
        run_id = run.run_id

        # Step 2: Wait for completion
        self._wait_for_pipeline(adf_client, run_id, timeout=1800)

        # Step 3: Verify Bronze
        bronze_df = spark.read.format("parquet") \
            .load("abfss://bronze-test@storage.dfs.core.windows.net/sap_customers/")
        assert bronze_df.count() == 10000, "Bronze row count mismatch"

        # Step 4: Verify Silver
        silver_df = spark.read.format("delta") \
            .load("abfss://silver-test@storage.dfs.core.windows.net/customers/")
        assert silver_df.count() == 9985, "Silver row count mismatch (dedup failed?)"
        assert silver_df.filter("customer_id IS NULL").count() == 0, "NULL customer_id in Silver"

        # Step 5: Verify Gold
        gold_df = spark.read.format("delta") \
            .load("abfss://gold-test@storage.dfs.core.windows.net/customer_360/")
        total_revenue = gold_df.agg({"revenue": "sum"}).collect()[0][0]
        assert total_revenue == pytest.approx(45231789.00, rel=0.01), \
            f"Gold total revenue mismatch: {total_revenue}"

        # Step 6: Verify DQ results
        dq_df = spark.read.format("delta") \
            .load("abfss://gold-test@storage.dfs.core.windows.net/dq_results/")
        failed_rules = dq_df.filter("passed = false").count()
        assert failed_rules == 0, f"{failed_rules} DQ rules failed"

        # Step 7: Verify Power BI semantic model
        # Using DAX query via MCP tools
        dax_result = pbi_connection.execute("""
            EVALUATE
            ROW("Total Revenue", [Total Revenue])
        """)
        pbi_revenue = dax_result["rows"][0]["[Total Revenue]"]
        assert pbi_revenue == pytest.approx(45231789.00, rel=0.01), \
            f"Power BI measure mismatch: {pbi_revenue}"

    def _wait_for_pipeline(self, adf_client, run_id, timeout=1800):
        start = time.time()
        while time.time() - start < timeout:
            run = adf_client.pipeline_runs.get(
                resource_group_name=ADF_RESOURCE_GROUP,
                factory_name=ADF_FACTORY_NAME,
                run_id=run_id
            )
            if run.status in ("Succeeded", "Failed", "Cancelled"):
                assert run.status == "Succeeded", f"Pipeline status: {run.status}"
                return
            time.sleep(15)
        pytest.fail(f"Pipeline timed out after {timeout}s")
```

---

## 6. Performance Testing

### 6.1 Performance Test Plan

| Metric | Target | Measurement Method |
|---|---|---|
| Bronze ingestion (1M rows) | < 5 minutes | ADF pipeline duration |
| Silver transformation (1M rows) | < 10 minutes | Databricks job duration |
| Gold aggregation (1M rows) | < 5 minutes | Databricks job duration |
| DQ rules (1M rows, 20 rules) | < 2 minutes | Databricks job duration |
| Power BI dataset refresh | < 15 minutes | Power BI refresh API |
| Full E2E (10M rows) | < 60 minutes | Total wall-clock time |

### 6.2 Performance Test Script

```python
# test_performance.py
"""
Performance test: Measure pipeline throughput with production-scale data.
Run quarterly or before major releases.
"""
import pytest
import time

@pytest.mark.performance
@pytest.mark.slow
class TestPerformance:

    @pytest.mark.parametrize("row_count", [100000, 1000000, 10000000])
    def test_bronze_ingestion_throughput(self, adf_client, row_count):
        """Measure Bronze layer ingestion time for varying data volumes."""
        start = time.time()

        run = adf_client.pipelines.create_run(
            resource_group_name=ADF_RESOURCE_GROUP,
            factory_name=ADF_FACTORY_NAME,
            pipeline_name="pl_perf_test_bronze",
            parameters={"RowCount": str(row_count), "Environment": "perf"}
        )

        self._wait_for_completion(adf_client, run.run_id)
        duration = time.time() - start

        throughput = row_count / duration
        print(f"\n  Rows: {row_count:,}, Duration: {duration:.1f}s, Throughput: {throughput:,.0f} rows/s")

        # Assert minimum throughput
        min_throughput = 5000  # rows per second
        assert throughput >= min_throughput, \
            f"Throughput {throughput:,.0f} rows/s below minimum {min_throughput:,} rows/s"

        # Record for trend analysis
        self._record_metric("bronze_ingestion", row_count, duration, throughput)
```

---

## 7. CI/CD Test Integration

### 7.1 Azure DevOps Pipeline

```yaml
# azure-pipelines-test.yml
trigger:
  branches:
    include:
      - main
      - develop
      - release/*

stages:
  - stage: UnitTests
    jobs:
      - job: RunUnitTests
        pool:
          vmImage: 'ubuntu-latest'
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: '3.11'
          - script: |
              pip install pytest chispa pyspark==3.5.0
              pytest tests/unit/ --junitxml=test-results.xml --tb=short
            displayName: 'Run Unit Tests'
          - task: PublishTestResults@2
            inputs:
              testResultsFiles: 'test-results.xml'
              testRunTitle: 'Unit Tests'

  - stage: IntegrationTests
    dependsOn: UnitTests
    condition: succeeded()
    jobs:
      - job: RunIntegrationTests
        pool:
          vmImage: 'ubuntu-latest'
        steps:
          - task: AzureKeyVault@2
            inputs:
              azureSubscription: 'dataeng-service-connection'
              keyVaultName: 'kv-dataeng-test'
          - script: |
              pip install pytest azure-identity azure-mgmt-datafactory
              export AZURE_TENANT_ID=$(tenant-id)
              export AZURE_CLIENT_ID=$(client-id)
              export AZURE_CLIENT_SECRET=$(client-secret)
              pytest tests/integration/ --junitxml=integration-results.xml -m integration
            displayName: 'Run Integration Tests'
          - task: PublishTestResults@2
            inputs:
              testResultsFiles: 'integration-results.xml'
              testRunTitle: 'Integration Tests'

  - stage: E2ETests
    dependsOn: IntegrationTests
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    jobs:
      - job: RunE2ETests
        timeoutInMinutes: 60
        steps:
          - script: |
              pip install pytest azure-identity azure-mgmt-datafactory
              pytest tests/e2e/ --junitxml=e2e-results.xml -m e2e
            displayName: 'Run E2E Tests'
          - task: PublishTestResults@2
            inputs:
              testResultsFiles: 'e2e-results.xml'
              testRunTitle: 'E2E Tests'
```

### 7.2 Test Markers

```python
# conftest.py
import pytest

def pytest_configure(config):
    config.addinivalue_line("markers", "unit: fast isolated tests")
    config.addinivalue_line("markers", "integration: tests requiring Azure services")
    config.addinivalue_line("markers", "e2e: full pipeline end-to-end tests")
    config.addinivalue_line("markers", "performance: load and throughput tests")
    config.addinivalue_line("markers", "slow: tests taking > 60 seconds")
    config.addinivalue_line("markers", "dq: data quality assertion tests")
```

---

## 8. Test Data Management

### 8.1 Fixtures

```python
# fixtures/data_fixtures.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test-fixtures").getOrCreate()


@pytest.fixture
def sample_customers(spark):
    """10-row customer fixture for unit tests."""
    data = [
        ("C001", "ACME Corp",   "acme@example.com",    "2023-01-15", 150000.00),
        ("C002", "Globex Inc",  "globex@example.com",  "2023-02-20", 75000.00),
        ("C003", "Initech",     "info@initech.com",    "2023-03-10", 200000.00),
        ("C004", "Umbrella Co", "contact@umbrella.com","2023-04-05", 50000.00),
        ("C005", "Stark Inds",  "hello@stark.com",     "2023-05-12", 300000.00),
        ("C006", "Wayne Ent",   "office@wayne.com",    "2023-06-18", 175000.00),
        ("C007", "LexCorp",     "admin@lexcorp.com",   "2023-07-22", 90000.00),
        ("C008", "Cyberdyne",   "sys@cyberdyne.com",   "2023-08-30", 110000.00),
        ("C009", "Soylent Co",  "mail@soylent.com",    "2023-09-14", 65000.00),
        ("C010", "Tyrell Corp", "hr@tyrell.com",       "2023-10-01", 220000.00),
    ]
    schema = "customer_id STRING, customer_name STRING, email STRING, created_date STRING, revenue DOUBLE"
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_with_nulls(sample_customers, spark):
    """Same fixture but with some NULL values for DQ testing."""
    return sample_customers.withColumn(
        "email",
        when(col("customer_id") == "C003", lit(None)).otherwise(col("email"))
    )
```

### 8.2 Synthetic Data Generation

```python
# utils/synthetic_data.py
from faker import Faker
import random

def generate_customer_data(num_rows=10000):
    """Generate synthetic customer data for performance testing."""
    fake = Faker()
    rows = []
    for i in range(num_rows):
        rows.append((
            f"C{i:06d}",
            fake.company(),
            fake.email(),
            fake.date_between(start_date="-2y", end_date="today").isoformat(),
            round(random.uniform(10000, 500000), 2)
        ))
    return rows
```

---

## 9. Test Plan Checklist

Before each release, ensure the following checklist is completed:

- [ ] All unit tests pass (coverage ≥ 80%)
- [ ] All DQ rules in metadata have corresponding test assertions
- [ ] Integration tests pass in test environment
- [ ] E2E test passes for at least one critical pipeline
- [ ] Performance test run within last quarter (or before major release)
- [ ] Test data fixtures are up-to-date with current schema
- [ ] Test results published to Azure DevOps
- [ ] Any failing tests have associated bug tickets
- [ ] DAX measures validated against expected values
- [ ] Rollback procedure tested (pipeline can be reverted without data loss)

---

## 10. Summary

A robust testing strategy for data engineering must cover:

1. **Unit tests** for individual transformation functions (50% of tests)
2. **Data quality tests** integrated into the pipeline via metadata-driven DQ rules (30%)
3. **Integration tests** verifying pipeline activities chain correctly (15%)
4. **End-to-end tests** validating the full source-to-semantic-model flow (5%)
5. **Performance tests** ensuring throughput meets SLAs (quarterly)

The key principles: test early, test often, automate everything, and treat test data as a first-class artifact with its own lifecycle and versioning.
