# =============================================================================
# conftest.py — Shared Pytest Fixtures for Data Engineering Tests
# =============================================================================
# Place this file at the root of your test directory or alongside tests.
# Fixtures are auto-discovered by pytest for all test modules in scope.
# =============================================================================

import os
import json
import shutil
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import yaml

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
TEST_DATA_DIR = Path(__file__).parent / "fixtures"
DQ_RULES_FILE = TEST_DATA_DIR / "dq-rules.yaml"
METADATA_CONFIG_FILE = TEST_DATA_DIR / "metadata-config.yaml"


# =============================================================================
# Spark Session Fixture
# =============================================================================
@pytest.fixture(scope="session")
def spark():
    """
    Provides a shared SparkSession for the entire test session.
    Configured for local execution with Delta Lake support.
    """
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder
        .appName("pytest-data-eng-tests")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()


# =============================================================================
# Sample Delta Table Fixture
# =============================================================================
@pytest.fixture(scope="function")
def sample_delta_table(spark, tmp_path):
    """
    Creates a temporary Delta table with sample customer data.
    Table is cleaned up after each test function.
    """
    from delta.tables import DeltaTable
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, TimestampType, BooleanType
    )

    schema = StructType([
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("first_name", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=True),
        StructField("is_active", BooleanType(), nullable=True),
    ])

    data = [
        (1, "John", "Doe", "john.doe@example.com", "active", "2024-01-15 10:00:00", True),
        (2, "Jane", "Smith", "jane.smith@example.com", "active", "2024-02-20 14:30:00", True),
        (3, "Bob", "Johnson", None, "inactive", "2024-03-10 09:15:00", False),
        (4, "Alice", "Williams", "alice.w@example.com", "pending", "2024-04-05 16:45:00", True),
        (5, None, "Brown", "bob.b@example.com", "active", "2024-05-12 11:20:00", True),
    ]

    table_path = str(tmp_path / "sample_delta_table")

    df = spark.createDataFrame(data, schema)
    df.write.format("delta").mode("overwrite").save(table_path)

    yield DeltaTable.forPath(spark, table_path), table_path

    # Cleanup
    shutil.rmtree(tmp_path, ignore_errors=True)


# =============================================================================
# Test Data Path Fixture
# =============================================================================
@pytest.fixture(scope="session")
def test_data_path():
    """
    Returns the path to the test fixtures directory.
    Raises if directory does not exist.
    """
    if not TEST_DATA_DIR.exists():
        pytest.fail(f"Test data directory not found: {TEST_DATA_DIR}")
    return TEST_DATA_DIR


@pytest.fixture(scope="function")
def test_output_dir():
    """
    Creates and returns a temporary directory for test outputs.
    Cleaned up after the test function completes.
    """
    tmp_dir = tempfile.mkdtemp(prefix="pytest_output_")
    yield Path(tmp_dir)
    shutil.rmtree(tmp_dir, ignore_errors=True)


# =============================================================================
# DQ Rules Loader Fixture
# =============================================================================
@pytest.fixture(scope="session")
def dq_rules():
    """
    Loads data quality rules from the DQ rules YAML fixture file.
    Returns a list of rule dictionaries.
    """
    if not DQ_RULES_FILE.exists():
        pytest.skip(f"DQ rules file not found: {DQ_RULES_FILE}")
    with open(DQ_RULES_FILE, "r") as f:
        config = yaml.safe_load(f)
    return config.get("rules", [])


# =============================================================================
# Metadata Config Loader Fixture
# =============================================================================
@pytest.fixture(scope="session")
def metadata_config():
    """
    Loads the metadata configuration YAML fixture.
    Returns the parsed configuration dictionary.
    """
    if not METADATA_CONFIG_FILE.exists():
        pytest.skip(f"Metadata config file not found: {METADATA_CONFIG_FILE}")
    with open(METADATA_CONFIG_FILE, "r") as f:
        return yaml.safe_load(f)


# =============================================================================
# Mock ADF Pipeline Fixture
# =============================================================================
@pytest.fixture
def mock_adf_pipeline():
    """
    Provides a mock Azure Data Factory pipeline client for testing
    pipeline triggers and parameter passing without hitting ADF.
    """
    mock_client = MagicMock()
    mock_run = MagicMock()
    mock_run.run_id = "test-run-12345"
    mock_run.status = "Succeeded"
    mock_client.create_run.return_value = mock_run
    mock_client.get_run.return_value = mock_run
    mock_client.cancel_run.return_value = MagicMock(status="Cancelled")
    yield mock_client


# =============================================================================
# Mock Databricks Connection Fixture
# =============================================================================
@pytest.fixture
def mock_databricks_connection():
    """
    Provides a mock Databricks connection for testing notebook execution
    and cluster interactions without a real Databricks workspace.
    """
    with patch("databricks.sdk.WorkspaceClient") as mock_ws_class:
        mock_ws = MagicMock()

        # Mock notebook execution
        mock_run = MagicMock()
        mock_run.run_id = "test-notebook-run-001"
        mock_run.state.life_cycle_state = "TERMINATED"
        mock_run.state.result_state = "SUCCESS"
        mock_ws.notebooks.start_run.return_value = mock_run
        mock_ws.notebooks.get_run.return_value = mock_run

        # Mock cluster
        mock_cluster = MagicMock()
        mock_cluster.cluster_id = "test-cluster-001"
        mock_cluster.state = "RUNNING"
        mock_ws.clusters.get.return_value = mock_cluster

        # Mock DBFS
        mock_ws.dbfs.read.return_value = MagicMock(data=b'{"status": "ok"}')
        mock_ws.dbfs.exists.return_value = True

        mock_ws_class.return_value = mock_ws
        yield mock_ws


# =============================================================================
# Spark DataFrame Equality Helper Fixture
# =============================================================================
@pytest.fixture
def assert_df_equal(spark):
    """
    Returns a helper function that asserts two DataFrames are equal
    by comparing schema and row content (order-insensitive).
    """
    def _assert_df_equal(actual, expected, check_order=False):
        if not check_order:
            actual = actual.orderBy([c.name for c in actual.schema.fields])
            expected = expected.orderBy([c.name for c in expected.schema.fields])

        # Compare schemas
        assert actual.schema == expected.schema, (
            f"Schema mismatch:\nActual:   {actual.schema}\nExpected: {expected.schema}"
        )

        # Compare row counts
        assert actual.count() == expected.count(), (
            f"Row count mismatch: actual={actual.count()}, expected={expected.count()}"
        )

        # Compare content
        actual_rows = {tuple(row) for row in actual.collect()}
        expected_rows = {tuple(row) for row in expected.collect()}
        assert actual_rows == expected_rows, (
            f"Row content mismatch:\n"
            f"In actual but not expected: {actual_rows - expected_rows}\n"
            f"In expected but not actual: {expected_rows - actual_rows}"
        )

    return _assert_df_equal


# =============================================================================
# Environment Fixture
# =============================================================================
@pytest.fixture(autouse=True, scope="function")
def env_test_settings(tmp_path):
    """
    Sets environment variables for the test session.
    Automatically applied to all tests.
    """
    env_vars = {
        "ENVIRONMENT": "test",
        "SPARK_HOME": os.environ.get("SPARK_HOME", ""),
        "DATABRICKS_HOST": "https://test-workspace.azuredatabricks.net",
        "DATABRICKS_TOKEN": "test-token-not-real",
        "ADLS_ACCOUNT": "teststorageaccount",
        "ADLS_CONTAINER": "test-data",
        "METADATA_DB": "test_metadata_db",
    }
    old_values = {}
    for key, value in env_vars.items():
        old_values[key] = os.environ.get(key)
        os.environ[key] = value

    yield

    for key, old_val in old_values.items():
        if old_val is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = old_val
