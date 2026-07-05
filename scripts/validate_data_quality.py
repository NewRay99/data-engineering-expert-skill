#!/usr/bin/env python3
"""
Data Quality Validator
======================
Reads DQ rules from a YAML file and validates a Delta table or Spark DataFrame
against them. Supports six check types: format, range, domain, calculation,
completeness, and uniqueness.

Usage:
    python validate_data_quality.py \
        --rules rules.yaml \
        --table catalog.schema.table \
        --delta-path /path/to/delta

Exit codes:
    0 = all rules passed
    1 = warnings only (some rules failed but no critical errors)
    2 = errors (critical failures or exceptions)
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

# ---------------------------------------------------------------------------
# Logging — structured JSON-ish output
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("dq-validator")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class DQRule:
    """A single data-quality rule parsed from YAML."""
    rule_id: str
    rule_type: str          # format | range | domain | calculation | completeness | uniqueness
    column: Optional[str] = None
    description: str = ""
    severity: str = "error"  # error | warning
    # format
    pattern: Optional[str] = None
    # range
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    # domain
    allowed_values: Optional[List[Any]] = None
    # calculation
    expression: Optional[str] = None        # SQL expression expected to evaluate to True
    # completeness
    allow_null: bool = True
    null_threshold: float = 0.0             # fraction of nulls allowed (0 = no nulls)
    # uniqueness
    unique_columns: Optional[List[str]] = None


@dataclass
class DQResult:
    """Result of evaluating a single rule."""
    rule_id: str
    rule_type: str
    column: Optional[str]
    severity: str
    passed: bool
    failed_records: int = 0
    total_records: int = 0
    message: str = ""


@dataclass
class DQReport:
    """Aggregate report across all rules."""
    results: List[DQResult] = field(default_factory=list)
    table_name: str = ""
    run_timestamp: str = ""

    @property
    def total_rules(self) -> int:
        return len(self.results)

    @property
    def passed_rules(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def failed_errors(self) -> int:
        return sum(1 for r in self.results if not r.passed and r.severity == "error")

    @property
    def failed_warnings(self) -> int:
        return sum(1 for r in self.results if not r.passed and r.severity == "warning")

    @property
    def score(self) -> float:
        """Percentage of rules that passed (0–100)."""
        if not self.results:
            return 0.0
        return round(self.passed_rules / self.total_rules * 100, 2)

    @property
    def exit_code(self) -> int:
        if self.failed_errors > 0:
            return 2
        if self.failed_warnings > 0:
            return 1
        return 0


# ---------------------------------------------------------------------------
# YAML rule loader
# ---------------------------------------------------------------------------

def load_rules(yaml_path: str) -> List[DQRule]:
    """Load data-quality rules from a YAML file.

    Expected YAML structure (see templates/dq-rules-template.yaml):

        rules:
          - rule_id: dq_001
            rule_type: completeness
            column: customer_id
            severity: error
            null_threshold: 0.0
            description: "customer_id must not be null"
          - rule_id: dq_002
            rule_type: uniqueness
            unique_columns: [customer_id]
            severity: error
            ...
    """
    path = Path(yaml_path)
    if not path.exists():
        raise FileNotFoundError(f"Rules file not found: {yaml_path}")

    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    if not data or "rules" not in data:
        raise ValueError(f"No 'rules' key found in {yaml_path}")

    rules: List[DQRule] = []
    for raw in data["rules"]:
        rules.append(DQRule(
            rule_id=raw["rule_id"],
            rule_type=raw["rule_type"],
            column=raw.get("column"),
            description=raw.get("description", ""),
            severity=raw.get("severity", "error"),
            pattern=raw.get("pattern"),
            min_value=raw.get("min_value"),
            max_value=raw.get("max_value"),
            allowed_values=raw.get("allowed_values"),
            expression=raw.get("expression"),
            allow_null=raw.get("allow_null", True),
            null_threshold=raw.get("null_threshold", 0.0),
            unique_columns=raw.get("unique_columns"),
        ))

    logger.info(f"Loaded {len(rules)} DQ rules from {yaml_path}")
    return rules


# ---------------------------------------------------------------------------
# Spark helpers
# ---------------------------------------------------------------------------

def get_spark(app_name: str = "DQ-Validator"):
    """Initialise and return a Spark session."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        logger.error("pyspark is not installed. Install with: pip install pyspark")
        raise

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark session initialised: {spark.version}")
    return spark


def load_dataframe(spark, table: Optional[str] = None, delta_path: Optional[str] = None):
    """Load a DataFrame from a catalog table or a Delta path."""
    if table:
        logger.info(f"Loading table: {table}")
        return spark.table(table)
    if delta_path:
        logger.info(f"Loading Delta path: {delta_path}")
        return spark.read.format("delta").load(delta_path)
    raise ValueError("Either --table or --delta-path must be provided.")


# ---------------------------------------------------------------------------
# Check implementations
# ---------------------------------------------------------------------------

def _total_count(df) -> int:
    return df.count()


def check_format(df, rule: DQRule, total: int) -> DQResult:
    """Regex format check on a string column."""
    if not rule.pattern or not rule.column:
        return DQResult(rule.rule_id, rule.rule_type, rule.column, rule.severity,
                        passed=False, total_records=total,
                        message="Missing 'pattern' or 'column' for format rule.")

    from pyspark.sql.functions import col, regexp

    # Nulls are handled by completeness rules — treat them as pass for format.
    viol = df.filter(col(rule.column).isNotNull() & ~col(rule.column).rlike(rule.pattern))
    failed = viol.count()
    return DQResult(
        rule.rule_id, rule.rule_type, rule.column, rule.severity,
        passed=failed == 0,
        failed_records=failed,
        total_records=total,
        message=f"{failed} records failed regex '{rule.pattern}' on '{rule.column}'.",
    )


def check_range(df, rule: DQRule, total: int) -> DQResult:
    """Numeric range check (min/max)."""
    if not rule.column:
        return DQResult(rule.rule_id, rule.rule_type, rule.column, rule.severity,
                        passed=False, total_records=total,
                        message="Missing 'column' for range rule.")

    from pyspark.sql.functions import col

    conditions = []
    if rule.min_value is not None:
        conditions.append(col(rule.column) < rule.min_value)
    if rule.max_value is not None:
        conditions.append(col(rule.column) > rule.max_value)
    if not conditions:
        return DQResult(rule.rule_id, rule.rule_type, rule.column, rule.severity,
                        passed=False, total_records=total,
                        message="No min_value/max_value specified for range rule.")

    combined = conditions[0]
    for c in conditions[1:]:
        combined = combined | c

    viol = df.filter(col(rule.column).isNotNull() & combined)
    failed = viol.count()
    lo = rule.min_value if rule.min_value is not None else "-∞"
    hi = rule.max_value if rule.max_value is not None else "∞"
    return DQResult(
        rule.rule_id, rule.rule_type, rule.column, rule.severity,
        passed=failed == 0,
        failed_records=failed,
        total_records=total,
        message=f"{failed} records outside range [{lo}, {hi}] on '{rule.column}'.",
    )


def check_domain(df, rule: DQRule, total: int) -> DQResult:
    """Domain / allowed-values check."""
    if not rule.column or not rule.allowed_values:
        return DQResult(rule.rule_id, rule.rule_type, rule.column, rule.severity,
                        passed=False, total_records=total,
                        message="Missing 'column' or 'allowed_values' for domain rule.")

    from pyspark.sql.functions import col

    viol = df.filter(col(rule.column).isNotNull() & ~col(rule.column).isin(rule.allowed_values))
    failed = viol.count()
    return DQResult(
        rule.rule_id, rule.rule_type, rule.column, rule.severity,
        passed=failed == 0,
        failed_records=failed,
        total_records=total,
        message=f"{failed} records have values outside allowed domain {rule.allowed_values} on '{rule.column}'.",
    )


def check_calculation(df, rule: DQRule, total: int, spark) -> DQResult:
    """SQL expression check — the expression must evaluate to True for every row.

    The expression is evaluated via a Spark SQL select.  Columns from the
    DataFrame are available by name.  The rule 'expression' should be a
    valid Spark SQL boolean expression, e.g. ``amount == quantity * unit_price``.
    """
    if not rule.expression:
        return DQResult(rule.rule_id, rule.rule_type, rule.column, rule.severity,
                        passed=False, total_records=total,
                        message="Missing 'expression' for calculation rule.")

    # Register the DataFrame as a temp view so we can use Spark SQL.
    temp_view = f"_dq_calc_{rule.rule_id}"
    df.createOrReplaceTempView(temp_view)

    query = f"SELECT * FROM {temp_view} WHERE NOT ({rule.expression})"
    try:
        viol = spark.sql(query)
        failed = viol.count()
    except Exception as exc:
        return DQResult(
            rule.rule_id, rule.rule_type, rule.column, rule.severity,
            passed=False, total_records=total,
            message=f"SQL expression error: {exc}",
        )

    return DQResult(
        rule.rule_id, rule.rule_type, rule.column, rule.severity,
        passed=failed == 0,
        failed_records=failed,
        total_records=total,
        message=f"{failed} records failed calculation '{rule.expression}'.",
    )


def check_completeness(df, rule: DQRule, total: int) -> DQResult:
    """Null / completeness check with optional threshold."""
    if not rule.column:
        return DQResult(rule.rule_id, rule.rule_type, rule.column, rule.severity,
                        passed=False, total_records=total,
                        message="Missing 'column' for completeness rule.")

    from pyspark.sql.functions import col, count, when

    null_count = df.select(count(when(col(rule.column).isNull(), True))).collect()[0][0]
    null_frac = (null_count / total) if total else 0.0

    passed = null_frac <= rule.null_threshold
    return DQResult(
        rule.rule_id, rule.rule_type, rule.column, rule.severity,
        passed=passed,
        failed_records=null_count,
        total_records=total,
        message=(
            f"{null_count} nulls ({null_frac:.2%}) in '{rule.column}'; "
            f"threshold {rule.null_threshold:.2%}."
        ),
    )


def check_uniqueness(df, rule: DQRule, total: int) -> DQResult:
    """Uniqueness / duplicate-detection check."""
    cols = rule.unique_columns or ([rule.column] if rule.column else None)
    if not cols:
        return DQResult(rule.rule_id, rule.rule_type, rule.column, rule.severity,
                        passed=False, total_records=total,
                        message="No columns specified for uniqueness rule.")

    from pyspark.sql.functions import count

    dupes = (
        df.groupBy(*cols)
        .agg(count("*").alias("_cnt"))
        .filter("_cnt > 1")
    )
    dupe_groups = dupes.count()
    # Total duplicated *records* (every row in a group beyond the first)
    if dupe_groups > 0:
        dupe_rows = dupes.agg({"_cnt": "sum"}).collect()[0][0]
        failed = dupe_rows - dupe_groups  # subtract one per group (the "original")
    else:
        failed = 0

    return DQResult(
        rule.rule_id, rule.rule_type, rule.column, rule.severity,
        passed=failed == 0,
        failed_records=failed,
        total_records=total,
        message=f"{failed} duplicate records across {dupe_groups} groups on {cols}.",
    )


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

CHECK_MAP = {
    "format": check_format,
    "range": check_range,
    "domain": check_domain,
    "calculation": check_calculation,
    "completeness": check_completeness,
    "uniqueness": check_uniqueness,
}


def run_rules(df, rules: List[DQRule], spark) -> DQReport:
    """Run all DQ rules against the DataFrame and return a report."""
    report = DQReport(
        table_name=df._jdf.queryExecution().analyzed().output().apply(0).qualifier().toString()
        if hasattr(df, "_jdf") else "",
        run_timestamp=datetime.now(timezone.utc).isoformat(),
    )

    total = _total_count(df)
    logger.info(f"Total records in source: {total}")

    for rule in rules:
        handler = CHECK_MAP.get(rule.rule_type)
        if not handler:
            result = DQResult(
                rule.rule_id, rule.rule_type, rule.column, rule.severity,
                passed=False, total_records=total,
                message=f"Unknown rule_type '{rule.rule_type}'.",
            )
        elif rule.rule_type == "calculation":
            result = handler(df, rule, total, spark)
        else:
            result = handler(df, rule, total)

        report.results.append(result)
        status = "PASS" if result.passed else "FAIL"
        logger.info(
            f"[{status}] {result.rule_id} ({result.rule_type}, {result.severity}) "
            f"— {result.message}"
        )

    return report


# ---------------------------------------------------------------------------
# Reporter & quarantine
# ---------------------------------------------------------------------------

def print_report(report: DQReport) -> None:
    """Print a human-readable summary."""
    print("\n" + "=" * 70)
    print("DATA QUALITY REPORT")
    print("=" * 70)
    print(f"  Run timestamp : {report.run_timestamp}")
    print(f"  Table         : {report.table_name or '(unknown)'}")
    print(f"  Total rules   : {report.total_rules}")
    print(f"  Passed        : {report.passed_rules}")
    print(f"  Failed errors : {report.failed_errors}")
    print(f"  Failed warns  : {report.failed_warnings}")
    print(f"  DQ Score      : {report.score}%")
    print("-" * 70)

    for r in report.results:
        status = "✅ PASS" if r.passed else "❌ FAIL"
        print(f"  {status}  {r.rule_id:<20} {r.rule_type:<16} "
              f"[{r.severity:<7}]  {r.failed_records}/{r.total_records} failed")
        if not r.passed:
            print(f"           → {r.message}")

    print("=" * 70)
    print(f"  Exit code: {report.exit_code}  "
          f"(0=all pass, 1=warnings, 2=errors)")
    print("=" * 70 + "\n")


def write_quarantine(df, report: DQReport, rules: List[DQRule],
                     spark, quarantine_path: Optional[str] = None) -> None:
    """Write failing records to a quarantine Delta table.

    For each failed rule we filter the DataFrame and append (union) the
    violating rows into a single quarantine set, then write to
    ``quarantine_path`` (or ``/tmp/dq_quarantine`` by default).
    """
    if not quarantine_path:
        quarantine_path = "/tmp/dq_quarantine"

    from pyspark.sql.functions import lit

    violating = None
    for rule, result in zip(rules, report.results):
        if result.passed:
            continue

        cols = rule.unique_columns or ([rule.column] if rule.column else None)

        if rule.rule_type == "completeness" and rule.column:
            subset = df.filter(f"`{rule.column}` IS NULL")
        elif rule.rule_type == "format" and rule.column and rule.pattern:
            subset = df.filter(f"`{rule.column}` IS NOT NULL AND NOT `{rule.column}` RLIKE '{rule.pattern}'")
        elif rule.rule_type == "range" and rule.column:
            conds = []
            if rule.min_value is not None:
                conds.append(f"`{rule.column}` < {rule.min_value}")
            if rule.max_value is not None:
                conds.append(f"`{rule.column}` > {rule.max_value}")
            subset = df.filter(" OR ".join(conds)) if conds else None
        elif rule.rule_type == "domain" and rule.column and rule.allowed_values:
            vals = ",".join(f"'{v}'" for v in rule.allowed_values)
            subset = df.filter(f"`{rule.column}` IS NOT NULL AND `{rule.column}` NOT IN ({vals})")
        elif rule.rule_type == "uniqueness" and cols:
            # Collect duplicate rows
            dup_ids = (
                df.groupBy(*cols)
                .count()
                .filter("count > 1")
                .drop("count")
            )
            subset = df.join(dup_ids, on=cols, how="inner")
        elif rule.rule_type == "calculation" and rule.expression:
            temp_view = f"_dq_quar_{rule.rule_id}"
            df.createOrReplaceTempView(temp_view)
            subset = spark.sql(f"SELECT * FROM {temp_view} WHERE NOT ({rule.expression})")
        else:
            subset = None

        if subset is not None:
            tagged = subset.withColumn("_dq_rule_id", lit(rule.rule_id)) \
                           .withColumn("_dq_rule_type", lit(rule.rule_type))
            violating = tagged if violating is None else violating.unionByName(tagged, allowMissingColumns=True)

    if violating is None:
        logger.info("No quarantined records to write.")
        return

    count = violating.count()
    logger.info(f"Writing {count} quarantined records to {quarantine_path}")
    (
        violating.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(quarantine_path)
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Validate a Delta table / DataFrame against YAML-defined DQ rules.",
    )
    p.add_argument("--rules", required=True, help="Path to the YAML rules file.")
    p.add_argument("--table", default=None, help="Fully-qualified table name (catalog.schema.table).")
    p.add_argument("--delta-path", default=None, help="Path to a Delta table on disk.")
    p.add_argument("--quarantine-path", default=None,
                   help="Delta path for quarantined records (default: /tmp/dq_quarantine).")
    p.add_argument("--verbose", action="store_true", help="Enable DEBUG logging.")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        rules = load_rules(args.rules)
        spark = get_spark()
        df = load_dataframe(spark, table=args.table, delta_path=args.delta_path)
        report = run_rules(df, rules, spark)
        print_report(report)
        write_quarantine(df, report, rules, spark, args.quarantine_path)
        return report.exit_code
    except Exception as exc:
        logger.error(f"DQ validation failed: {exc}", exc_info=True)
        return 2
    finally:
        try:
            from pyspark.sql import SparkSession
            SparkSession.getActiveSession().stop()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
