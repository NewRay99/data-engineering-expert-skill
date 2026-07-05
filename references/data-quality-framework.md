# Data Quality Framework Reference

## Overview

Data quality is the foundation of reliable analytics, machine learning, and business operations. This reference provides a comprehensive framework for assessing, measuring, and improving data quality across enterprise data pipelines. It covers the six core dimensions of data quality, detection patterns, remediation strategies, and implementation examples in SQL and Python.

---

## 1. The Six Dimensions of Data Quality

### 1.1 Completeness

Completeness measures whether all required data is present. Missing values can lead to incorrect analytics, biased models, and broken downstream processes.

**Types of completeness:**
- **Record completeness**: Are all expected records present? (e.g., did we receive all daily partitions?)
- **Column completeness**: Are all required fields populated? (e.g., is `email` always non-null?)
- **Value completeness**: Are sub-fields or nested attributes populated when expected?

**SQL — Completeness Check:**

```sql
-- Column-level null rate
SELECT
    COUNT(*) AS total_rows,
    COUNT(email) AS non_null_email,
    COUNT(*) - COUNT(email) AS null_email_count,
    ROUND(100.0 * COUNT(email) / COUNT(*), 2) AS email_completeness_pct
FROM customers;

-- Record completeness (expected vs actual partition count)
WITH expected AS (
    SELECT generate_series(
        DATE '2024-01-01',
        DATE '2024-01-31',
        INTERVAL '1 day'
    )::DATE AS dt
),
actual AS (
    SELECT DISTINCT DATE(order_timestamp) AS dt
    FROM orders
)
SELECT
    e.dt,
    CASE WHEN a.dt IS NULL THEN 'MISSING' ELSE 'PRESENT' END AS status
FROM expected e
LEFT JOIN actual a ON e.dt = a.dt
WHERE a.dt IS NULL
ORDER BY e.dt;
```

**Python — Completeness Profiler:**

```python
import pandas as pd
from dataclasses import dataclass
from typing import Dict

@dataclass
class CompletenessReport:
    column: str
    total_rows: int
    null_count: int
    completeness_pct: float
    status: str

def profile_completeness(df: pd.DataFrame, required_columns: list[str]) -> list[CompletenessReport]:
    """Profile completeness for specified columns."""
    reports = []
    total = len(df)
    for col in required_columns:
        null_count = df[col].isna().sum()
        completeness = ((total - null_count) / total * 100) if total > 0 else 0
        status = "PASS" if completeness >= 99.0 else "WARN" if completeness >= 95.0 else "FAIL"
        reports.append(CompletenessReport(
            column=col,
            total_rows=total,
            null_count=int(null_count),
            completeness_pct=round(completeness, 2),
            status=status
        ))
    return reports

# Usage
df = pd.read_csv("data/customers.csv")
reports = profile_completeness(df, ["customer_id", "email", "phone", "address_line1"])
for r in reports:
    print(f"{r.column}: {r.completeness_pct}% ({r.status})")
```

### 1.2 Accuracy

Accuracy measures how closely data reflects the real-world entity or event it represents. Accuracy checks often require reference data or external validation sources.

**Common accuracy validation patterns:**
- Cross-reference against authoritative source (e.g., postal code validation)
- Range checks (e.g., age between 0 and 150)
- Business rule validation (e.g., order_total = sum(line_items))
- Format validation (e.g., email regex, ISO country codes)

**SQL — Accuracy Checks:**

```sql
-- Range check
SELECT
    COUNT(*) FILTER (WHERE age < 0 OR age > 150) AS invalid_age_count,
    COUNT(*) FILTER (WHERE order_total < 0) AS negative_total_count,
    COUNT(*) FILTER (WHERE discount > order_total) AS discount_exceeds_total
FROM orders;

-- Cross-reference check against reference table
SELECT c.customer_id, c.country_code
FROM customers c
LEFT JOIN ref_iso_countries r ON c.country_code = r.iso_code
WHERE r.iso_code IS NULL;

-- Business rule: order total must match line items
SELECT
    o.order_id,
    o.order_total,
    SUM(li.quantity * li.unit_price) AS calculated_total,
    ABS(o.order_total - SUM(li.quantity * li.unit_price)) AS discrepancy
FROM orders o
JOIN line_items li ON o.order_id = li.order_id
GROUP BY o.order_id, o.order_total
HAVING ABS(o.order_total - SUM(li.quantity * li.unit_price)) > 0.01;
```

**Python — Accuracy Validation with Rules Engine:**

```python
import re
from decimal import Decimal
from typing import Callable

class AccuracyValidator:
    """Rule-based accuracy validation engine."""

    def __init__(self):
        self.rules: dict[str, Callable] = {}

    def add_rule(self, name: str, rule: Callable):
        self.rules[name] = rule

    def validate(self, row: dict) -> dict[str, bool]:
        return {name: fn(row) for name, fn in self.rules.items()}

validator = AccuracyValidator()

# Range check
validator.add_rule("age_range", lambda r: 0 <= r.get("age", -1) <= 150)

# Email format check
validator.add_rule("email_format",
    lambda r: bool(re.match(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$',
                    r.get("email", ""))))

# Monetary consistency
validator.add_rule("total_consistency",
    lambda r: Decimal(str(r.get("order_total", 0))) ==
              sum(Decimal(str(li["price"])) * li["qty"] for li in r.get("line_items", [])))

# Validate a row
sample_row = {
    "age": 35,
    "email": "user@example.com",
    "order_total": "150.00",
    "line_items": [{"price": "50.00", "qty": 3}]
}
results = validator.validate(sample_row)
print(results)  # {'age_range': True, 'email_format': True, 'total_consistency': True}
```

### 1.3 Consistency

Consistency ensures that data is uniform across different systems, tables, and time periods. Inconsistencies arise from integration issues, race conditions, or divergent ETL logic.

**Types of consistency:**
- **Cross-system consistency**: Same entity has matching attributes across systems
- **Cross-table consistency**: Referential integrity is maintained
- **Temporal consistency**: Data doesn't contradict itself over time
- **Format consistency**: Data types and formats are uniform

**SQL — Consistency Checks:**

```sql
-- Referential integrity check
SELECT COUNT(*) AS orphaned_records
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Cross-system consistency (same customer, different emails)
SELECT
    crm.customer_id,
    crm.email AS crm_email,
    erp.email AS erp_email
FROM crm_customers crm
JOIN erp_customers erp ON crm.customer_id = erp.customer_id
WHERE crm.email <> erp.email;

-- Temporal consistency: end date must be after start date
SELECT COUNT(*) AS invalid_date_ranges
FROM subscriptions
WHERE end_date < start_date;
```

### 1.4 Timeliness

Timeliness measures whether data is available when needed and reflects the current state of the business. This is critical for real-time dashboards, alerting, and operational processes.

**Key metrics:**
- **Data latency**: Time between event occurrence and data availability
- **Freshness**: Age of the most recent record
- **SLA adherence**: Whether data meets agreed-upon delivery times

**SQL — Timeliness Checks:**

```sql
-- Data freshness check
SELECT
    MAX(event_timestamp) AS latest_event,
    NOW() - MAX(event_timestamp) AS data_age,
    EXTRACT(EPOCH FROM (NOW() - MAX(event_timestamp)))/3600 AS age_hours,
    CASE
        WHEN NOW() - MAX(event_timestamp) < INTERVAL '1 hour' THEN 'FRESH'
        WHEN NOW() - MAX(event_timestamp) < INTERVAL '6 hours' THEN 'STALE'
        ELSE 'CRITICAL'
    END AS freshness_status
FROM events;

-- Pipeline SLA tracking
SELECT
    pipeline_name,
    scheduled_completion_time,
    actual_completion_time,
    EXTRACT(EPOCH FROM (actual_completion_time - scheduled_completion_time))/60
        AS sla_delay_minutes,
    CASE
        WHEN actual_completion_time <= scheduled_completion_time THEN 'ON_TIME'
        ELSE 'LATE'
    END AS sla_status
FROM pipeline_runs
WHERE run_date = CURRENT_DATE;
```

### 1.5 Uniqueness

Uniqueness ensures that entities are represented exactly once where deduplication is expected. Duplicate records cause double-counting, inflated metrics, and user experience issues.

**SQL — Uniqueness Checks:**

```sql
-- Primary key violation check
SELECT
    customer_id,
    COUNT(*) AS duplicate_count
FROM customers
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Fuzzy duplicate detection (same name, same address, different IDs)
SELECT
    a.customer_id AS id_a,
    b.customer_id AS id_b,
    a.customer_name,
    a.address
FROM customers a
JOIN customers b ON a.customer_name = b.customer_name
    AND a.address = b.address
    AND a.customer_id < b.customer_id;
```

**Python — Deduplication with Fuzzy Matching:**

```python
import pandas as pd
from rapidfuzz import fuzz

def find_fuzzy_duplicates(df: pd.DataFrame, name_col: str, threshold: int = 90):
    """Find fuzzy duplicate records based on name similarity."""
    duplicates = []
    names = df[name_col].dropna().tolist()
    indices = df[name_col].dropna().index.tolist()

    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            score = fuzz.ratio(names[i].lower(), names[j].lower())
            if score >= threshold:
                duplicates.append({
                    "index_a": indices[i],
                    "index_b": indices[j],
                    "name_a": names[i],
                    "name_b": names[j],
                    "similarity": score
                })
    return pd.DataFrame(duplicates)

# Usage
dupes = find_fuzzy_duplicates(df, "customer_name", threshold=88)
```

### 1.6 Validity

Validity ensures that data conforms to defined business rules, formats, and domain constraints. Invalid data may be complete but still incorrect in structure or content.

**SQL — Validity Checks:**

```sql
-- Enum/lookup validation
SELECT COUNT(*) AS invalid_status_count
FROM orders
WHERE status NOT IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled');

-- Format validation (ISO 8601 date)
SELECT COUNT(*) AS invalid_date_format
FROM events
WHERE event_date::TEXT !~ '^\d{4}-\d{2}-\d{2}$';

-- Domain constraint check
SELECT COUNT(*) AS invalid_temperature
FROM sensor_readings
WHERE temperature < -273.15 OR temperature > 1000;
```

---

## 2. Data Quality Scoring

A composite data quality score combines all dimensions into a single metric for tracking and SLAs.

### 2.1 Weighted Scoring Model

```python
from dataclasses import dataclass

@dataclass
class DQScore:
    completeness: float  # 0-100
    accuracy: float
    consistency: float
    timeliness: float
    uniqueness: float
    validity: float

    @property
    def composite_score(self) -> float:
        weights = {
            "completeness": 0.25,
            "accuracy": 0.25,
            "consistency": 0.15,
            "timeliness": 0.15,
            "uniqueness": 0.10,
            "validity": 0.10,
        }
        scores = {
            "completeness": self.completeness,
            "accuracy": self.accuracy,
            "consistency": self.consistency,
            "timeliness": self.timeliness,
            "uniqueness": self.uniqueness,
            "validity": self.validity,
        }
        return sum(weights[k] * scores[k] for k in weights)

    @property
    def grade(self) -> str:
        score = self.composite_score
        if score >= 98: return "A"
        elif score >= 95: return "B"
        elif score >= 90: return "C"
        elif score >= 80: return "D"
        else: return "F"
```

### 2.2 SQL — DQ Scorecard View

```sql
CREATE OR REPLACE VIEW dq_scorecard AS
WITH completeness_metrics AS (
    SELECT
        'customers' AS table_name,
        100.0 * COUNT(email) / COUNT(*) AS completeness
    FROM customers
),
accuracy_metrics AS (
    SELECT
        'customers' AS table_name,
        100.0 * COUNT(*) FILTER (WHERE age BETWEEN 0 AND 150) / COUNT(*) AS accuracy
    FROM customers
),
uniqueness_metrics AS (
    SELECT
        'customers' AS table_name,
        100.0 * COUNT(DISTINCT customer_id) / COUNT(*) AS uniqueness
    FROM customers
)
SELECT
    c.table_name,
    c.completeness,
    a.accuracy,
    u.uniqueness,
    ROUND(0.4 * c.completeness + 0.4 * a.accuracy + 0.2 * u.uniqueness, 2) AS composite_score
FROM completeness_metrics c
JOIN accuracy_metrics a ON c.table_name = a.table_name
JOIN uniqueness_metrics u ON c.table_name = u.table_name;
```

---

## 3. Data Quality Pipeline Architecture

### 3.1 Pipeline with Great Expectations (Python)

```python
import great_expectations as gx
from great_expectations.core.expectation_configuration import ExpectationConfiguration

def build_dq_suite(context, datasource_name="my_datasource"):
    """Build a comprehensive data quality suite."""

    suite = context.add_expectation_suite("production_dq_suite")

    expectations = [
        # Completeness
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "customer_id"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "email", "mostly": 0.95}
        ),
        # Uniqueness
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": "customer_id"}
        ),
        # Validity
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "status", "value_set": ["active", "inactive", "suspended"]}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_match_regex",
            kwargs={"column": "email", "regex": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"}
        ),
        # Accuracy / Range
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "age", "min_value": 0, "max_value": 150}
        ),
    ]

    for exp in expectations:
        suite.add_expectation(exp)

    context.save_expectation_suite(suite)
    return suite
```

### 3.2 SQL-Based DQ Framework with Error Logging

```sql
-- DQ results table
CREATE TABLE IF NOT EXISTS dq_results (
    run_id UUID DEFAULT gen_random_uuid(),
    run_timestamp TIMESTAMP DEFAULT NOW(),
    table_name TEXT NOT NULL,
    check_name TEXT NOT NULL,
    dimension TEXT NOT NULL,
    total_records BIGINT,
    failed_records BIGINT,
    pass_rate NUMERIC(5,2),
    severity TEXT NOT NULL DEFAULT 'WARNING',
    PRIMARY KEY (run_id, table_name, check_name)
);

-- Reusable DQ check procedure
CREATE OR REPLACE PROCEDURE run_dq_check(
    p_table_name TEXT,
    p_check_name TEXT,
    p_dimension TEXT,
    p_query TEXT,
    p_severity TEXT DEFAULT 'WARNING'
) LANGUAGE plpgsql AS $$
DECLARE
    v_total BIGINT;
    v_failed BIGINT;
    v_pass_rate NUMERIC(5,2);
BEGIN
    EXECUTE format('SELECT COUNT(*) FROM %I', p_table_name) INTO v_total;
    EXECUTE format('SELECT COUNT(*) FROM (%s) sub', p_query) INTO v_failed;
    v_pass_rate := CASE WHEN v_total > 0
        THEN ROUND(100.0 * (v_total - v_failed) / v_total, 2)
        ELSE 0 END;

    INSERT INTO dq_results (table_name, check_name, dimension, total_records,
                            failed_records, pass_rate, severity)
    VALUES (p_table_name, p_check_name, p_dimension, v_total, v_failed, v_pass_rate, p_severity);
END;
$$;

-- Run checks
CALL run_dq_check('customers', 'null_email_check', 'completeness',
    'SELECT * FROM customers WHERE email IS NULL', 'ERROR');

CALL run_dq_check('customers', 'duplicate_id_check', 'uniqueness',
    'SELECT customer_id FROM customers GROUP BY customer_id HAVING COUNT(*) > 1', 'ERROR');

CALL run_dq_check('orders', 'negative_total_check', 'accuracy',
    'SELECT * FROM orders WHERE order_total < 0', 'WARNING');
```

---

## 4. Remediation Strategies

### 4.1 Quarantine Pattern

Records that fail DQ checks are routed to a quarantine table for manual review or automated correction.

```python
import pandas as pd
from typing import Tuple

def split_by_quality(df: pd.DataFrame, check_fn, table_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split records into clean and quarantined sets.
    Returns (clean_df, quarantined_df)
    """
    mask = df.apply(check_fn, axis=1)
    clean = df[mask].copy()
    quarantined = df[~mask].copy()

    if not quarantined.empty:
        quarantined["quarantine_timestamp"] = pd.Timestamp.now()
        quarantined["source_table"] = table_name
        quarantined["failure_reason"] = "failed_dq_check"
        # Write to quarantine table
        quarantined.to_sql(f"{table_name}_quarantine", con=engine,
                          if_exists="append", index=False)

    return clean, quarantined

def customer_check(row):
    return (
        pd.notna(row.get("customer_id"))
        and pd.notna(row.get("email"))
        and 0 <= row.get("age", -1) <= 150
    )

clean_df, bad_df = split_by_quality(df, customer_check, "customers")
```

### 4.2 Auto-Correction Rules

```python
CORRECTION_RULES = {
    "trim_whitespace": lambda v: v.strip() if isinstance(v, str) else v,
    "lowercase_email": lambda v: v.lower() if isinstance(v, str) and "@" in v else v,
    "normalize_phone": lambda v: re.sub(r'[^\d+]', '', v) if isinstance(v, str) else v,
    "fill_missing_status": lambda v: v if pd.notna(v) else "unknown",
}

def apply_corrections(df: pd.DataFrame, column_rules: dict) -> pd.DataFrame:
    """Apply auto-correction rules to specified columns."""
    for col, rule_names in column_rules.items():
        for rule_name in rule_names:
            if col in df.columns and rule_name in CORRECTION_RULES:
                df[col] = df[col].apply(CORRECTION_RULES[rule_name])
    return df

# Usage
column_rules = {
    "email": ["trim_whitespace", "lowercase_email"],
    "phone": ["normalize_phone"],
    "status": ["fill_missing_status"],
}
df = apply_corrections(df, column_rules)
```

---

## 5. Monitoring and Alerting

### 5.1 Alerting Thresholds

| Dimension | Green | Yellow | Red |
|-----------|-------|--------|-----|
| Completeness | ≥99% | 95-99% | <95% |
| Accuracy | ≥98% | 95-98% | <95% |
| Consistency | ≥99% | 97-99% | <97% |
| Timeliness | <1hr lag | 1-6hr lag | >6hr lag |
| Uniqueness | 100% | 99-100% | <99% |
| Validity | ≥98% | 95-98% | <95% |

### 5.2 Python — Alerting Integration

```python
import smtplib
from enum import Enum

class Severity(Enum):
    GREEN = "GREEN"
    YELLOW = "YELLOW"
    RED = "RED"

def evaluate_threshold(value: float, green: float, yellow: float) -> Severity:
    if value >= green:
        return Severity.GREEN
    elif value >= yellow:
        return Severity.YELLOW
    return Severity.RED

def send_alert(dimension: str, score: float, severity: Severity, recipients: list[str]):
    if severity == Severity.GREEN:
        return  # No alert for green
    subject = f"[{severity.value}] DQ Alert: {dimension} at {score}%"
    body = f"Data quality dimension '{dimension}' has dropped to {score}%.\nSeverity: {severity.value}"
    # Integrate with Slack, PagerDuty, or email
    print(f"ALERT: {subject}")
```

---

## 6. Best Practices Summary

1. **Shift left**: Run DQ checks as early as possible in the pipeline — ideally at ingestion.
2. **Fail fast, quarantine softly**: Block bad data from propagating, but preserve it for review.
3. **Version your rules**: Treat DQ rules as code — version control, review, and test them.
4. **Track trends**: A 2% drop in completeness over a week is more informative than a single snapshot.
5. **Business context matters**: Not all nulls are equal. `middle_name` being null is fine; `customer_id` is not.
6. **Automate remediation**: For predictable, low-risk corrections (whitespace trimming, case normalization), automate. For complex decisions, escalate to human review.
7. **Document data contracts**: Define SLAs between data producers and consumers explicitly.
8. **Monitor metadata too**: Schema changes, new enum values, and type drift are quality issues too.
