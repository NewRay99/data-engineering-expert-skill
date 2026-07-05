# Time Series Data Handling Reference

## Overview

Time series data — sequential observations indexed by time — is ubiquitous in analytics: IoT sensors, financial markets, application logs, sales metrics, and more. Handling time series correctly requires careful attention to timestamp parsing, time zones, resampling, windowing, anomaly detection, and storage patterns. This reference provides production-ready patterns and code examples in Python and SQL for building robust time series pipelines.

---

## 1. Timestamp Handling Fundamentals

### 1.1 UTC-First Principle

Always store timestamps in UTC. Convert to local time zones only at presentation layer. This eliminates ambiguity around daylight saving time (DST) transitions and simplifies cross-region analytics.

```python
from datetime import datetime, timezone
import pandas as pd

# Parse a naive timestamp and localize to UTC
naive_ts = datetime(2024, 3, 10, 2, 30, 0)  # Ambiguous during DST transition
utc_ts = naive_ts.replace(tzinfo=timezone.utc)
print(utc_ts)  # 2024-03-10 02:30:00+00:00

# Convert local time to UTC
local_ts = pd.Timestamp("2024-03-10 02:30:00", tz="America/New_York")
utc_ts = local_ts.tz_convert("UTC")
print(utc_ts)  # 2024-03-10 07:30:00+00:00 (EST -> UTC offset is -5)

# Handle ambiguous DST times
ambiguous = pd.Timestamp("2024-11-03 01:30:00", tz="America/New_York", ambiguous="infer")
```

### 1.2 Pandas Timestamp Best Practices

```python
import pandas as pd

# Parse timestamps with explicit format (faster and unambiguous)
df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S", utc=True)

# Handle mixed time zones in a column
df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

# Set timezone-aware index for time series operations
df = df.set_index("timestamp")
df.index = df.index.tz_convert("UTC")

# Common timestamp extraction
df["year"] = df.index.year
df["month"] = df.index.month
df["day"] = df.index.day
df["hour"] = df.index.hour
df["day_of_week"] = df.index.dayofweek  # Monday=0, Sunday=6
df["is_weekend"] = df.index.dayofweek >= 5
df["quarter"] = df.index.quarter
```

### 1.3 SQL — Timestamp Handling

```sql
-- Store timestamps as TIMESTAMPTZ (PostgreSQL)
CREATE TABLE sensor_readings (
    id BIGSERIAL PRIMARY KEY,
    sensor_id TEXT NOT NULL,
    reading_ts TIMESTAMPTZ NOT NULL,  -- Always store in UTC
    temperature NUMERIC(10,2),
    humidity NUMERIC(10,2)
);

-- Convert local time to UTC on insert
INSERT INTO sensor_readings (sensor_id, reading_ts, temperature)
VALUES (
    'SENSOR_001',
    '2024-03-10 02:30:00 America/New_York'::TIMESTAMPTZ AT TIME ZONE 'UTC',
    22.5
);

-- Query in a specific time zone
SELECT
    sensor_id,
    reading_ts AT TIME ZONE 'America/New_York' AS local_reading_ts,
    temperature
FROM sensor_readings
WHERE reading_ts >= '2024-03-01'::TIMESTAMPTZ
  AND reading_ts < '2024-04-01'::TIMESTAMPTZ;

-- Generate a continuous time series (for gap filling)
WITH time_grid AS (
    SELECT generate_series(
        '2024-03-01'::TIMESTAMPTZ,
        '2024-03-31'::TIMESTAMPTZ,
        INTERVAL '1 hour'
    ) AS ts
)
SELECT tg.ts, sr.sensor_id, sr.temperature
FROM time_grid tg
LEFT JOIN sensor_readings sr ON sr.reading_ts = tg.ts
ORDER BY tg.ts;
```

---

## 2. Resampling and Aggregation

### 2.1 Downsampling (High Frequency → Low Frequency)

Downsampling reduces data volume by aggregating higher-frequency data into larger time buckets.

```python
import pandas as pd

# Assume df has a DatetimeIndex
# Downsample from 1-minute to 1-hour, with multiple aggregations
hourly = df.resample("1h").agg({
    "temperature": ["mean", "min", "max", "std"],
    "humidity": "mean",
    "pressure": "last"
})

# Flatten multi-level columns
hourly.columns = ["_".join(col).strip() for col in hourly.columns.values]

# Downsample with custom labels
daily = df.resample("D", label="left", closed="left").agg({
    "sales": "sum",
    "visitors": "nunique"
})

# Business day resampling (skips weekends)
bday = df.resample("B").agg({"close": "last", "volume": "sum"})

# Weekly resampling ending on Friday
weekly = df.resample("W-FRI").agg({"close": "last", "volume": "sum"})
```

### 2.2 Upsampling (Low Frequency → High Frequency)

Upsampling increases frequency and requires interpolation or forward-filling.

```python
# Upsample from daily to hourly
hourly = daily.resample("1h").asfreq()

# Forward fill (carry last known value)
hourly_ffill = daily.resample("1h").ffill()

# Linear interpolation
hourly_interp = daily.resample("1h").interpolate(method="linear")

# Time-weighted interpolation
hourly_time = daily.resample("1h").interpolate(method="time")

# Spline interpolation (smoother, but may overshoot)
hourly_spline = daily.resample("1h").interpolate(method="spline", order=2)
```

### 2.3 SQL — Time Bucket Aggregation (TimescaleDB)

```sql
-- TimescaleDB time_bucket for efficient downsampling
SELECT
    time_bucket('1 hour', reading_ts) AS hour_bucket,
    sensor_id,
    AVG(temperature) AS avg_temp,
    MIN(temperature) AS min_temp,
    MAX(temperature) AS max_temp,
    STDDEV(temperature) AS std_temp,
    COUNT(*) AS reading_count
FROM sensor_readings
WHERE reading_ts >= NOW() - INTERVAL '7 days'
GROUP BY hour_bucket, sensor_id
ORDER BY hour_bucket DESC, sensor_id;

-- Gap detection: find missing hourly buckets
WITH expected AS (
    SELECT generate_series(
        date_trunc('hour', NOW() - INTERVAL '24 hours'),
        date_trunc('hour', NOW()),
        INTERVAL '1 hour'
    ) AS hour_ts
)
SELECT e.hour_ts
FROM expected e
LEFT JOIN (
    SELECT DISTINCT time_bucket('1 hour', reading_ts) AS hour_ts
    FROM sensor_readings
    WHERE reading_ts >= NOW() - INTERVAL '24 hours'
) actual ON e.hour_ts = actual.hour_ts
WHERE actual.hour_ts IS NULL;
```

---

## 3. Window Functions and Rolling Operations

### 3.1 Rolling Windows (Pandas)

```python
import pandas as pd

# Simple rolling mean (moving average)
df["ma_7d"] = df["value"].rolling(window="7D").mean()

# Rolling with min periods (handles early rows)
df["ma_7d"] = df["value"].rolling(window="7D", min_periods=3).mean()

# Exponential weighted moving average
df["ewma_12"] = df["value"].ewm(span=12, adjust=False).mean()

# Rolling custom aggregation
df["rolling_median"] = df["value"].rolling(window=24, center=True).median()
df["rolling_quantile_95"] = df["value"].rolling(window=24).quantile(0.95)

# Rolling with multiple stats
rolling_stats = df["value"].rolling(window="7D").agg(["mean", "std", "min", "max"])
df = df.join(rolling_stats.add_prefix("rolling_7d_"))
```

### 3.2 Expanding Windows

```python
# Cumulative statistics
df["cumulative_mean"] = df["value"].expanding().mean()
df["cumulative_max"] = df["value"].expanding().max()

# Expanding with minimum periods
df["expanding_std"] = df["value"].expanding(min_periods=10).std()
```

### 3.3 SQL — Window Functions for Time Series

```sql
-- Moving average using window functions
SELECT
    reading_ts,
    sensor_id,
    temperature,
    AVG(temperature) OVER (
        PARTITION BY sensor_id
        ORDER BY reading_ts
        RANGE BETWEEN INTERVAL '6 hours' PRECEDING AND CURRENT ROW
    ) AS ma_6h,
    AVG(temperature) OVER (
        PARTITION BY sensor_id
        ORDER BY reading_ts
        ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    ) AS ma_24_rows
FROM sensor_readings
WHERE sensor_id = 'SENSOR_001'
ORDER BY reading_ts;

-- Lag/Lead for period-over-period comparison
SELECT
    reading_ts,
    temperature,
    LAG(temperature, 1) OVER (PARTITION BY sensor_id ORDER BY reading_ts) AS prev_reading,
    temperature - LAG(temperature, 1) OVER (PARTITION BY sensor_id ORDER BY reading_ts) AS delta,
    ROUND(
        100.0 * (temperature - LAG(temperature, 1) OVER (PARTITION BY sensor_id ORDER BY reading_ts))
        / NULLIF(LAG(temperature, 1) OVER (PARTITION BY sensor_id ORDER BY reading_ts), 0),
        2
    ) AS pct_change
FROM sensor_readings
WHERE sensor_id = 'SENSOR_001';

-- First and last value in a window
SELECT
    reading_ts,
    temperature,
    FIRST_VALUE(temperature) OVER w AS window_open,
    LAST_VALUE(temperature) OVER w AS window_close
FROM sensor_readings
WINDOW w AS (
    PARTITION BY sensor_id, date_trunc('hour', reading_ts)
    ORDER BY reading_ts
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
);
```

---

## 4. Gap Detection and Imputation

### 4.1 Detecting Gaps

```python
import pandas as pd

def detect_gaps(df: pd.DataFrame, expected_freq: str = "1min") -> pd.DataFrame:
    """
    Detect gaps in a time series by comparing to expected frequency.
    Returns a DataFrame of gap intervals.
    """
    expected_index = pd.date_range(
        start=df.index.min(),
        end=df.index.max(),
        freq=expected_freq
    )
    missing = expected_index.difference(df.index)

    if len(missing) == 0:
        return pd.DataFrame(columns=["gap_start", "gap_end", "gap_duration"])

    # Group consecutive missing timestamps into gap intervals
    gaps = []
    gap_start = missing[0]
    prev = missing[0]

    for ts in missing[1:]:
        if (ts - prev).total_seconds() > pd.Timedelta(expected_freq).total_seconds():
            gaps.append({
                "gap_start": gap_start,
                "gap_end": prev + pd.Timedelta(expected_freq),
                "gap_duration": prev + pd.Timedelta(expected_freq) - gap_start
            })
            gap_start = ts
        prev = ts

    gaps.append({
        "gap_start": gap_start,
        "gap_end": prev + pd.Timedelta(expected_freq),
        "gap_duration": prev + pd.Timedelta(expected_freq) - gap_start
    })

    return pd.DataFrame(gaps)

# Usage
gaps = detect_gaps(df, expected_freq="5min")
print(f"Found {len(gaps)} gaps totaling {gaps['gap_duration'].sum()}")
```

### 4.2 Imputation Strategies

```python
import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer

def impute_time_series(df: pd.DataFrame, value_col: str, method: str = "linear") -> pd.Series:
    """
    Impute missing values in a time series using various strategies.
    """
    s = df[value_col].copy()

    if method == "ffill":
        return s.ffill()
    elif method == "bfill":
        return s.bfill()
    elif method == "linear":
        return s.interpolate(method="linear")
    elif method == "time":
        return s.interpolate(method="time")
    elif method == "seasonal":
        # Deseasonalize, interpolate, reseasonalize
        seasonal = s.groupby(s.index.hour).transform("mean")
        deseasonalized = s - seasonal
        imputed = deseasonalized.interpolate(method="linear")
        return imputed + seasonal
    elif method == "knn":
        # KNN imputation using time features
        features = pd.DataFrame({
            "hour": df.index.hour,
            "dayofweek": df.index.dayofweek,
            "value": s
        }, index=df.index)
        imputer = KNNImputer(n_neighbors=5)
        imputed = imputer.fit_transform(features)
        return pd.Series(imputed[:, -1], index=df.index, name=value_col)
    else:
        raise ValueError(f"Unknown method: {method}")

# Usage
df = df.asfreq("5min")  # Ensure regular frequency
df["temperature_imputed"] = impute_time_series(df, "temperature", method="seasonal")
```

### 4.3 SQL — Gap Filling with Lateral Join

```sql
-- Fill gaps with linear interpolation in SQL
WITH time_grid AS (
    SELECT generate_series(
        '2024-03-01'::TIMESTAMPTZ,
        '2024-03-31'::TIMESTAMPTZ,
        INTERVAL '1 hour'
    ) AS ts
),
bounded AS (
    SELECT
        tg.ts,
        sr.temperature,
        -- Find surrounding known values
        LAST_VALUE(temperature) OVER (ORDER BY tg.ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known,
        FIRST_VALUE(temperature) OVER (ORDER BY tg.ts ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS next_known,
        LAST_VALUE(reading_ts) OVER (ORDER BY tg.ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ts,
        FIRST_VALUE(reading_ts) OVER (ORDER BY tg.ts ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS next_known_ts
    FROM time_grid tg
    LEFT JOIN sensor_readings sr ON sr.reading_ts = tg.ts
    WHERE sr.sensor_id = 'SENSOR_001' OR sr.sensor_id IS NULL
)
SELECT
    ts,
    COALESCE(temperature, last_known + (next_known - last_known) *
        EXTRACT(EPOCH FROM (ts - last_known_ts)) /
        NULLIF(EXTRACT(EPOCH FROM (next_known_ts - last_known_ts)), 0)
    ) AS interpolated_temperature
FROM bounded
ORDER BY ts;
```

---

## 5. Seasonality and Trend Decomposition

### 5.1 Classical Decomposition

```python
import pandas as pd
from statsmodels.tsa.seasonal import seasonal_decompose, STL

# Classical decomposition (additive)
result = seasonal_decompose(df["value"], model="additive", period=24)  # 24 hours
trend = result.trend
seasonal = result.seasonal
residual = result.resid

# Classical decomposition (multiplicative)
result = seasonal_decompose(df["value"], model="multiplicative", period=24)

# STL decomposition (more robust to outliers)
stl = STL(df["value"], period=24, robust=True)
result = stl.fit()
trend = result.trend
seasonal = result.seasonal
residual = result.resid

# Reconstruct
reconstructed = trend + seasonal + residual
assert np.allclose(reconstructed, df["value"], equal_nan=True)
```

### 5.2 Differencing for Stationarity

```python
from statsmodels.tsa.stattools import adfuller

def check_stationarity(series: pd.Series, significance: float = 0.05) -> dict:
    """Augmented Dickey-Fuller test for stationarity."""
    result = adfuller(series.dropna())
    return {
        "adf_statistic": result[0],
        "p_value": result[1],
        "critical_values": result[4],
        "is_stationary": result[1] < significance
    }

# First-order differencing
df["diff_1"] = df["value"].diff()

# Seasonal differencing
df["diff_seasonal"] = df["value"].diff(periods=24)

# Combined differencing
df["diff_combined"] = df["value"].diff().diff(periods=24)

print(check_stationarity(df["value"]))
print(check_stationarity(df["diff_1"].dropna()))
```

---

## 6. Anomaly Detection

### 6.1 Statistical Methods

```python
import pandas as pd
import numpy as np

def detect_anomalies_zscore(series: pd.Series, window: int = 24, threshold: float = 3.0) -> pd.Series:
    """Detect anomalies using rolling z-score."""
    rolling_mean = series.rolling(window=window, center=True).mean()
    rolling_std = series.rolling(window=window, center=True).std()
    z_scores = (series - rolling_mean) / rolling_std
    return z_scores.abs() > threshold

def detect_anomalies_iqr(series: pd.Series, window: int = 24, multiplier: float = 1.5) -> pd.Series:
    """Detect anomalies using rolling IQR."""
    q1 = series.rolling(window=window, center=True).quantile(0.25)
    q3 = series.rolling(window=window, center=True).quantile(0.75)
    iqr = q3 - q1
    lower = q1 - multiplier * iqr
    upper = q3 + multiplier * iqr
    return (series < lower) | (series > upper)

def detect_anomalies_ewma(series: pd.Series, span: int = 20, threshold: float = 3.0) -> pd.Series:
    """Detect anomalies using EWMA control chart."""
    ewma = series.ewm(span=span, adjust=False).mean()
    ewma_std = series.ewm(span=span, adjust=False).std()
    z = (series - ewma) / ewma_std
    return z.abs() > threshold
```

### 6.2 Isolation Forest

```python
from sklearn.ensemble import IsolationForest
import pandas as pd

def detect_anomalies_isolation_forest(df: pd.DataFrame, value_col: str,
                                       contamination: float = 0.05) -> pd.DataFrame:
    """Detect anomalies using Isolation Forest with time features."""
    features = pd.DataFrame({
        "value": df[value_col],
        "hour": df.index.hour,
        "dayofweek": df.index.dayofweek,
        "lag_1": df[value_col].shift(1),
        "rolling_mean_6": df[value_col].rolling(6).mean(),
        "rolling_std_6": df[value_col].rolling(6).std(),
    }).dropna()

    model = IsolationForest(
        contamination=contamination,
        n_estimators=100,
        random_state=42,
        n_jobs=-1
    )
    predictions = model.fit_predict(features)
    scores = model.decision_function(features)

    result = features.copy()
    result["is_anomaly"] = predictions == -1
    result["anomaly_score"] = scores
    return result

# Usage
anomalies = detect_anomalies_isolation_forest(df, "temperature", contamination=0.02)
print(f"Detected {anomalies['is_anomaly'].sum()} anomalies")
```

### 6.3 SQL — Statistical Anomaly Detection

```sql
WITH stats AS (
    SELECT
        reading_ts,
        sensor_id,
        temperature,
        AVG(temperature) OVER (
            PARTITION BY sensor_id
            ORDER BY reading_ts
            RANGE BETWEEN INTERVAL '23 hours' PRECEDING AND CURRENT ROW
        ) AS rolling_mean,
        STDDEV(temperature) OVER (
            PARTITION BY sensor_id
            ORDER BY reading_ts
            RANGE BETWEEN INTERVAL '23 hours' PRECEDING AND CURRENT ROW
        ) AS rolling_std
    FROM sensor_readings
)
SELECT
    reading_ts,
    sensor_id,
    temperature,
    rolling_mean,
    rolling_std,
    (temperature - rolling_mean) / NULLIF(rolling_std, 0) AS z_score,
    CASE
        WHEN ABS((temperature - rolling_mean) / NULLIF(rolling_std, 0)) > 3
        THEN 'ANOMALY'
        ELSE 'NORMAL'
    END AS status
FROM stats
WHERE ABS((temperature - rolling_mean) / NULLIF(rolling_std, 0)) > 3
ORDER BY ABS(z_score) DESC;
```

---

## 7. Time Series Forecasting Patterns

### 7.1 Simple Baseline: Seasonal Naive

```python
import pandas as pd
import numpy as np

def seasonal_naive_forecast(train: pd.Series, horizon: int, seasonal_period: int) -> pd.Series:
    """Forecast using last season's values."""
    last_season = train.iloc[-seasonal_period:]
    forecasts = []
    for i in range(horizon):
        forecasts.append(last_season.iloc[i % seasonal_period])
    forecast_index = pd.date_range(
        start=train.index[-1] + pd.Timedelta(hours=1),
        periods=horizon,
        freq=train.index.freq or "1h"
    )
    return pd.Series(forecasts, index=forecast_index, name="forecast")
```

### 7.2 Prophet Forecast

```python
from prophet import Prophet
import pandas as pd

def prophet_forecast(df: pd.DataFrame, value_col: str, horizon: int = 168) -> pd.DataFrame:
    """
    Forecast using Facebook Prophet.
    df must have a DatetimeIndex.
    """
    prophet_df = pd.DataFrame({
        "ds": df.index.tz_localize(None),  # Prophet doesn't support timezones
        "y": df[value_col]
    }).dropna()

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=True,
        changepoint_prior_scale=0.05,
        interval_width=0.95
    )
    model.fit(prophet_df)

    future = model.make_future_dataframe(periods=horizon, freq="1h")
    forecast = model.predict(future)

    return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(horizon)
```

### 7.3 Evaluation Metrics

```python
import numpy as np
import pandas as pd
from typing import Dict

def evaluate_forecast(actual: pd.Series, predicted: pd.Series) -> Dict[str, float]:
    """Calculate common time series forecast evaluation metrics."""
    actual, predicted = actual.align(predicted, join="inner")
    actual = actual.dropna()
    predicted = predicted.reindex(actual.index)

    residuals = actual - predicted

    mae = np.mean(np.abs(residuals))
    mse = np.mean(residuals ** 2)
    rmse = np.sqrt(mse)
    mape = np.mean(np.abs(residuals / actual.replace(0, np.nan))) * 100
    smape = np.mean(2 * np.abs(residuals) / (np.abs(actual) + np.abs(predicted))) * 100

    # Mean Absolute Scaled Error (MASE) — compares to naive forecast
    naive_errors = np.abs(actual.diff().dropna())
    mase = mae / np.mean(naive_errors) if len(naive_errors) > 0 else np.nan

    return {
        "MAE": round(mae, 4),
        "MSE": round(mse, 4),
        "RMSE": round(rmse, 4),
        "MAPE": round(mape, 2),
        "sMAPE": round(smape, 2),
        "MASE": round(mase, 4)
    }

# Usage
metrics = evaluate_forecast(actual_series, forecast_series)
for k, v in metrics.items():
    print(f"{k}: {v}")
```

---

## 8. Storage Patterns for Time Series

### 8.1 Wide vs. Long Format

```python
# Long format (preferred for storage and querying by entity)
# Columns: timestamp, sensor_id, metric_name, value
long_df = pd.DataFrame({
    "timestamp": pd.date_range("2024-01-01", periods=5, freq="1h").repeat(3),
    "sensor_id": ["S1", "S2", "S3"] * 5,
    "metric_name": ["temperature"] * 15,
    "value": [22.1, 23.4, 21.8, 22.3, 23.5, 22.0, 22.5, 23.7, 22.2, 22.4, 23.6, 22.1, 22.6, 23.9, 22.3]
})

# Pivot to wide format (preferred for analysis)
wide_df = long_df.pivot_table(
    index="timestamp",
    columns="sensor_id",
    values="value",
    aggfunc="first"
)

# Melt back to long
long_again = wide_df.reset_index().melt(
    id_vars="timestamp",
    var_name="sensor_id",
    value_name="value"
)
```

### 8.2 Partitioning Strategy

```sql
-- PostgreSQL native partitioning by time range
CREATE TABLE sensor_readings (
    id BIGSERIAL,
    reading_ts TIMESTAMPTZ NOT NULL,
    sensor_id TEXT NOT NULL,
    temperature NUMERIC(10,2),
    humidity NUMERIC(10,2),
    PRIMARY KEY (id, reading_ts)
) PARTITION BY RANGE (reading_ts);

-- Monthly partitions
CREATE TABLE sensor_readings_2024_01 PARTITION OF sensor_readings
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE sensor_readings_2024_02 PARTITION OF sensor_readings
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

-- Automated partition creation (using pg_partman or custom function)
CREATE OR REPLACE FUNCTION create_monthly_partition(
    p_table TEXT, p_start DATE, p_end DATE
) RETURNS void AS $$
DECLARE
    partition_name TEXT;
BEGIN
    partition_name := format('%s_%s', p_table, to_char(p_start, 'YYYY_MM'));
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
        partition_name, p_table, p_start, p_end
    );
END;
$$ LANGUAGE plpgsql;
```

### 8.3 TimescaleDB Hypertable

```sql
-- Convert regular table to TimescaleDB hypertable
CREATE TABLE sensor_readings (
    reading_ts TIMESTAMPTZ NOT NULL,
    sensor_id TEXT NOT NULL,
    temperature NUMERIC(10,2),
    humidity NUMERIC(10,2)
);

SELECT create_hypertable('sensor_readings', 'reading_ts',
    chunk_time_interval => INTERVAL '1 day');

-- Create indexes for common query patterns
CREATE INDEX idx_sensor_readings_sensor_ts
    ON sensor_readings (sensor_id, reading_ts DESC);

-- Enable compression (old chunks)
ALTER TABLE sensor_readings SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'sensor_id',
    timescaledb.compress_orderby = 'reading_ts DESC'
);

SELECT add_compression_policy('sensor_readings', INTERVAL '7 days');

-- Continuous aggregates for pre-computed downsampling
CREATE MATERIALIZED VIEW sensor_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', reading_ts) AS hour_ts,
    sensor_id,
    AVG(temperature) AS avg_temp,
    MIN(temperature) AS min_temp,
    MAX(temperature) AS max_temp,
    COUNT(*) AS reading_count
FROM sensor_readings
GROUP BY hour_ts, sensor_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('sensor_hourly',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
```

---

## 9. Time Series Joins

### 9.1 As-of Join (Last Known Value)

As-of joins are essential when aligning time series with different frequencies — e.g., joining trade ticks to a 1-minute OHLC bar.

```python
import pandas as pd

# Tick data (irregular)
ticks = pd.DataFrame({
    "timestamp": pd.to_datetime(["2024-01-01 10:00:01", "2024-01-01 10:00:05",
                                  "2024-01-01 10:02:30", "2024-01-01 10:05:00"]),
    "price": [100.0, 100.5, 101.2, 102.0]
}).set_index("timestamp")

# Reference signal (regular, 1-minute)
signals = pd.DataFrame({
    "timestamp": pd.date_range("2024-01-01 10:00", periods=7, freq="1min"),
    "signal": [0.1, 0.2, 0.15, 0.3, 0.25, 0.4, 0.35]
}).set_index("timestamp")

# Merge as-of: for each signal, get the last known tick price
merged = pd.merge_asof(
    signals.reset_index(),
    ticks.reset_index(),
    on="timestamp",
    direction="backward"
).set_index("timestamp")

print(merged)
```

### 9.2 SQL — As-of Join

```sql
-- PostgreSQL LATERAL join for as-of lookups
SELECT
    s.signal_ts,
    s.signal_value,
    t.price AS last_known_price,
    t.tick_ts AS price_timestamp
FROM signals s
LEFT JOIN LATERAL (
    SELECT *
    FROM ticks
    WHERE tick_ts <= s.signal_ts
    ORDER BY tick_ts DESC
    LIMIT 1
) t ON true
ORDER BY s.signal_ts;

-- TimescaleDB ASOF JOIN (if available)
SELECT *
FROM signals s
ASOF JOIN ticks t
    ON (s.sensor_id = t.sensor_id AND s.signal_ts >= t.tick_ts);
```

---

## 10. Working with Time Zones in Pipelines

### 10.1 Time Zone-Aware Pipeline Pattern

```python
import pandas as pd
from typing import Optional

class TimeSeriesPipeline:
    """Time zone-aware time series ingestion pipeline."""

    def __init__(self, source_tz: str = "UTC", target_tz: str = "UTC"):
        self.source_tz = source_tz
        self.target_tz = target_tz

    def parse_timestamps(self, df: pd.DataFrame, ts_col: str,
                         format: Optional[str] = None) -> pd.DataFrame:
        """Parse timestamps and convert to target timezone."""
        if format:
            df[ts_col] = pd.to_datetime(df[ts_col], format=format)
        else:
            df[ts_col] = pd.to_datetime(df[ts_col])

        # Localize if naive
        if df[ts_col].dt.tz is None:
            df[ts_col] = df[ts_col].dt.tz_localize(self.source_tz)

        # Convert to target timezone
        df[ts_col] = df[ts_col].dt.tz_convert(self.target_tz)
        return df

    def resample_and_aggregate(self, df: pd.DataFrame, ts_col: str,
                                value_cols: list[str], freq: str = "1h") -> pd.DataFrame:
        """Resample to target frequency with aggregation."""
        df = df.set_index(ts_col)
        agg_dict = {col: ["mean", "min", "max", "count"] for col in value_cols}
        result = df.resample(freq).agg(agg_dict)
        result.columns = [f"{col}_{stat}" for col, stat in result.columns]
        return result.reset_index()

    def add_time_features(self, df: pd.DataFrame, ts_col: str,
                          timezone: str = "UTC") -> pd.DataFrame:
        """Add time-based features for ML/analysis."""
        local_ts = df[ts_col].dt.tz_convert(timezone)
        df["hour"] = local_ts.dt.hour
        df["dayofweek"] = local_ts.dt.dayofweek
        df["month"] = local_ts.dt.month
        df["quarter"] = local_ts.dt.quarter
        df["is_weekend"] = local_ts.dt.dayofweek >= 5
        df["is_business_hours"] = (local_ts.dt.hour >= 9) & (local_ts.dt.hour < 17)
        df["dayofyear"] = local_ts.dt.dayofyear
        df["weekofyear"] = local_ts.dt.isocalendar().week
        return df

# Usage
pipeline = TimeSeriesPipeline(source_tz="America/New_York", target_tz="UTC")
df = pipeline.parse_timestamps(raw_df, "timestamp", format="%Y-%m-%d %H:%M:%S")
df = pipeline.resample_and_aggregate(df, "timestamp", ["temperature", "humidity"], freq="15min")
df = pipeline.add_time_features(df, "timestamp", timezone="America/New_York")
```

---

## 11. Best Practices Summary

1. **Always use UTC internally**: Convert to local time zones only at the presentation layer.
2. **Use timezone-aware timestamps**: `datetime` with `tzinfo` or pandas `Timestamp` with `tz`.
3. **Set explicit frequency**: Use `asfreq()` to establish regular frequency before resampling.
4. **Handle gaps explicitly**: Detect, log, and impute missing data — don't silently ignore.
5. **Choose imputation wisely**: Forward-fill for short gaps; seasonal interpolation for longer ones; KNN for multivariate.
6. **Decompose before modeling**: Understand trend, seasonality, and residual components before forecasting.
7. **Use partitioning**: Time-based partitioning is essential for query performance on large time series tables.
8. **Pre-aggregate with continuous aggregates**: Materialize common downsampling queries (e.g., hourly from raw).
9. **Store in long format, analyze in wide**: Long format is flexible for storage; wide format is efficient for analysis.
10. **Test for stationarity**: Many forecasting models assume stationarity — check with ADF or KPSS tests.
11. **Use as-of joins for alignment**: When combining series with different frequencies, as-of joins preserve temporal correctness.
12. **Version your models**: Track model parameters, training windows, and evaluation metrics over time.
13. **Monitor for concept drift**: Re-evaluate forecast accuracy periodically; retrain when performance degrades.
14. **Leverage columnar storage**: For high-cardinality time series (many sensors), columnar formats like Parquet or ClickHouse excel.
15. **Be explicit about inclusive/exclusive bounds**: `>= start AND < end` is the standard pattern for time range queries — avoids off-by-one errors.
