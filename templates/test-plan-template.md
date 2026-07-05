# Test Plan: Data Pipeline

## 1. Test Plan ID

| Field | Value |
|-------|-------|
| **Test Plan ID** | TP-<pipeline_name>-<YYYYMMDD> |
| **Version** | 1.0 |
| **Author** | <author_name> |
| **Date Created** | YYYY-MM-DD |
| **Last Updated** | YYYY-MM-DD |
| **Approval Status** | Draft / In Review / Approved |

---

## 2. Pipeline Name

| Field | Value |
|-------|-------|
| **Pipeline Name** | <pipeline_name> |
| **Source System(s)** | <source_system> |
| **Target System(s)** | <target_system> |
| **Layer(s)** | Bronze / Silver / Gold |
| **Orchestrator** | ADF / Databricks / Airflow / Other |
| **Schedule** | <cron_expression_or_frequency> |

---

## 3. Test Scope

### In Scope
- [ ] Source extraction (full and incremental)
- [ ] Column mapping and type conversion
- [ ] Data quality rule execution
- [ ] Bronze → Silver transformation logic
- [ ] Silver → Gold aggregation logic
- [ ] Merge/upsert behavior (SCD Type 1/2)
- [ ] Error handling and retry logic
- [ ] Watermark management
- [ ] Schema drift handling
- [ ] Performance under expected data volume

### Out of Scope
- [ ] Source system internal logic
- [ ] BI/reporting layer validations
- [ ] Network/infrastructure stress testing
- [ ] Security/penetration testing

### Assumptions
- Test environment is provisioned and accessible
- Source data fixtures are available in test containers
- Required Databricks/ADF compute is allocated

### Dependencies
- Metadata configuration file (`metadata-config.yaml`)
- DQ rules file (`dq-rules.yaml`)
- Source schema documentation
- Environment variable / Key Vault secrets

---

## 4. Test Environment

| Resource | Test Value |
|----------|-----------|
| **Databricks Workspace URL** | `https://<test-workspace>.azuredatabricks.net` |
| **Databricks Cluster** | `<test_cluster_id>` |
| **ADLS Gen2 Account** | `<test_storage_account>` |
| **Container** | `<test_container>` |
| **Delta Table Root** | `abfss://<container>@<account>.dfs.core.windows.net/<path>` |
| **ADF Name** | `<test_adf_name>` |
| **Metadata Database** | `<test_metadata_db>` |
| **Python Version** | 3.11 |
| **Spark Version** | <spark_version> |
| **Pytest Version** | >= 8.0 |

---

## 5. Test Data Strategy

| Strategy | Description |
|----------|-------------|
| **Fixture Source** | Curated Parquet/CSV files in test data lake path |
| **Volume** | Small (1K rows) for unit tests; Medium (100K rows) for integration; Large (1M+ rows) for performance |
| **Sensitive Data** | Synthetic / masked data only; no PII in test environment |
| **Edge Cases** | Nulls, empty strings, unicode, dates outside range, duplicate keys, schema drift |
| **Refresh Strategy** | Fixtures versioned in Git; rebuilt via `make test-data` |
| **Cleanup** | Tables dropped and recreated per test suite run |

---

## 6. Test Cases

### 6.1 Unit Tests

| TC ID | Description | Input | Expected Output | Status |
|-------|-------------|-------|-----------------|--------|
| UT-001 | Column type conversion (string → date) | `{"dob": "1990-01-15"}` | `java.sql.Date(1990-01-15)` | Pending |
| UT-002 | Null handling for required column | `{"email": null}` | Row quarantined, DQ error logged | Pending |
| UT-003 | String normalization (trim + upper) | `"  john  "` | `"JOHN"` | Pending |
| UT-004 | Currency formatting | `1234.5` | `"1,234.50"` | Pending |
| UT-005 | SCD2 merge logic — new row | New row with new key | Inserted into target with `start_date = today` | Pending |
| UT-006 | SCD2 merge logic — changed attribute | Existing key with changed column | Old row closed (`end_date = today`), new row inserted | Pending |

### 6.2 Integration Tests

| TC ID | Description | Input | Expected Output | Status |
|-------|-------------|-------|-----------------|--------|
| IT-001 | End-to-end Bronze write + read | Source fixture → Bronze | Bronze Delta table count matches source | Pending |
| IT-002 | Bronze → Silver transformation | Bronze fixture | Silver table with transformed columns | Pending |
| IT-003 | Watermark incremental load | 2 batches of source data | Only second batch loaded on second run | Pending |
| IT-004 | ADF pipeline trigger via API | Pipeline parameters | Pipeline run ID returned, status = Succeeded | Pending |
| IT-005 | Databricks notebook execution | Notebook path + params | Notebook run succeeds, output table populated | Pending |

### 6.3 End-to-End Tests

| TC ID | Description | Input | Expected Output | Status |
|-------|-------------|-------|-----------------|--------|
| E2E-001 | Full pipeline: source → Gold | Complete source fixture | Gold table matches expected result set | Pending |
| E2E-002 | Pipeline failure + retry | Simulated transient error | Pipeline retries per policy, succeeds on 2nd attempt | Pending |
| E2E-003 | Schema drift — new column added | Source with extra column | Column added to Bronze, flagged in metadata | Pending |

### 6.4 Data Quality Tests

| TC ID | Description | Rule | Expected Output | Status |
|-------|-------------|------|-----------------|--------|
| DQ-001 | Completeness — not null | `customer_id IS NOT NULL` | 0 violations | Pending |
| DQ-002 | Uniqueness — primary key | `COUNT(customer_id) = COUNT(DISTINCT customer_id)` | 0 duplicates | Pending |
| DQ-003 | Range — age between 0-120 | `age BETWEEN 0 AND 120` | 0 out-of-range rows | Pending |
| DQ-004 | Format — email pattern | `email RLIKE '^[^@]+@[^@]+\\.[^@]+$'` | 0 format violations | Pending |
| DQ-005 | Domain — status code | `status IN ('active','inactive','pending')` | 0 invalid status values | Pending |
| DQ-006 | Calculation — total = qty × price | `ABS(total - qty * price) < 0.01` | 0 calculation errors | Pending |

### 6.5 Performance Tests

| TC ID | Description | Data Volume | Target Metric | Status |
|-------|-------------|-------------|---------------|--------|
| PT-001 | Bronze write throughput | 1M rows | < 5 minutes | Pending |
| PT-002 | Silver transformation | 1M rows | < 10 minutes | Pending |
| PT-003 | Gold aggregation query | 10M rows | < 30 seconds | Pending |
| PT-004 | Concurrent pipeline runs | 2 parallel runs | No deadlocks, both succeed | Pending |

---

## 7. Sign-off Criteria

- [ ] All critical (P0/P1) test cases passed
- [ ] 0 data quality rule violations at `error` severity
- [ ] Warning-severity DQ violations documented and accepted by data owner
- [ ] Performance targets met for all PT-* test cases
- [ ] Pipeline run succeeds end-to-end in test environment
- [ ] Code coverage ≥ 80% for transformation modules
- [ ] No open `detect-secrets` findings
- [ ] Documentation updated (pipeline README, data catalog entry)

**Sign-off:**

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Data Engineer | | | |
| Data Owner | | | |
| QA Lead | | | |
| Tech Lead | | | |

---

## 8. Risks

| Risk ID | Description | Likelihood | Impact | Mitigation |
|---------|-------------|------------|--------|------------|
| R-001 | Test data doesn't reflect production edge cases | Medium | High | Review with source SME; expand fixtures |
| R-002 | Databricks compute unavailable during test window | Low | High | Pre-reserve cluster; have fallback workspace |
| R-003 | Schema drift in source breaks pipeline | Medium | High | Schema drift handling tested in E2E-003 |
| R-004 | Performance targets not met at scale | Medium | Medium | Run PT tests early; optimize partitioning |
| R-005 | Metadata misconfiguration | Low | High | Validate metadata in CI before pipeline run |

---

## 9. Schedule

| Phase | Activity | Start Date | End Date | Owner |
|-------|----------|------------|----------|-------|
| 1 | Test plan review and approval | YYYY-MM-DD | YYYY-MM-DD | QA Lead |
| 2 | Test data fixture creation | YYYY-MM-DD | YYYY-MM-DD | Data Engineer |
| 3 | Unit + DQ test development | YYYY-MM-DD | YYYY-MM-DD | Data Engineer |
| 4 | Integration + E2E test development | YYYY-MM-DD | YYYY-MM-DD | Data Engineer |
| 5 | Performance test execution | YYYY-MM-DD | YYYY-MM-DD | Data Engineer |
| 6 | Regression and sign-off | YYYY-MM-DD | YYYY-MM-DD | QA Lead |

---

## 10. Appendix

- A: Metadata configuration file
- B: DQ rules file
- C: Test fixture file listing
- D: Spark session configuration
- E: CI/CD pipeline definition reference
