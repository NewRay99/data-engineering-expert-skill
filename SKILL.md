---
name: data-engineering-expert
description: "Use when building, reviewing, or governing data engineering pipelines on Azure Data Factory + Databricks. Covers Medallion architecture (Bronze/Silver/Gold), Delta Live Tables, data quality checks (format, range, domain, calculation), Master Data Management with survivorship, time-series handling, metadata-driven solutions, pytest test plans, pre-commit hooks, git hygiene, and new-engineer onboarding. Continuously evaluates latest Databricks/Azure features and adopts them only after system testing."
version: 1.0.0
author: Hermes Agent
license: MIT
platforms: [linux, macos, windows]
metadata:
  hermes:
    tags: [data-engineering, databricks, azure-data-factory, medallion, delta-lake, data-quality, mdm, time-series, metadata-driven, testing, git-hygiene, pre-commit, pytest]
    related_skills: [github-pr-workflow, github-auth, requesting-code-review, test-driven-development]
---

# Data Engineering Expert — Azure Data Factory + Databricks

## Overview

This skill provides industry-standard data engineering practices for pipelines built on **Azure Data Factory (ADF)** for ingestion and **Databricks** for transformation, following the **Medallion architecture** (Bronze → Silver → Gold). It is designed for teams that need:

- Repeatable, testable, metadata-driven pipelines
- Data quality enforcement at every layer
- Master Data Management (MDM) with survivorship rules
- Time-series aware transformations
- Git hygiene with pre-commit hooks, pytest, and PR-based code review
- A clear onboarding path for new engineers

**Core principle:** Continuously evaluate the latest Databricks and Azure features. Adopt new methods **only** after they pass a full system test — never on release day. See `references/official-documentation-links.md` for live doc URLs to check for new features and patches.

## When to Use

- Building a new data pipeline on Azure + Databricks
- Reviewing or refactoring an existing pipeline for standards compliance
- Onboarding a new engineer to a data engineering codebase
- Implementing data quality, MDM, time-series, or metadata-driven patterns
- Setting up testing, pre-commit hooks, or git hygiene for a data project
- Checking if new Databricks/Azure features should be adopted

**Don't use for:**
- Real-time streaming-only architectures (Kafka/Flink without Databricks) — this skill focuses on batch and micro-batch via Databricks
- Non-Azure cloud setups (AWS/GCP) — patterns transfer but commands differ

## Architecture: Medallion (Bronze → Silver → Gold)

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   BRONZE        │     │   SILVER         │     │   GOLD          │
│   (Raw)         │────▶│   (Cleansed)     │────▶│   (Curated)     │
│                 │     │                  │     │                 │
│ • Exact copy    │     │ • Deduplicated   │     │ • Business-ready │
│ • Schema-on-read│     │ • Quality checks │     │ • Aggregated    │
│ • Delta format  │     │ • Conformed dims │     │ • Star schema   │
│ • ADF ingests   │     │ • Databricks     │     │ • Databricks    │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

**Detailed guide:** `references/medallion-architecture.md`

### Layer Responsibilities

| Layer | Ingestion | Transformation | Quality Gate |
|-------|-----------|----------------|--------------|
| Bronze | ADF copy activities → Delta tables | None (raw landing) | Row count + schema check |
| Silver | Databricks notebook/JDBC from Bronze | Filter, cast, deduplicate, join | Full DQ suite (format, range, domain, calc) |
| Gold | Databricks from Silver | Aggregate, conform, enrich | Business rules + reconciliation |

## Ingestion: Azure Data Factory

ADF handles all source-to-Bronze ingestion. Key patterns:

- **Linked Services** for source systems (REST APIs, SQL Server, Blob Storage, Snowflake, etc.)
- **Copy Activities** with Delta Lake sink (via Databricks Delta or direct ADLS Gen2)
- **Pipeline orchestration** with triggers (schedule, tumbling window, event-based)
- **Integration Runtimes** — Self-hosted for on-prem, Azure IR for cloud
- **Parameterized datasets** for metadata-driven ingestion (see `references/metadata-driven-solutions.md`)

**Detailed guide:** `references/azure-data-factory-ingestion.md`
**ADF pipeline template:** `templates/adf-pipeline-template.json`

## Transformation: Databricks

Databricks handles all Silver and Gold transformations. Key components:

- **Unity Catalog** for governance, access control, and lineage
- **Delta Lake** as the storage format across all layers (ACID, time travel, Z-ordering)
- **Notebooks or SQL files** for transformation logic
- **Databricks Workflows** (formerly Jobs) for orchestration
- **Cluster policies** for cost control and standardization

**Detailed guide:** `references/databricks-transformation.md`
**Notebook template:** `templates/databricks-notebook-template.py`

## Options (Select Per Project)

### 1. Delta Live Tables (DLT)

Declarative pipeline framework for Bronze→Silver→Gold with built-in data quality (expectations).

- **When to choose:** Greenfield pipelines, streaming + batch, want auto-scaling and auto-retries
- **When NOT to choose:** Complex branching logic, existing notebook-based pipelines that work fine, need fine-grained cluster control

**Detailed guide:** `references/delta-live-tables.md`

### 2. Data Quality Framework

Enforce quality at every layer with six check categories:

| Check Type | Description | Example |
|------------|-------------|---------|
| **Format** | Regex / type conformance | Email matches `^[\w.]+@[\w]+\.[a-z]{2,}$` |
| **Range** | Min/max bounds | Price between 0 and 1,000,000 |
| **Domain** | Value in allowed set | Status ∈ {active, pending, closed} |
| **Calculation** | Cross-field / cross-row | `end_date >= start_date` |
| **Completeness** | No nulls in required fields | `order_id IS NOT NULL` |
| **Uniqueness** | No duplicates on key | `customer_id` appears once |

**Detailed guide:** `references/data-quality-framework.md`
**DQ rules template:** `templates/dq-rules-template.yaml`
**Validation script:** `scripts/validate_data_quality.py`

### 3. Master Data Management (MDM) with Survivorship

Create and maintain golden records from multiple source systems.

- **Survivorship strategies:** Last-wins, source-priority, most-complete, most-frequent, rules-based
- **Deduplication:** Deterministic (exact key match) and probabilistic (fuzzy matching with similarity scores)
- **SCD Type 2:** Track historical changes to master records
- **Golden record maintenance:** Scheduled reconciliation, conflict resolution, audit trail

**Detailed guide:** `references/master-data-management.md`

### 4. Time-Series Handling

Patterns for temporal data in Silver/Gold layers.

- **Window functions:** tumbling, sliding, session windows
- **As-of joins:** point-in-time lookups (e.g., latest exchange rate as of trade date)
- **Business calendars:** trading days vs calendar days, holiday handling
- **Gap detection:** identify missing time periods in series
- **Resampling:** upsample/downsample with forward-fill, interpolation

**Detailed guide:** `references/time-series-handling.md`

### 5. Metadata-Driven Solutions

Drive pipelines from configuration tables rather than hardcoded logic.

- **Metadata tables:** source config, column mappings, DQ rules, transformation rules
- **Dynamic SQL/M:** generate pipeline logic from metadata
- **ADF parameterization:** linked services, datasets, and activities driven by lookup + ForEach
- **Databricks parameterization:** notebook params from metadata, dynamic table names

**Detailed guide:** `references/metadata-driven-solutions.md`
**Metadata config template:** `templates/metadata-config-template.yaml`

## Testing Standards

Every pipeline change must include tests before merging. See `references/testing-and-test-plans.md` for the full framework.

### Test Pyramid

```
        ┌─────────┐
        │  E2E    │  ← Full pipeline run on small sample (slow, 1-3 tests)
        ├─────────┤
        │   Int.  │  ← Silver→Gold transformation on test data (medium)
        ├─────────┤
        │  Unit   │  ← Individual functions, SQL logic, DQ rules (fast, many)
        └─────────┘
```

### Tools

- **pytest** — Python unit tests for UDFs, DQ logic, transformation functions
- **Great Expectations** (optional) — declarative data quality testing
- **Databricks Repos + CI** — run notebook tests via Databricks asset bundles
- **ADF CI/CD** — Azure DevOps / GitHub Actions ARM template deployment

**Test plan template:** `templates/test-plan-template.md`
**pytest conftest:** `templates/conftest.py`

## Git Hygiene Standards

See `references/git-hygiene-standards.md` for the full standard.

### Branching Strategy (GitHub Flow)

```
main (protected, requires PR review + passing CI)
 ├── feature/ADF-123-add-customer-ingestion
 ├── feature/DBT-456-silver-dedup-rules
 └── hotfix/fix-dq-range-check-prices
```

### Commit Convention (Conventional Commits)

```
feat: add customer dedup survivorship in silver layer
fix: correct exchange rate as-of join for weekends
docs: update MDM guide with fuzzy matching thresholds
test: add pytest for DQ range check on price column
refactor: extract metadata loader into shared module
```

### Pre-Commit Hooks

See `references/pre-commit-hooks.md` for setup. Config template: `templates/pre-commit-config.yaml`

Key hooks:
- **SQLFluff** — SQL formatting and linting
- **black + isort** — Python formatting
- **sqlfluff-fix** — auto-fix SQL style
- **detect-secrets** — prevent API keys/passwords in code
- **end-of-file-fixer** — ensure files end with newline
- **yaml-check** — validate YAML files

## New Engineer Onboarding

See `references/new-engineer-guide.md` for the step-by-step guide.

**Quick start:**
1. Clone the repo, install pre-commit hooks
2. Get Azure + Databricks access (service principal or AAD)
3. Run the test suite: `pytest tests/ -v`
4. Pick a `good-first-issue` ticket
5. Create a feature branch, make changes, run tests + pre-commit
6. Open a PR, request review, address feedback
7. Merge after CI passes — **never commit directly to main**

## Official Documentation Links

Always check these for new features, patches, and breaking changes before adopting:

**Databricks:**
- Databricks Documentation: https://docs.databricks.com/
- Delta Lake Documentation: https://docs.databricks.com/delta/
- Delta Live Tables: https://docs.databricks.com/dlt/
- Unity Catalog: https://docs.databricks.com/data-governance/unity-catalog/
- Databricks Asset Bundles: https://docs.databricks.com/dev-tools/bundles/
- Databricks Release Notes: https://docs.databricks.com/release-notes/
- Databricks SQL Reference: https://docs.databricks.com/sql/language-manual/

**Azure:**
- Azure Data Factory Docs: https://learn.microsoft.com/azure/data-factory/
- ADF Connector Reference: https://learn.microsoft.com/azure/data-factory/connector-overview
- ADF Pipeline & Activities: https://learn.microsoft.com/azure/data-factory/concepts-pipelines-activities
- Azure Integration Runtime: https://learn.microsoft.com/azure/data-factory/concepts-integration-runtime
- Azure DevOps Pipelines: https://learn.microsoft.com/azure/devops/pipelines/
- Azure Updates (check monthly): https://azure.microsoft.com/updates/
- Azure Architecture Center: https://learn.microsoft.com/azure/architecture/

**Detailed link registry with update-check instructions:** `references/official-documentation-links.md`

## Learning Material: Databricks Pioneers & Advanced DataOps

A curated guide of leading pioneers in the Databricks ecosystem covering DABs, CI/CD automation, DLT, and software engineering data standards.

- **Hubert Dudek** — Docker runtimes, MDM optimizations, Delta tuning
- **Alessandro Armillotta** — Zero-trust CI/CD, OIDC auth, validate/plan gates
- **Maciej Tarsa** — Decoupled PySpark logic, modular architecture, dev isolation
- **Simon Doy** — IaC integration, workflow determinism, Unity Catalog alignment

**Detailed guide:** `references/databricks-pioneers-guide.md`

## Common Pitfalls

1. **Skipping Bronze quality gate.** Even raw data needs a row count check. A failed source extraction with zero rows will silently propagate nulls downstream.

2. **Hardcoding table names and file paths.** Use metadata tables and parameterized pipelines. See `references/metadata-driven-solutions.md`.

3. **Adopting new Databricks features on release day.** Wait for at least one patch release. Test against a non-production workspace first. Check the release notes for known issues.

4. **Not versioning Databricks notebooks.** Use Databricks Repos to sync with Git. Never develop directly in the workspace without version control.

5. **Ignoring time travel for debugging.** Delta Lake's `VERSION AS OF` and `TIMESTAMP AS OF` are your best debugging tools. Use them before re-running pipelines.

6. **Pre-commit hooks not installed.** New engineers must run `pre-commit install` after cloning. Add it to the onboarding checklist. See `references/new-engineer-guide.md`.

7. **No test data strategy.** Don't test against production data. Create representative sample datasets in a dev container/catalog.

8. **Mixing ingestion and transformation logic.** ADF should only land data in Bronze. All cleansing, dedup, and business logic belongs in Databricks. Violating this creates unmaintainable pipelines.

9. **DLT expectations without actions.** Define `ON VIOLATION` behavior (drop, fail, retry) explicitly. Silent row drops are worse than pipeline failures.

10. **No MDM audit trail.** Survivorship decisions must be logged — who, when, what changed, why. Without this, golden record changes are unexplainable.

## Verification Checklist

- [ ] ADF pipelines land data in Bronze as Delta format
- [ ] Bronze tables have row count + schema quality gate
- [ ] Silver transformations run on Databricks (not ADF)
- [ ] Data quality checks (format, range, domain, calculation) defined and enforced
- [ ] MDM survivorship rules documented and implemented (if applicable)
- [ ] Time-series transformations handle gaps and business calendars (if applicable)
- [ ] Metadata tables drive pipeline configuration (not hardcoded values)
- [ ] pytest suite passes locally before PR
- [ ] Pre-commit hooks installed and passing
- [ ] PR opened against `main`, not direct commit
- [ ] CI pipeline (GitHub Actions / Azure DevOps) passing
- [ ] New engineers have completed onboarding checklist
- [ ] Latest Databricks/Azure features reviewed (check release notes monthly)
- [ ] Official documentation links reviewed for new features/patches

## Adopting New Features — Evaluation Process

1. **Monitor:** Check `references/official-documentation-links.md` monthly for new releases
2. **Assess:** Read release notes, breaking changes, and known issues
3. **Spike:** Create a `spike/` branch, build a minimal proof-of-concept
4. **System Test:** Run the full test suite against the new feature
5. **Document:** Update the relevant reference doc with the new approach
6. **Review:** Present findings to the team, get approval
7. **Adopt:** Merge into `main` only after all tests pass and team approves

**Never skip steps 3-4.** Untested features in production are the #1 cause of pipeline outages.

## Maintenance & Extension

For maintaining and extending this skill (adding references, templates, scripts, bulk updates, GitHub backup workflow, monthly review checklist):

**Detailed guide:** `references/maintenance-and-extension-guide.md`
