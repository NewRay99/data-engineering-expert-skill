# Git Hygiene Standards

## Overview

Consistent Git hygiene is the foundation of a maintainable, auditable, and collaborative data engineering codebase. This document defines the branching strategy, commit conventions, PR workflow, and review standards that all data engineering teams must follow.

---

## 1. Branching Strategy

We use a **trunk-based development** model with short-lived feature branches. This balances speed of integration with the safety of code review.

### 1.1 Branch Model

```
main (protected)
  │
  ├── feature/DAT-123-add-customer-ingestion ──→ PR → main
  ├── feature/DAT-124-silver-transformations  ──→ PR → main
  ├── bugfix/DAT-125-fix-watermark-update     ──→ PR → main
  └── release/v2.1.0 (tagged from main)
```

### 1.2 Branch Naming Convention

```
<type>/<JIRA-ID>-<short-description>
```

| Type | Purpose | Example |
|---|---|---|
| `feature` | New functionality | `feature/DAT-123-add-customer-ingestion` |
| `bugfix` | Bug fix | `bugfix/DAT-125-fix-watermark-update` |
| `hotfix` | Urgent production fix | `hotfix/DAT-126-fix-pipeline-failure` |
| `release` | Release preparation | `release/v2.1.0` |
| `chore` | Maintenance, refactoring | `chore/DAT-127-update-adf-sdk` |
| `docs` | Documentation only | `docs/DAT-128-update-readme` |

### 1.3 Branch Lifecycle Rules

- **Maximum branch lifetime**: 3 working days. If a branch lives longer, it must be rebased against `main` daily.
- **Maximum branch size**: 400 lines changed. If larger, split into multiple branches/PRs.
- **No direct commits to `main`**: All changes go through PR.
- **No merge commits**: Use **squash and merge** to keep history linear.
- **Delete branches after merge**: Configure Azure DevOps / GitHub to auto-delete.

### 1.4 Branch Protection Rules

```
Branch: main
  ✅ Require pull request before merging
  ✅ Require at least 2 reviewers
  ✅ Require status checks: [Unit Tests, Integration Tests, Lint]
  ✅ Require branches to be up to date before merging
  ✅ Do not allow bypassing the above
  ✅ Require linear history (squash merge only)
```

---

## 2. Commit Conventions

### 2.1 Commit Message Format

We follow the **Conventional Commits** specification:

```
<type>(<scope>): <subject>

<body>

<footer>
```

### 2.2 Types

| Type | Description |
|---|---|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation changes |
| `style` | Code style (formatting, no logic change) |
| `refactor` | Code refactoring (no feature/fix) |
| `perf` | Performance improvement |
| `test` | Adding or modifying tests |
| `chore` | Build, dependencies, CI/CD, tooling |
| `ci` | CI/CD pipeline changes |
| `revert` | Reverting a previous commit |

### 2.3 Examples

```
feat(ingestion): add SAP customer source to metadata config

- Added source_config row for SAP CRM CUSTOMERS table
- Added 15 column mappings (KUNNR → customer_id, NAME1 → customer_name, etc.)
- Added 3 DQ rules: NOT_NULL on customer_id, UNIQUE on customer_id, REGEX on email
- Updated metadata-config.yaml

JIRA: DAT-123
```

```
fix(silver): correct watermark comparison operator in incremental load

The incremental filter was using >= instead of >, causing the last row
of each run to be reprocessed. Changed to > to ensure only new rows
are picked up.

JIRA: DAT-125
```

```
refactor(dq): extract DQ rule execution into reusable function

Moved execute_dq_rules() from notebook-level code into src/dq_engine.py
for reusability across pipelines. No behavior change.

JIRA: DAT-127
```

### 2.4 Commit Rules

1. **One logical change per commit**: Don't mix a feature addition with a dependency upgrade.
2. **Atomic commits**: Each commit should leave the codebase in a buildable state.
3. **Write descriptive messages**: The subject line should complete the sentence "If applied, this commit will ___".
4. **Reference tickets**: Include JIRA ticket ID in the footer.
5. **No secrets in commits**: Never hard-code connection strings, passwords, or API keys. Use Key Vault references.
6. **No generated files**: Don't commit build outputs, `.class` files, or compiled notebooks. Use `.gitignore`.

### 2.5 .gitignore for Data Engineering

```gitignore
# Build outputs
__pycache__/
*.pyc
*.pyo
*.class
*.jar
dist/
build/
*.egg-info/

# Databricks
.databricks/
*.egg

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# Secrets (NEVER commit these)
*.env
*.key
*.pem
secrets.json
local.settings.json

# Data files (use ADLS, not Git)
*.csv
*.parquet
*.delta
*.xlsx
*.json.gz
data/

# Logs
*.log
logs/

# OS
.DS_Store
Thumbs.db
```

---

## 3. Pull Request Workflow

### 3.1 PR Creation Checklist

Before creating a PR, ensure:

- [ ] Branch is rebased on latest `main`
- [ ] All unit tests pass locally
- [ ] Linting passes (flake8, black, sqlfluff)
- [ ] Commit messages follow conventional commits format
- [ ] No secrets in code (run pre-commit hook)
- [ ] If new source: metadata YAML updated and validated
- [ ] If schema change: downstream impact assessed
- [ ] PR description filled out completely

### 3.2 PR Description Template

```markdown
## Description
<!-- What does this PR do? Why is it needed? -->

## JIRA Ticket
<!-- Link to JIRA ticket -->
[JIRA: DAT-XXX](https://yourorg.atlassian.net/browse/DAT-XXX)

## Type of Change
- [ ] New feature (non-breaking)
- [ ] Bug fix (non-breaking)
- [ ] Breaking change (fix or feature that causes existing functionality to change)
- [ ] Documentation update
- [ ] Refactoring (no functional change)
- [ ] Performance improvement

## Changes Made
<!-- Bullet list of changes -->
-
-
-

## Testing
<!-- How was this tested? -->
- [ ] Unit tests added/updated
- [ ] Integration tests pass in test environment
- [ ] E2E test executed (if applicable)
- [ ] Manual testing in dev workspace

## Downstream Impact
<!-- Does this affect other pipelines, semantic models, or reports? -->
- [ ] No downstream impact
- [ ] Downstream impact assessed and communicated

## Metadata Changes
<!-- If adding/modifying metadata sources -->
- [ ] metadata-config.yaml updated
- [ ] Metadata validation passed
- [ ] DQ rules added for new sources

## Checklist
- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Comments added for complex logic
- [ ] No new warnings in build
- [ ] Documentation updated (if applicable)
- [ ] No secrets committed
```

### 3.3 Code Review Standards

#### Reviewer Responsibilities

1. **Review within 4 working hours** of assignment. If you can't, unassign yourself.
2. **Review for correctness first**: Does the code do what the PR says?
3. **Review for testability**: Are there tests? Do they cover edge cases?
4. **Review for maintainability**: Will someone understand this in 6 months?
5. **Review for security**: No hardcoded secrets, no SQL injection vulnerabilities.
6. **Review for performance**: Are there obvious performance anti-patterns?

#### Reviewer Guidelines

- **Be specific**: "This is wrong" → "This query doesn't handle NULLs in the `customer_id` column. Consider adding `COALESCE(customer_id, '')`."
- **Be constructive**: Suggest alternatives, don't just point out problems.
- **Distinguish blocking from non-blocking**: Use "Must fix" vs "Nit" / "Suggestion".
- **Approve when ready**: Don't block on nits. Approve with comments.
- **Two approvals required** for merging to `main`.

#### Author Responsibilities

- **Respond to all comments**: Either address the feedback or explain why you disagree.
- **Push updates promptly**: Don't leave reviewers waiting for updates.
- **Resolve threads**: Mark conversations as resolved once addressed.
- **Don't force-push after review starts**: Add new commits for review clarity. Squash at merge time.

---

## 4. Pre-Commit Hooks

### 4.1 Setup

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.6.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-json
      - id: check-merge-conflict
      - id: check-added-large-files
        args: ['--maxkb=500']
      - id: detect-private-key
      - id: no-commit-to-branch
        args: ['--branch', 'main']

  - repo: https://github.com/psf/black
    rev: 24.4.2
    hooks:
      - id: black
        language_version: python3.11

  - repo: https://github.com/pycqa/flake8
    rev: 7.1.0
    hooks:
      - id: flake8
        args: ['--max-line-length=120', '--extend-ignore=E203,W503']

  - repo: https://github.com/sqlfluff/sqlfluff
    rev: 3.1.0
    hooks:
      - id: sqlfluff-lint
        args: ['--dialect', 'spark_sql']
      - id: sqlfluff-fix
        args: ['--dialect', 'spark_sql']

  - repo: https://github.com/Yelp/detect-secrets
    rev: v1.5.0
    hooks:
      - id: detect-secrets
        args: ['--baseline', '.secrets.baseline']
```

### 4.2 Installation

```bash
pip install pre-commit
pre-commit install
pre-commit install --hook-type commit-msg
```

---

## 5. Repository Structure

```
data-engineering/
├── .github/                    # or .azuredevops/
│   └── pull_request_template.md
├── .gitignore
├── .pre-commit-config.yaml
├── .secrets.baseline           # detect-secrets baseline
├── README.md
├── pyproject.toml
├── requirements.txt
├── requirements-dev.txt
│
├── pipelines/                  # ADF pipeline JSON
│   ├── pl_metadata_ingestion.json
│   ├── pl_silver_transform.json
│   └── pl_gold_aggregation.json
│
├── datasets/                   # ADF dataset JSON
│   ├── Ds_DynamicSource.json
│   └── Ds_DynamicParquetSink.json
│
├── linkedServices/             # ADF linked service JSON
│   ├── Ls_AdlsGen2.json
│   └── Ls_KeyVaultSql.json
│
├── notebooks/                  # Databricks notebooks
│   ├── bronze/
│   │   └── ingest_source.py
│   ├── silver/
│   │   ├── transform_customers.py
│   │   └── transform_orders.py
│   └── gold/
│       └── aggregate_customer_360.py
│
├── src/                        # Shared Python modules
│   ├── __init__.py
│   ├── metadata.py
│   ├── transformations.py
│   ├── dq_engine.py
│   └── utils.py
│
├── metadata/                   # Source-controlled metadata
│   └── metadata-config.yaml
│
├── tests/
│   ├── unit/
│   │   ├── test_transformations.py
│   │   └── test_dq_engine.py
│   ├── integration/
│   │   ├── test_adf_pipeline.py
│   │   └── test_databricks_notebook.py
│   └── e2e/
│       └── test_customer_360.py
│
├── terraform/                  # Infrastructure as Code
│   ├── main.tf
│   ├── variables.tf
│   └── environments/
│       ├── dev.tfvars
│       ├── test.tfvars
│       └── prod.tfvars
│
├── ci/
│   ├── azure-pipelines.yml
│   └── azure-pipelines-test.yml
│
├── docs/
│   ├── architecture.md
│   ├── runbooks/
│   └── adr/                    # Architecture Decision Records
│       └── 001-metadata-driven-architecture.md
│
└── config/
    ├── dev/
    ├── test/
    └── prod/
```

---

## 6. Release Management

### 6.1 Semantic Versioning

```
v<MAJOR>.<MINOR>.<PATCH>

v2.1.3
 │ │ └── Patch: bug fixes, no new features
 │ └──── Minor: new features, backward compatible
 └────── Major: breaking changes
```

### 6.2 Release Process

1. **Create release branch**: `release/v2.1.0` from `main`
2. **Run full test suite**: Unit + Integration + E2E in test environment
3. **Update version**: Bump version in `pyproject.toml`, README, and any version-tracking files
4. **Create release notes**: Summarize all changes since last release
5. **Tag the release**: `git tag -a v2.1.0 -m "Release v2.1.0"`
6. **Deploy to production**: Via CI/CD pipeline (manual approval gate)
7. **Merge release branch back to main**: If any fixes were made on the release branch

### 6.3 Release Notes Template

```markdown
# Release v2.1.0

**Release Date**: 2025-07-05
**Release Manager**: [Name]

## Breaking Changes
- None

## New Features
- DAT-123: Added SAP customer ingestion source
- DAT-130: Added metadata-driven DQ framework

## Bug Fixes
- DAT-125: Fixed watermark comparison operator in incremental loads
- DAT-131: Fixed null handling in customer name transformation

## Improvements
- DAT-127: Refactored DQ engine for reusability
- DAT-132: Improved pipeline logging with structured JSON output

## Infrastructure
- DAT-133: Upgraded Databricks runtime to 14.3 LTS
- DAT-134: Added auto-scaling to Silver transformation cluster

## Known Issues
- DAT-135: CDC mode not yet supported for SAP sources (planned for v2.2.0)

## Migration Steps
1. Update metadata-config.yaml with new SAP customer source definition
2. Run `python scripts/migrate_metadata.py --env prod`
3. Deploy updated ADF pipelines via CI/CD
4. Verify pipeline execution in production
```

---

## 7. Hotfix Process

```
main ────────────────────────────────────────
       \                                  /
        hotfix/v2.1.1-fix-pipeline ──────→ PR → main + tag v2.1.1
```

1. **Branch from the release tag**: `git checkout -b hotfix/v2.1.1-fix-pipeline v2.1.0`
2. **Make the minimal fix**: One commit, one purpose
3. **Test in test environment**: At minimum, run the affected pipeline's tests
4. **PR to main**: Expedited review (1 reviewer minimum for hotfixes)
5. **Tag and deploy**: `git tag -a v2.1.1 -m "Hotfix: pipeline failure"`
6. **Post-mortem**: Create a post-mortem document within 48 hours

---

## 8. Conflict Resolution

### 8.1 Rebase Strategy

```bash
# Keep your feature branch up to date with main
git fetch origin
git rebase origin/main

# If conflicts arise, resolve them file by file
git status                          # See conflicted files
# Edit files to resolve conflicts
git add <resolved-file>
git rebase --continue

# Never use git merge on feature branches
# Always rebase to keep history clean
```

### 8.2 Conflict Resolution Principles

1. **Understand both sides**: Read both versions before deciding.
2. **Prefer the main branch version** unless your change explicitly supersedes it.
3. **Test after resolving**: Run unit tests to ensure the merge didn't break anything.
4. **Communicate**: If a conflict is non-trivial, talk to the other author before resolving.

---

## 9. Secrets Management in Git

### 9.1 Golden Rule

> **Never commit secrets to Git.** Git history is permanent. Even if you delete the file, the secret remains in history.

### 9.2 Acceptable Patterns

```python
# GOOD: Reference Key Vault secret by name
connection_string = dbutils.secrets.get(scope="kv-dataeng", key="metadata-db-url")

# GOOD: Use environment variable
api_key = os.environ["API_KEY"]

# GOOD: Use Azure Key Vault task in ADF
# (configured in linked service, not in pipeline JSON)

# BAD: Hardcoded connection string
connection_string = "Server=tcp:sql-prod.database.windows.net,1433;User=admin;Password=P@ssw0rd123!"
```

### 9.3 If a Secret Is Accidentally Committed

1. **Immediately rotate the secret** in Azure Key Vault / the source system.
2. **Remove from history** using `git filter-repo` or BFG Repo-Cleaner:
   ```bash
   git filter-repo --replace-text <(echo "P@ssw0rd123!==>***REMOVED***")
   ```
3. **Force-push** the cleaned history.
4. **Notify the team**: Everyone must re-clone the repository.
5. **Run detect-secrets** to update the baseline.
6. **Post-mortem**: Document what happened and how to prevent recurrence.

---

## 10. Git Hygiene Checklist

- [ ] `main` branch is protected (no direct pushes)
- [ ] Squash-and-merge is enforced
- [ ] PR template is in place and used
- [ ] Pre-commit hooks installed and passing
- [ ] `.gitignore` covers all generated/secret file types
- [ ] Branch names follow convention
- [ ] Commit messages follow conventional commits
- [ ] No secrets in repository history (verified by detect-secrets)
- [ ] Release tags follow semantic versioning
- [ ] Stale branches are cleaned up weekly
- [ ] Repository structure follows the defined layout
- [ ] CI/CD pipeline runs on every PR
- [ ] Two-reviewer approval required for merge to main

---

## 11. Summary

Git hygiene is not about bureaucracy — it's about creating a codebase that is:
- **Reviewable**: Small, well-described PRs that reviewers can assess confidently
- **Traceable**: Every change links to a ticket and a reason
- **Reversible**: Atomic commits and semantic versioning make rollback straightforward
- **Secure**: Secrets never enter version control
- **Collaborative**: Clear conventions reduce friction and merge conflicts

The cost of poor Git hygiene is paid in debugging time, broken pipelines, and team frustration. The investment in these standards pays dividends from day one.
