# Pre-commit Hooks for Data Engineering

## What is Pre-commit?

[Pre-commit](https://pre-commit.com/) is a framework for managing and maintaining multi-language pre-commit hooks. It runs checks against your staged files **before** they are committed to version control, catching issues early — when they are cheapest to fix.

### Why It Matters for Data Engineering Teams

Data engineering code (SQL transformations, Python pipelines, notebook logic) is often shared across teams and deployed to production environments where failures are costly. Pre-commit hooks provide:

- **Consistent formatting** — eliminates style debates in code reviews (SQLFluff for SQL, Black for Python)
- **Secret prevention** — blocks accidental commits of API keys, connection strings, and passwords
- **Notebook hygiene** — strips Jupyter notebook outputs so diffs stay clean and secrets in cell outputs never reach the repo
- **Early linting** — catches syntax errors, undefined variables, and anti-patterns before CI runs
- **Faster CI** — trivial issues are fixed locally, freeing CI for meaningful test suites
- **Onboarding aid** — new engineers get immediate feedback without needing to know every project convention

---

## Installation

```bash
# Install the pre-commit package
pip install pre-commit

# Install the git hook scripts (run once per clone)
pre-commit install

# (Optional) Install pre-commit hooks for commit-msg as well
pre-commit install --hook-type commit-msg
```

After `pre-commit install`, every `git commit` will automatically trigger the configured hooks against staged files.

---

## The `.pre-commit-config.yaml` File

The configuration file lives at the repository root. A reference template is available at `templates/pre-commit-config.yaml`.

### Structure

```yaml
# .pre-commit-config.yaml

# Default language version for hooks
default_language_version:
  python: python3.11

# Global exclude patterns
exclude: '^docs/|\.json$|\.csv$'

# Hook repositories
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.6.0
    hooks:
      - id: end-of-file-fixer
      - id: trailing-whitespace
      - id: check-yaml
      - id: check-json
      - id: check-merge-conflict

  - repo: https://github.com/sqlfluff/sqlfluff
    rev: 3.1.0
    hooks:
      - id: sqlfluff-lint
        args: [--config, .sqlfluff]
      - id: sqlfluff-fix
        args: [--config, .sqlfluff]

  - repo: https://github.com/psf/black
    rev: 24.4.2
    hooks:
      - id: black
        language_version: python3.11

  - repo: https://github.com/pycqa/isort
    rev: 5.13.2
    hooks:
      - id: isort

  - repo: https://github.com/pycqa/flake8
    rev: 7.0.0
    hooks:
      - id: flake8
        args: [--max-line-length=120, --extend-ignore=E203,W503]

  - repo: https://github.com/Yelp/detect-secrets
    rev: v1.5.0
    hooks:
      - id: detect-secrets
        args: ['--baseline', '.secrets.baseline']

  - repo: https://github.com/kynan/nbstripout
    rev: 0.7.1
    hooks:
      - id: nbstripout
```

---

## Key Hooks for Data Engineering

### 1. SQLFluff (`sqlfluff-lint`, `sqlfluff-fix`)

**Purpose:** Lints and auto-fixes SQL formatting, style, and structural issues. Essential for dbt projects and raw SQL transformations.

```yaml
- repo: https://github.com/sqlfluff/sqlfluff
  rev: 3.1.0
  hooks:
    - id: sqlfluff-lint
      args: [--config, .sqlfluff]
      files: \.(sql)$
    - id: sqlfluff-fix
      args: [--config, .sqlfluff, --force]
      files: \.(sql)$
```

**Notes:**
- Configure dialect (e.g., `sparksql`, `snowflake`, `bigquery`) in `.sqlfluff`
- `sqlfluff-fix` will **modify** your SQL files in place — review staged changes after it runs
- For dbt projects, SQLFluff has a dbt templater that understands Jinja macros

### 2. Black

**Purpose:** Opinionated Python code formatter. Ensures uniform formatting across all Python files.

```yaml
- repo: https://github.com/psf/black
  rev: 24.4.2
  hooks:
    - id: black
      language_version: python3.11
```

**Notes:**
- No configuration needed beyond optional `pyproject.toml` settings (line length, target version)
- Black is non-negotiable — it reformats without asking; this is the point

### 3. isort

**Purpose:** Sorts Python imports alphabetically and by section (stdlib, third-party, local).

```yaml
- repo: https://github.com/pycqa/isort
  rev: 5.13.2
  hooks:
    - id: isort
      args: [--profile, black]
```

**Notes:**
- Always use `--profile black` so isort doesn't fight with Black
- Configure further in `pyproject.toml` under `[tool.isort]`

### 4. Flake8

**Purpose:** Python linting for style, complexity, and logical errors that Black/isort don't cover.

```yaml
- repo: https://github.com/pycqa/flake8
  rev: 7.0.0
  hooks:
    - id: flake8
      args: [--max-line-length=120, --extend-ignore=E203,W503]
```

**Notes:**
- `E203` (whitespace before `:`) conflicts with Black — always ignore it
- `W503` (line break before binary operator) conflicts with PEP 8 modern guidance — ignore it
- Add project-specific rules in `.flake8` or `setup.cfg`

### 5. detect-secrets

**Purpose:** Scans staged files for secrets (API keys, passwords, tokens) before they enter version control.

```yaml
- repo: https://github.com/Yelp/detect-secrets
  rev: v1.5.0
  hooks:
    - id: detect-secrets
      args: ['--baseline', '.secrets.baseline']
```

**Notes:**
- Generate the baseline: `detect-secrets scan > .secrets.baseline`
- If a false positive appears, audit it: `detect-secrets audit .secrets.baseline`
- This is a **critical** hook for data engineering — pipelines often contain connection strings

### 6. end-of-file-fixer

**Purpose:** Ensures every file ends with a newline character. POSIX compliance and cleaner diffs.

```yaml
- id: end-of-file-fixer
```

### 7. trailing-whitespace

**Purpose:** Removes trailing whitespace from every line. Prevents noisy diffs and editor warnings.

```yaml
- id: trailing-whitespace
```

### 8. check-yaml

**Purpose:** Validates YAML syntax in `.yaml`/`.yml` files. Catches indentation and structure errors.

```yaml
- id: check-yaml
```

**Notes:** Some YAML files (e.g., Helm charts with templates) may need `--allow-multiple-documents` or `--unsafe`.

### 9. check-json

**Purpose:** Validates JSON syntax. Prevents malformed config files from being committed.

```yaml
- id: check-json
```

### 10. check-merge-conflict

**Purpose:** Prevents committing files with unresolved merge conflict markers (`<<<<<<<`, `=======`, `>>>>>>>`).

```yaml
- id: check-merge-conflict
```

### 11. nbstripout

**Purpose:** Strips outputs and metadata from Jupyter notebooks before commit. Keeps notebook diffs clean and prevents accidental leakage of data/secrets rendered in cell outputs.

```yaml
- repo: https://github.com/kynan/nbstripout
  rev: 0.7.1
  hooks:
    - id: nbstripout
```

**Notes:**
- Critical for data engineering repos that store notebooks alongside pipeline code
- Can be configured to keep certain metadata: `args: [--keep-count, --keep-output]` (not recommended for shared repos)

---

## Local Hooks for Custom DQ Checks

You can define **local hooks** for project-specific data quality (DQ) checks that don't come from a public repository.

```yaml
repos:
  - repo: local
    hooks:
      # Run project-specific DQ validation script
      - id: dq-check
        name: Data Quality Check
        entry: python -m scripts.run_dq_checks
        language: system
        files: ^transformations/.*\.sql$
        pass_filenames: false

      # Validate dbt model schema files
      - id: dbt-schema-check
        name: dbt Schema Validation
        entry: python -m scripts.validate_dbt_schemas
        language: system
        files: ^models/.*\.yml$
        pass_filenames: false

      # Ensure all SQL files have a header comment
      - id: sql-header-check
        name: SQL Header Comment Check
        entry: bash scripts/check_sql_header.sh
        language: system
        files: \.(sql)$
```

**Guidelines for local hooks:**
- Keep them fast — slow local hooks discourage usage
- Use `pass_filenames: false` when the hook needs to inspect the full repo, not just staged files
- Document the hook's purpose in the script's docstring

---

## CI/CD Integration

### GitHub Actions

Add pre-commit as a CI step so that PRs are validated even if a developer skipped local hooks:

```yaml
# .github/workflows/lint.yml
name: Lint & Format

on:
  pull_request:
    branches: [main, develop]

jobs:
  pre-commit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Install pre-commit
        run: pip install pre-commit
      - name: Run pre-commit
        run: pre-commit run --all-files
```

### Azure DevOps

```yaml
# azure-pipelines.yml
trigger:
  branches:
    include:
      - main
      - develop

pr:
  branches:
    include:
      - main

jobs:
  - job: PreCommit
    pool:
      vmImage: 'ubuntu-latest'
    steps:
      - task: UsePythonVersion@0
        inputs:
          versionSpec: '3.11'
      - script: pip install pre-commit
        displayName: 'Install pre-commit'
      - script: pre-commit run --all-files
        displayName: 'Run pre-commit hooks'
      - script: |
          git diff --exit-code
        displayName: 'Check for uncommitted changes from hooks'
```

**Tip:** Cache the pre-commit environments in CI to speed up runs:

```yaml
# GitHub Actions cache example
- uses: actions/cache@v4
  with:
    path: ~/.cache/pre-commit
    key: pre-commit-${{ runner.os }}-${{ hashFiles('.pre-commit-config.yaml') }}
```

---

## Bypassing Hooks

### Running Against All Files

```bash
# Run all hooks against every file in the repo (not just staged)
pre-commit run --all-files

# Run a specific hook against all files
pre-commit run sqlfluff-lint --all-files
```

Use this to:
- Validate the entire repo after initial setup
- Run hooks in CI
- Check the state after a large merge

### Skipping Hooks with `--no-verify`

```bash
git commit --no-verify -m "WIP: experimental pipeline"
```

**When it's acceptable:**
- Work-in-progress (WIP) commits on a local feature branch
- Emergency hotfixes where you will follow up immediately

**When it is NOT acceptable:**
- Commits to `main` or `develop`
- PR merges
- Any commit that will trigger a production deployment
- When you haven't run the hooks locally first

> ⚠️ **Using `--no-verify` shifts the burden to CI.** If CI fails on formatting or secrets, your PR is blocked anyway. It is almost always faster to let the hooks run locally.

---

## Updating Hooks

Hook repositories release new versions regularly. Update all hooks to their latest versions:

```bash
pre-commit autoupdate
```

This updates the `rev:` field in `.pre-commit-config.yaml` to the latest tag for each repo. After updating:

1. Run `pre-commit run --all-files` to verify nothing breaks
2. Commit the updated `.pre-commit-config.yaml`
3. Open a PR for review

**Best practice:** Run `pre-commit autoupdate` monthly or as part of a dependency update cadence.

---

## Troubleshooting

### Hook is slow

- **Cause:** Hooks run on all files instead of only changed files.
- **Fix:** Ensure `pre-commit install` was run. Pre-commit by default only checks staged files. If running `--all-files`, expect slower runs.
- **Tip:** Use `--files` to limit scope: `pre-commit run --files path/to/file.sql`

### `sqlfluff-fix` modified my files but I didn't stage the changes

- **Cause:** `sqlfluff-fix` modifies files in place. If the fix applies to unstaged content, the staged version isn't updated.
- **Fix:** Run `git add` on the fixed files and re-commit. Or run `sqlfluff fix` manually before staging.

### detect-secrets flags a false positive

- **Cause:** A string looks like a secret pattern.
- **Fix:** Audit the baseline:
  ```bash
  detect-secrets audit .secrets.baseline
  ```
  Mark the finding as `false_positive`. Commit the updated baseline.

### Black and isort conflict

- **Cause:** isort sorts imports in a way Black then reformats.
- **Fix:** Configure isort with the Black profile:
  ```yaml
  args: [--profile, black]
  ```
  Or in `pyproject.toml`:
  ```toml
  [tool.isort]
  profile = "black"
  ```

### `check-yaml` fails on Helm/Jinja templates

- **Cause:** The file contains templating syntax that isn't valid YAML.
- **Fix:** Exclude the file or use `--unsafe`:
  ```yaml
  - id: check-yaml
    args: [--unsafe]
    exclude: ^helm/
  ```

### Hooks don't run on commit

- **Cause:** `pre-commit install` was not run, or the hook was removed.
- **Fix:**
  ```bash
  pre-commit install
  # Verify
  cat .git/hooks/pre-commit
  ```

### `pre-commit` not found

- **Cause:** The package isn't installed in the active virtual environment.
- **Fix:**
  ```bash
  pip install pre-commit
  # Or with uv
  uv pip install pre-commit
  ```

### Python version mismatch

- **Cause:** A hook requires a specific Python version not available.
- **Fix:** Specify the version in the config:
  ```yaml
  default_language_version:
    python: python3.11
  ```

---

## Quick Reference

| Command | Description |
|---|---|
| `pre-commit install` | Install git hooks |
| `pre-commit run` | Run hooks on staged files |
| `pre-commit run --all-files` | Run hooks on all files |
| `pre-commit run <hook-id>` | Run a specific hook |
| `pre-commit autoupdate` | Update hook versions |
| `pre-commit clean` | Clear cached hook environments |
| `pre-commit gc` | Remove unused cached repos |
| `git commit --no-verify` | Skip hooks (use sparingly) |
