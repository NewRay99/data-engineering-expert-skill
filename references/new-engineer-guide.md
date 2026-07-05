# New Engineer Onboarding Guide

Welcome to the data engineering team! This guide walks you through everything you need to get set up and productive. Work through it step by step — each section builds on the previous one.

---

## Prerequisites

Install the following tools on your machine before you begin:

| Tool | Version | Install |
|---|---|---|
| **Python** | 3.11+ | [python.org](https://www.python.org/downloads/) or `pyenv install 3.11` |
| **Git** | 2.40+ | [git-scm.com](https://git-scm.com/downloads) |
| **Databricks CLI** | 0.210+ (v2) | `pip install databricks-cli` or [download](https://docs.databricks.com/dev-tools/cli/index.html) |
| **Azure CLI** | 2.50+ | [docs.microsoft.com](https://docs.microsoft.com/cli/azure/install-azure-cli) |
| **pre-commit** | latest | `pip install pre-commit` |
| **VS Code** (recommended) | latest | [code.visualstudio.com](https://code.visualstudio.com/) |

### Recommended VS Code Extensions

- Python (Microsoft)
- SQL (Microsoft)
- Databricks (Databricks)
- GitLens
- YAML
- Jupyter
- Black Formatter
- SQLFluff

---

## Step-by-Step Onboarding

### Step 1: Clone the Repository

```bash
# Clone the main data platform repository
git clone git@github.com:your-org/data-platform.git
cd data-platform

# Set up your git identity if not already done
git config --global user.name "Your Name"
git config --global user.email "your.name@company.com"
```

**Verify:** `git remote -v` should show `origin` pointing to the organization repo.

### Step 2: Set Up Python Virtual Environment

```bash
# Create a virtual environment
python -m venv venv

# Activate it
# On Windows (Git Bash):
source venv/Scripts/activate
# On macOS/Linux:
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install project dependencies
pip install -r requirements.txt

# Install development dependencies
pip install -r requirements-dev.txt
```

**Verify:** `python --version` should show 3.11+ and `pip list` should include `dbt-core`, `pytest`, etc.

> 💡 **Tip:** If you use `uv`, you can speed this up: `uv venv && uv pip install -r requirements.txt`

### Step 3: Install Pre-commit Hooks

```bash
# Install the pre-commit package (if not already installed)
pip install pre-commit

# Install the git hooks
pre-commit install

# Run hooks against all files to verify your environment
pre-commit run --all-files
```

**Verify:** `cat .git/hooks/pre-commit` should contain a pre-commit launcher script.

See `references/pre-commit-hooks.md` for detailed hook documentation.

### Step 4: Get Azure Access

You need Azure access to interact with Data Lake Storage, Key Vault, and other Azure resources.

1. **Request AAD group membership:**
   - Ask your team lead to add you to the `data-engineering-team` Azure AD group
   - This group has Reader access to the data landing zone and Contributor access to the dev resource group

2. **Service Principal (for automation):**
   - If you need to run pipelines locally that connect to Azure services, request a service principal
   - Store credentials in Azure Key Vault — never in code or `.env` files committed to the repo

3. **Key Vault Access:**
   - Request access to `kv-dataeng-dev` for development environment secrets
   - Use the Azure CLI to verify access:
     ```bash
     az keyvault secret show --vault-name kv-dataeng-dev --name databricks-token
     ```

4. **Storage Account Access:**
   - Your AAD group should grant access to `stdatalake_dev` (dev container)
   - Verify with Azure Storage Explorer or:
     ```bash
     az storage blob list --account-name stdatalake_dev --container-name raw --auth-mode login
     ```

### Step 5: Get Databricks Access

1. **Workspace Access:**
   - Request access to the `data-engineering-dev` Databricks workspace
   - Your team lead will add you via the Databricks admin console

2. **Unity Catalog:**
   - You'll be granted access to the `dev` catalog
   - Schemas: `dev.raw`, `dev.bronze`, `dev.silver`, `dev.gold`
   - Verify access in the Databricks UI under **Data Explorer**

3. **Cluster Policy:**
   - A personal cluster policy (`personal-dev`) is assigned to you
   - This limits your cluster size and auto-terminates after 30 minutes of inactivity
   - Create a compute resource in the Databricks UI using this policy

4. **PAT Token:**
   - Generate a Personal Access Token in Databricks: **Settings → User Settings → Access Tokens → Generate New Token**
   - You'll use this for CLI authentication in the next step

### Step 6: Configure Databricks CLI

```bash
# Configure the Databricks CLI with your workspace URL and PAT
databricks configure --token

# You'll be prompted for:
# Databricks Host: https://adb-xxxxxxxx.x.azuredatabricks.net
# Token: dapiXXXXXXXXXXXXXXXX

# Verify the connection
databricks clusters list
databricks catalogs list
```

**For Databricks CLI v2 (recommended):**

```bash
# Using profile-based configuration
databricks configure --profile dev --token

# Use the profile
databricks clusters list --profile dev
```

**Verify:** `databricks clusters list` should return at least one cluster.

### Step 7: Configure Azure CLI

```bash
# Log in interactively (opens browser)
az login

# Set the default subscription
az account set --subscription "your-subscription-id"

# Verify
az account show
```

**Verify:** `az account show` should display your subscription details.

> 💡 For service principal login (CI/CD or automation): `az login --service-principal -u <app-id> -p <secret> --tenant <tenant-id>`

### Step 8: Run the Test Suite

```bash
# Run the full test suite
pytest tests/ -v

# Run only unit tests
pytest tests/unit/ -v

# Run with coverage
pytest tests/ -v --cov=src --cov-report=term-missing

# Run dbt tests (if applicable)
dbt test --profiles-dir profiles/
```

**All tests must pass before you open a pull request.** If tests fail on a fresh clone, contact your team lead — it may indicate an environment or dependency issue.

### Step 9: Understand the Medallion Architecture

Read **`references/medallion-architecture.md`** to understand our data layering strategy:

- **Bronze** — Raw ingestion, append-only, schema-on-read
- **Silver** — Cleansed, conformed, deduplicated
- **Gold** — Business-level aggregates, curated for analytics

Key concepts to understand:
- How data flows from source → bronze → silver → gold
- Naming conventions for tables and files
- Idempotency expectations for each layer
- How Unity Catalog governs access across layers

### Step 10: Pick a Good-First-Issue Ticket

1. Go to the project board (Jira / GitHub Projects)
2. Filter by label: `good-first-issue`
3. Pick a ticket that matches your interest area (SQL, Python, infra, etc.)
4. Assign it to yourself
5. Comment on the ticket so the team knows you're working on it
6. If you get stuck, ask in the team Slack channel — no question is too small

---

## First Commit Workflow

Follow this workflow for every change you make:

```
1. Create a branch
   git checkout -b feature/your-ticket-description

2. Write code
   - Follow the Medallion architecture conventions
   - Add/modify tests alongside your code

3. Run tests locally
   pytest tests/ -v

4. Run pre-commit hooks
   pre-commit run --all-files
   (This also runs automatically on git commit)

5. Commit your changes
   git add .
   git commit -m "feat: add silver layer transformation for customer_orders"

6. Push and open a Pull Request
   git push -u origin feature/your-ticket-description
   # Open a PR in GitHub/Azure DevOps targeting the develop branch

7. Request review
   - Assign at least one reviewer
   - Link the Jira ticket in the PR description
   - Use the PR template

8. Address review feedback
   - Push additional commits to the same branch
   - Re-request review after changes

9. Merge
   - Squash-and-merge once approved and CI is green
   - Delete your feature branch after merge
```

### Commit Message Convention

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>: <description>

[optional body]
[optional footer]
```

| Type | Use for |
|---|---|
| `feat` | New feature or transformation |
| `fix` | Bug fix |
| `refactor` | Code restructuring without behavior change |
| `test` | Adding or updating tests |
| `docs` | Documentation changes |
| `chore` | Maintenance, dependency updates |
| `ci` | CI/CD pipeline changes |

Example: `fix: handle null customer_id in silver_customers transformation`

---

## Code Review Expectations

### As an Author

- **Self-review first:** Review your own diff before requesting review. Catch obvious issues yourself.
- **Small PRs:** Keep PRs under 400 lines of diff when possible. Split large changes into multiple PRs.
- **Write a clear description:** What changed, why, and how to test it.
- **Link the ticket:** Always reference the Jira/GitHub issue.
- **Tests are mandatory:** Every PR that changes code must include or update tests.
- **Respond to all comments:** Either address the feedback or explain why you disagree. Never ignore a comment.
- **Don't take feedback personally:** Reviews improve code quality, not your worth as an engineer.

### As a Reviewer

- **Review within 24 hours:** Don't block your teammates.
- **Be specific:** "This is wrong" is not helpful. "This will fail if `customer_id` is null because ..." is helpful.
- **Distinguish blocking vs. non-blocking:** Use "blocking:" and "nit:" prefixes.
- **Praise good code:** If someone wrote something clever or clean, say so.
- **Ask questions:** "Why did you choose this approach?" is valid and educational.
- **Don't nitpick formatting:** That's what pre-commit is for. Only comment on formatting if the hooks missed something.
- **Test edge cases mentally:** What happens with nulls, empty arrays, duplicate keys, concurrent runs?

---

## Common Mistakes New Engineers Make

### 1. Committing Secrets

**Mistake:** Hardcoding connection strings, API keys, or passwords in code.

**Fix:** Use Azure Key Vault or environment variables. pre-commit's `detect-secrets` hook will catch most cases, but don't rely on it alone. If you accidentally commit a secret, notify the team immediately — it must be rotated.

### 2. Not Running Tests Before Pushing

**Mistake:** Pushing code that fails tests, then waiting for CI to tell you.

**Fix:** `pytest tests/ -v` takes seconds locally. Run it every time before pushing.

### 3. Large, Unfocused PRs

**Mistake:** Combining multiple features or refactors into one giant PR.

**Fix:** One PR = one logical change. If you're tempted to say "and also..." in your PR description, split it.

### 4. Skipping the Medallion Architecture

**Mistake:** Writing transformations that skip layers (e.g., reading directly from raw in a gold-layer model).

**Fix:** Always follow bronze → silver → gold. Each layer has a purpose. If you think you need to skip a layer, raise it in a design discussion first.

### 5. Not Understanding Idempotency

**Mistake:** Writing transformations that produce different results when re-run on the same data.

**Fix:** Every transformation must be idempotent. Use MERGE instead of INSERT. Use window functions for deduplication. Test by running your pipeline twice and comparing outputs.

### 6. Ignoring Data Types

**Mistake:** Letting Spark infer schemas, leading to silent type mismatches.

**Fix:** Always define explicit schemas. Use `schema` parameter in `spark.read`, and define column types in dbt models using `data_tests` and `columns.yml`.

### 7. Not Using Branch Protection

**Mistake:** Committing directly to `develop` or `main`.

**Fix:** Always work on a feature branch. Branch protection rules prevent direct commits — if you can commit to `main`, something is wrong. Report it.

### 8. Leaving Debug Code In

**Mistake:** Committing `print()` statements, `display(df)`, or commented-out code blocks.

**Fix:** Use a proper logging framework (`logging` module). Pre-commit hooks don't catch this — your reviewer will.

### 9. Not Documenting Why

**Mistake:** Code that explains *what* it does but not *why*.

**Fix:** Comments should explain decisions, not mechanics. "Filter to active customers because the downstream model expects only current accounts" is useful. "Filter the dataframe" is not.

### 10. Not Asking for Help Early

**Mistake:** Spinning on a problem for hours before asking.

**Fix:** The team norm is: if you're stuck for more than 30 minutes, ask in Slack. Someone has likely solved the same problem.

---

## Resources

### Internal Documentation

- **Medallion Architecture** — `references/medallion-architecture.md`
- **Pre-commit Hooks** — `references/pre-commit-hooks.md`
- **CI/CD Pipeline** — `references/cicd-pipeline.md` (if available)
- **Coding Standards** — `references/coding-standards.md` (if available)
- **Data Quality Framework** — `references/data-quality.md` (if available)
- **Confluence Space** — Search for "Data Engineering" in Confluence
- **Architecture Diagrams** — `docs/architecture/`

### Databricks Learning

- [Databricks Academy](https://www.databricks.com/learn/training) — Free courses on Spark, Delta Lake, MLflow
- [Databricks SQL Guide](https://docs.databricks.com/sql/index.html)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Unity Catalog Guide](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Databricks CLI Reference](https://docs.databricks.com/dev-tools/cli/index.html)

### Azure Learning

- [Azure Data Lake Storage Gen2](https://docs.microsoft.com/azure/storage/blobs/data-lake-storage-introduction)
- [Azure Key Vault](https://docs.microsoft.com/azure/key-vault/general/overview)
- [Azure CLI Documentation](https://docs.microsoft.com/cli/azure/)
- [Azure Data Factory](https://docs.microsoft.com/azure/data-factory/) (if applicable)
- [Microsoft Learn — Data Engineering Path](https://docs.microsoft.com/learn/paths/data-engineer-azure/)

### General Engineering

- [Conventional Commits](https://www.conventionalcommits.org/)
- [Pre-commit Documentation](https://pre-commit.com/)
- [SQLFluff Documentation](https://docs.sqlfluff.com/)
- [dbt Best Practices](https://docs.getdbt.com/best-practices)
- [The Twelve-Factor App](https://12factor.net/)

---

## Onboarding Checklist

Work through this checklist to track your progress. Don't skip items — each one is important.

### Environment Setup

- [ ] Python 3.11+ installed and verified (`python --version`)
- [ ] Git installed and configured (`git config --global user.name`, `user.email`)
- [ ] Databricks CLI installed (`databricks --version`)
- [ ] Azure CLI installed (`az --version`)
- [ ] pre-commit installed (`pip install pre-commit`)

### Repository Setup

- [ ] Repository cloned successfully
- [ ] Virtual environment created and activated
- [ ] `pip install -r requirements.txt` completed
- [ ] `pip install -r requirements-dev.txt` completed
- [ ] `pre-commit install` completed
- [ ] `pre-commit run --all-files` passes

### Access Provisioned

- [ ] Added to `data-engineering-team` Azure AD group
- [ ] Databricks workspace access confirmed
- [ ] Unity Catalog `dev` catalog access confirmed
- [ ] Azure Key Vault `kv-dataeng-dev` access confirmed
- [ ] Service principal created (if needed for local pipeline runs)
- [ ] Personal cluster policy assigned

### CLI Configuration

- [ ] `databricks configure --token` completed and verified (`databricks clusters list`)
- [ ] `az login` completed and verified (`az account show`)
- [ ] Default subscription set (`az account set`)

### Verification

- [ ] `pytest tests/ -v` passes on fresh clone
- [ ] `dbt test --profiles-dir profiles/` passes (if applicable)
- [ ] Can query `dev.bronze.*` tables in Databricks
- [ ] Can list blobs in `stdatalake_dev` container

### Knowledge Building

- [ ] Read `references/medallion-architecture.md`
- [ ] Read `references/pre-commit-hooks.md`
- [ ] Understand the branch → PR → review → merge workflow
- [ ] Picked a `good-first-issue` ticket from the board
- [ ] Introduced yourself in the team Slack channel

### First Contribution

- [ ] Created a feature branch
- [ ] Wrote code with tests
- [ ] Ran `pytest tests/ -v` locally
- [ ] Ran `pre-commit run --all-files`
- [ ] Opened a pull request
- [ ] Received code review feedback
- [ ] Addressed feedback and merged
- [ ] Deleted the feature branch after merge

---

Welcome aboard! 🚀 You're going to do great. Remember: when in doubt, ask — the team is here to help.
