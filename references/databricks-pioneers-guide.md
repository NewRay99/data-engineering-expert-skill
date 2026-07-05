# Databricks Pioneers & Advanced DataOps Reference Guide

This resource guide compiles the profiles, core focus areas, and technical findings of leading pioneers in the Databricks ecosystem, specifically focusing on **Databricks Asset Bundles (DABs)**, **CI/CD automation**, **Delta Live Tables (DLT)**, and **Software Engineering data standards**.

Use this as a learning resource for your team. Follow these pioneers to stay current with evolving best practices.

---

## 1. Directory of Pioneers

| Pioneer | Primary Focus | LinkedIn | GitHub / Blog |
| :--- | :--- | :--- | :--- |
| **Hubert Dudek** *(Databricks MVP)* | Docker Runtimes, Custom Master Data Dimension Engine, Delta Optimizations | [LinkedIn](https://www.linkedin.com/in/hubertdudek) | [Medium Blog](https://databrickster.medium.com/) · [GitHub](https://github.com/hubert-dukek/medium) |
| **Alessandro Armillotta** | Zero-Trust CI/CD pipelines, OIDC Authentication, Validate/Plan workflows | [LinkedIn](https://www.linkedin.com/in/alessandro-armillotta) | [GitHub](https://github.com/alessandro9110) |
| **Maciej Tarsa** | Declarative Configurations, Multi-User Development Isolation, Modular Architecture | [LinkedIn](https://www.linkedin.com/in/maciejtarsa/) | [GitHub](https://github.com/maciejtarsa) |
| **Simon Doy** | Infrastructure as Code (IaC) Bridging, Workflow Determinism, Unity Catalog Alignment | [LinkedIn](https://www.linkedin.com/in/simondoy) | [GitHub](https://github.com/SimonDoy) |

---

## 2. Technical Findings & Architectural Breakdown

### 💡 Hubert Dudek: Immutable Environments & Master Data Optimizations

- **Containerized Consistency:** Champions utilizing Databricks Docker container runtimes via CI/CD to eliminate parity issues between local pytest suites and Databricks runtime. This means the exact same Docker image runs locally and in the cloud — no more "works on my cluster" problems.
- **Deterministic Dimensions:** Focuses heavily on structuring high-column-count customer master dimensions using optimized Delta MERGE statements and clean Unity Catalog tracking. His patterns for MDM survivorship using Delta MERGE are directly applicable to the MDM module in this skill (see `references/master-data-management.md`).
- **Delta Optimizations:** Advocates for ZORDER BY on high-cardinality columns, liquid clustering for multi-column predicates, and predictive I/O for query acceleration.

**Key Takeaway:** Use Docker-based Databricks runtimes in CI/CD to guarantee environment parity. Test locally with the same runtime image that runs in production.

### 🔐 Alessandro Armillotta: Zero-Trust CI/CD & Security Gates

- **OIDC Authentication:** Strongly advocates for OpenID Connect (OIDC) identity federation over long-lived personal access tokens (PATs) inside enterprise CI/CD pipelines. OIDC tokens are short-lived, scoped, and eliminate the security risk of rotating PATs in CI secrets.
- **Pre-Deployment Planning:** Normalizes using `databricks bundle validate` and `databricks bundle plan` as automated PR check gates to visually confirm DLT pipeline structural adjustments before deployment. This prevents accidental pipeline destruction in production.
- **Validate/Plan Workflow:** His CI/CD pattern:
  1. On PR open: `databricks bundle validate` (syntax + schema check)
  2. On PR review: `databricks bundle plan` (show what will change)
  3. On merge to main: `databricks bundle deploy` (execute deployment)
  4. Post-deploy: automated smoke test against the deployed pipeline

**Key Takeaway:** Never use long-lived PATs in CI/CD. Use OIDC federation. Always run `validate` and `plan` as PR gates before any deployment.

### ⚙️ Maciej Tarsa: Declarative Configurations & Modular Architecture

- **Decoupled PySpark Logic:** Promotes building core processing layers using standalone, configuration-driven Python code engines (`.py` files), completely decoupled from Databricks notebooks. Notebooks become thin wrappers that import and call the `.py` modules. This enables:
  - Local unit testing with pytest without Databricks
  - Type checking with mypy
  - Code reusability across pipelines
  - Cleaner imports and dependency management
- **Isolation Paradigms:** Highlights the usage of the native `mode: development` flag inside DABs to programmatically segment workspace targets per developer, preventing staging/production contamination. Each developer gets their own workspace target:
  ```yaml
  targets:
    dev_alice:
      mode: development
      default_cluster: { node_type_id: "i3.xlarge", num_workers: 1 }
    dev_bob:
      mode: development
      default_cluster: { node_type_id: "i3.xlarge", num_workers: 1 }
    staging:
      mode: staging
      default_cluster: { node_type_id: "i3.2xlarge", num_workers: 4 }
    prod:
      mode: production
      default_cluster: { node_type_id: "i3.4xlarge", num_workers: 8 }
  ```
- **Modular Architecture:** His repo structure:
  ```
  project/
  ├── src/                    # Pure Python, no Databricks dependency
  │   ├── transforms/         # Transformation functions
  │   ├── dq/                 # Data quality checks
  │   └── utils/              # Shared utilities
  ├── tests/                  # pytest + chispa
  ├── notebooks/              # Thin wrappers importing from src/
  ├── databricks.yml          # DAB configuration
  └── requirements.txt
  ```

**Key Takeaway:** Decouple PySpark logic into importable `.py` files. Use DAB `mode: development` for per-developer workspace isolation. Test transformations locally without Databricks.

### 🌐 Simon Doy: IaC Integration & Workflow Determinism

- **IaC Integration:** Bridges the semantic gap between baseline cloud provider definitions (Terraform/Bicep) and the specialized configuration states required for Databricks Asset Bundles. His pattern:
  - Terraform manages: VNet, subnets, storage accounts, Key Vaults, Databricks workspace
  - DABs manage: notebooks, pipelines, jobs, cluster policies, Unity Catalog objects
  - The boundary: Terraform outputs workspace URL and storage account ID → DABs consume them as variables
- **Workflow Traps:** Prevents runtime task misalignment by strictly packaging both Delta Live Table graph code and upstream/downstream Orchestration Workflows inside the identical git tag bundle deployment. This ensures:
  - DLT pipeline and its triggering Workflow are always version-aligned
  - No partial deployments where a Workflow references a DLT pipeline that doesn't exist yet
  - Rollback restores both the pipeline AND the workflow together
- **Unity Catalog Alignment:** Ensures DAB deployments include Unity Catalog grants and schema creation as part of the bundle, not as manual post-deployment steps.

**Key Takeaway:** Use Terraform for infrastructure, DABs for Databricks assets. Always package DLT pipelines and their workflows in the same bundle deployment. Include Unity Catalog grants in the bundle.

---

## 3. High-Maturity Data Engineering Standards Checklist

Adapted from the combined practices of all four pioneers:

- [ ] **Local Check:** Enforce linting and fast type assertions via Ruff and Mypy inside git pre-commit configurations
- [ ] **Unit Testing:** Isolate core data transformations and Master Data survivorship rules into plain Python files (`src/`) to be tested using pytest + [chispa](https://github.com/MrPowers/chispa) (PySpark DataFrame testing library)
- [ ] **CI/CD Pipeline:** Execute code validations locally first, build artifact validations at the PR boundary, and target environments dynamically via variables inside the `databricks.yml` asset configuration
- [ ] **OIDC Authentication:** Replace all PAT-based CI/CD auth with OIDC identity federation (GitHub Actions → Azure → Databricks)
- [ ] **Environment Parity:** Use Databricks Docker container runtimes in CI/CD to match production runtime exactly
- [ ] **DAB Validate/Plan Gates:** Run `databricks bundle validate` on PR open and `databricks bundle plan` as a required PR check before merge
- [ ] **Decoupled Logic:** Notebooks are thin wrappers; all business logic lives in importable `.py` files under `src/`
- [ ] **Per-Developer Isolation:** Each developer has a dedicated DAB target with `mode: development`
- [ ] **Bundle Versioning:** DLT pipelines and their triggering Workflows are deployed together in the same git-tagged bundle
- [ ] **Unity Catalog in Bundles:** Schema creation and GRANT statements are part of the DAB deployment, not manual steps
- [ ] **Infrastructure Boundary:** Terraform/Bicep manages cloud infra; DABs manage Databricks assets; the two don't overlap

---

## 4. How to Use This Guide

### For New Engineers
1. Read each pioneer's section to understand the "why" behind their patterns
2. Follow their LinkedIn and GitHub profiles for ongoing updates
3. Apply the high-maturity checklist to your current project — identify gaps
4. Read the corresponding reference docs in this skill:
   - Hubert's MDM patterns → `references/master-data-management.md`
   - Alessandro's CI/CD patterns → `references/git-hygiene-standards.md`
   - Maciej's modular architecture → `references/databricks-transformation.md`
   - Simon's IaC patterns → `references/official-documentation-links.md`

### For Team Leads
1. Use the high-maturity checklist as a team audit tool — score each item 0-2
2. Prioritize adoption based on lowest-scoring areas
3. Assign team members to spike specific pioneer patterns
4. Review monthly as pioneers publish new findings

### For Feature Adoption
1. When a pioneer publishes a new pattern, trigger the feature evaluation process (see SKILL.md → "Adopting New Features")
2. Create a `spike/` branch, build a proof-of-concept
3. System test before merging to `main`

---

## 5. Pioneer Content Monitoring

Check these sources monthly for new patterns and updates:

- **Hubert Dudek:** https://databrickster.medium.com/ (Medium blog)
- **Alessandro Armillotta:** https://github.com/alessandro9110 (GitHub repos and READMEs)
- **Maciej Tarsa:** https://github.com/maciejtarsa (GitHub repos and blog posts)
- **Simon Doy:** https://github.com/SimonDoy (GitHub repos and releases)
- **Databricks Blog:** https://www.databricks.com/blog (official announcements)
- **Databricks Community:** https://community.databricks.com/ (forums and solutions)

---

## 6. Recommended Libraries & Tools

Based on pioneer practices:

| Tool | Purpose | Used By | Link |
| :--- | :--- | :--- | :--- |
| [chispa](https://github.com/MrPowers/chispa) | PySpark DataFrame unit testing | Hubert, Maciej | GitHub |
| [Ruff](https://github.com/astral-sh/ruff) | Fast Python linter + formatter | Maciej | GitHub |
| [Mypy](https://mypy.readthedocs.io/) | Static type checking for Python | Maciej | Docs |
| [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) | Bundle validate/plan/deploy | Alessandro, Simon | Docs |
| [Terraform](https://www.terraform.io/) | IaC for cloud infrastructure | Simon | Docs |
| [GitHub Actions OIDC](https://docs.github.com/actions/deployment/security-hardening-your-deployments/about-security-hardening-with-openid-connect) | Keyless CI/CD auth | Alessandro | Docs |
| [Docker](https://www.docker.com/) | Container runtimes for parity | Hubert | Docs |
| [pre-commit](https://pre-commit.com/) | Git hook management | All | Docs |

---

*This guide is a living document. Update it as pioneers publish new findings and as the team adopts new patterns. See `references/official-documentation-links.md` for the full documentation link registry.*
