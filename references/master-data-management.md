# Master Data Management (MDM) Reference

## Overview

Master Data Management (MDM) is the discipline of creating, maintaining, and governing a single, authoritative version of key business entities across an organization. Master data — customers, products, employees, locations, suppliers — is shared across multiple systems and must be consistent, accurate, and trustworthy. This reference covers MDM architecture, data matching and merging strategies, golden record creation, governance workflows, and implementation patterns with SQL and Python.

---

## 1. Master Data Domains

### 1.1 Common Master Data Entities

| Domain | Key Entities | Typical Source Systems |
|--------|-------------|----------------------|
| Customer | Customer, Contact, Account | CRM, ERP, E-commerce |
| Product | Product, SKU, Category | PIM, ERP, E-commerce |
| Employee | Employee, Department, Position | HRIS, Active Directory |
| Location | Address, Store, Warehouse | ERP, Facilities |
| Supplier | Vendor, Contract | Procurement, ERP |
| Finance | Account, Cost Center | GL, ERP |

### 1.2 Master Data vs. Transactional Data

- **Master data** changes slowly, is referenced by transactional data, and is shared across processes.
- **Transactional data** is high-volume, time-stamped, and references master data.
- **Reference data** is standardized code/lookup data (e.g., ISO country codes, currency codes).

```sql
-- Example: Master data relationship
SELECT
    c.customer_id AS master_customer_id,    -- Master data
    c.customer_name,                         -- Master data
    p.product_id AS master_product_id,       -- Master data
    p.product_name,                          -- Master data
    o.order_id,                              -- Transactional data
    o.order_date,                            -- Transactional data
    o.quantity                               -- Transactional data
FROM orders o                               -- Transactional table
JOIN dim_customer c ON o.customer_id = c.customer_id  -- Master data
JOIN dim_product p ON o.product_id = p.product_id;    -- Master data
```

---

## 2. MDM Architecture Patterns

### 2.1 Registry Model

In the registry model, master data remains in source systems. The MDM hub maintains cross-references and a golden record view without owning the data.

```
+----------+     +----------+     +----------+
|  Source A |     |  Source B |     |  Source C |
+----+-----+     +----+-----+     +----+-----+
     |                |                |
     +-------+--------+-------+--------+
             |                |
     +-------v------+  +------v-------+
     | Registry Hub |  | Cross-Ref Map|
     | (Golden Rec) |  | (Source IDs) |
     +--------------+  +--------------+
```

**Pros:** Minimal disruption to source systems; lightweight.
**Cons:** No single source of truth for writes; real-time consistency is harder.

### 2.2 Hub-and-Spoke (Coexistence) Model

The MDM hub owns the golden record. Source systems continue to operate but sync with the hub.

```
+----------+          +----------+          +----------+
|  Source A |<--sync-->|  MDM Hub  |<--sync-->|  Source B |
+----------+          | (Golden   |          +----------+
                      |  Record)  |
+----------+          |  Owner    |          +----------+
|  Source C |<--sync-->|           |<--sync-->|  Source D |
+----------+          +----------+          +----------+
```

**Pros:** Golden record is authoritative; bidirectional sync.
**Cons:** Requires more infrastructure; conflict resolution needed.

### 2.3 Consolidated (Transactional) Model

The MDM hub is the system of record. All writes go through the hub; source systems become read-only consumers.

**Pros:** Strongest data consistency; clean architecture.
**Cons:** High migration effort; requires source system changes.

---

## 3. Data Matching Strategies

### 3.1 Deterministic Matching

Deterministic matching uses exact key matching (e.g., national ID, email, tax ID) to identify the same entity across systems.

```sql
-- Deterministic match on email
SELECT
    a.source_system AS source_a,
    a.customer_id AS id_a,
    a.email,
    b.source_system AS source_b,
    b.customer_id AS id_b
FROM crm_customers a
JOIN erp_customers b ON LOWER(TRIM(a.email)) = LOWER(TRIM(b.email));
```

### 3.2 Probabilistic Matching (Fellegi-Sunter Model)

Probabilistic matching assigns weights to field comparisons and computes a match score. Fields with high discriminating power (e.g., SSN) get higher weights.

```python
from dataclasses import dataclass
from typing import Dict

@dataclass
class ProbabilisticMatcher:
    """Fellegi-Sunter probabilistic record linkage."""

    field_weights: Dict[str, Dict[str, float]]
    # e.g., {"email": {"m_prob": 0.9, "u_prob": 0.01}}

    def compare_field(self, val_a: str, val_b: str, field: str) -> float:
        """Return log-likelihood ratio for a single field comparison."""
        if field not in self.field_weights:
            return 0.0
        weights = self.field_weights[field]
        m_prob = weights["m_prob"]  # P(match | same entity)
        u_prob = weights["u_prob"]  # P(match | different entity)

        if val_a and val_b and val_a.lower().strip() == val_b.lower().strip():
            return self._log2(m_prob / u_prob)
        else:
            return self._log2((1 - m_prob) / (1 - u_prob))

    def match_score(self, record_a: dict, record_b: dict) -> float:
        """Compute aggregate match score."""
        score = 0.0
        for field in self.field_weights:
            score += self.compare_field(
                str(record_a.get(field, "")),
                str(record_b.get(field, "")),
                field
            )
        return score

    @staticmethod
    def _log2(x: float) -> float:
        import math
        return math.log2(x) if x > 0 else -999.0

# Configuration
matcher = ProbabilisticMatcher(
    field_weights={
        "email":      {"m_prob": 0.95, "u_prob": 0.001},   # High discriminating power
        "phone":      {"m_prob": 0.80, "u_prob": 0.01},
        "last_name":  {"m_prob": 0.90, "u_prob": 0.05},
        "first_name": {"m_prob": 0.85, "u_prob": 0.10},
        "zip_code":   {"m_prob": 0.80, "u_prob": 0.05},
    }
)

record_a = {"email": "john.doe@email.com", "phone": "5551234567",
            "last_name": "Doe", "first_name": "John", "zip_code": "10001"}
record_b = {"email": "john.doe@email.com", "phone": "5551234567",
            "last_name": "Doe", "first_name": "John", "zip_code": "10001"}

score = matcher.match_score(record_a, record_b)
# Threshold: > 5 = definite match, 0-5 = possible match, < 0 = non-match
print(f"Match score: {score:.2f}")
```

### 3.3 Fuzzy Matching with Blocking

Blocking pre-filters candidate pairs to avoid O(n²) comparisons. Records are grouped by a blocking key (e.g., first 3 chars of last name + zip code).

```python
import pandas as pd
from rapidfuzz import fuzz
from collections import defaultdict

def blocked_fuzzy_match(df: pd.DataFrame, blocking_keys: list[str],
                        match_fields: list[str], threshold: int = 85) -> pd.DataFrame:
    """
    Match records using blocking + fuzzy comparison.
    """
    # Step 1: Create blocks
    blocks = defaultdict(list)
    for idx, row in df.iterrows():
        block_key = "|".join(str(row.get(k, ""))[:3].upper() for k in blocking_keys)
        blocks[block_key].append(idx)

    # Step 2: Compare within blocks
    matches = []
    for block_key, indices in blocks.items():
        if len(indices) < 2:
            continue
        for i in range(len(indices)):
            for j in range(i + 1, len(indices)):
                idx_a, idx_b = indices[i], indices[j]
                row_a, row_b = df.loc[idx_a], df.loc[idx_b]

                # Compute average fuzzy score across match fields
                scores = []
                for field in match_fields:
                    val_a = str(row_a.get(field, ""))
                    val_b = str(row_b.get(field, ""))
                    if val_a and val_b:
                        scores.append(fuzz.token_sort_ratio(val_a.lower(), val_b.lower()))

                avg_score = sum(scores) / len(scores) if scores else 0
                if avg_score >= threshold:
                    matches.append({
                        "index_a": idx_a,
                        "index_b": idx_b,
                        "block_key": block_key,
                        "similarity": round(avg_score, 2)
                    })

    return pd.DataFrame(matches)

# Usage
matches = blocked_fuzzy_match(
    df,
    blocking_keys=["last_name", "zip_code"],
    match_fields=["first_name", "last_name", "address"],
    threshold=85
)
```

---

## 4. Golden Record Creation

The golden record is the authoritative, best-quality version of a master data entity, merged from multiple source systems.

### 4.1 Survivorship Rules

Survivorship rules determine which source system's value "wins" for each attribute when sources disagree.

**Common survivorship strategies:**
- **Source precedence**: System A > System B > System C (ranked trust)
- **Most recent**: Use the most recently updated value
- **Most complete**: Prefer the longest non-null value
- **Most frequent**: Use the value that appears most often across sources
- **Manual override**: Steward-curated value takes precedence

### 4.2 SQL — Golden Record with Survivorship

```sql
WITH ranked_sources AS (
    SELECT
        c.match_id,
        c.source_system,
        c.customer_id,
        c.customer_name,
        c.email,
        c.phone,
        c.address,
        c.updated_at,
        -- Source precedence ranking
        ROW_NUMBER() OVER (
            PARTITION BY c.match_id
            ORDER BY
                CASE c.source_system
                    WHEN 'CRM' THEN 1
                    WHEN 'ERP' THEN 2
                    WHEN 'E_COMMERCE' THEN 3
                    ELSE 99
                END
        ) AS source_rank,
        -- Most recent ranking
        ROW_NUMBER() OVER (
            PARTITION BY c.match_id, c.email
            ORDER BY c.updated_at DESC NULLS LAST
        ) AS email_recency_rank
    FROM customer_matches c
),
-- Pick email by most recent, name by source precedence
golden_name AS (
    SELECT match_id, customer_name, source_system
    FROM ranked_sources
    WHERE source_rank = 1 AND customer_name IS NOT NULL
),
golden_email AS (
    SELECT match_id, email, source_system
    FROM ranked_sources
    WHERE email_recency_rank = 1 AND email IS NOT NULL
),
golden_phone AS (
    SELECT match_id, phone,
        COUNT(*) OVER (PARTITION BY match_id, phone) AS frequency
    FROM ranked_sources
    WHERE phone IS NOT NULL
    QUALITY ROW_NUMBER() OVER (
        PARTITION BY match_id ORDER BY frequency DESC, updated_at DESC
    ) = 1
)
SELECT
    COALESCE(gn.match_id, ge.match_id, gp.match_id) AS golden_customer_id,
    gn.customer_name,
    gn.source_system AS name_source,
    ge.email,
    ge.source_system AS email_source,
    gp.phone
FROM golden_name gn
FULL OUTER JOIN golden_email ge ON gn.match_id = ge.match_id
FULL OUTER JOIN golden_phone gp ON COALESCE(gn.match_id, ge.match_id) = gp.match_id;
```

### 4.3 Python — Golden Record Builder

```python
from dataclasses import dataclass, field
from typing import Any, Optional
from enum import Enum
from datetime import datetime

class SurvivorshipStrategy(Enum):
    SOURCE_PRECEDENCE = "source_precedence"
    MOST_RECENT = "most_recent"
    MOST_COMPLETE = "most_complete"
    MOST_FREQUENT = "most_frequent"

@dataclass
class SourceRecord:
    source_system: str
    record: dict
    updated_at: datetime

@dataclass
class GoldenRecordBuilder:
    """Build a golden record from matched source records using per-attribute survivorship."""

    attribute_strategies: dict[str, SurvivorshipStrategy]
    source_precedence: dict[str, int] = field(default_factory=dict)

    def build(self, match_id: str, sources: list[SourceRecord]) -> dict:
        golden = {"match_id": match_id}
        for attr, strategy in self.attribute_strategies.items():
            golden[attr] = self._select_value(attr, strategy, sources)
        golden["_sources"] = [s.source_system for s in sources]
        golden["_golden_timestamp"] = datetime.now().isoformat()
        return golden

    def _select_value(self, attr: str, strategy: SurvivorshipStrategy,
                      sources: list[SourceRecord]) -> Optional[Any]:
        candidates = [(s, s.record.get(attr)) for s in sources if s.record.get(attr) is not None]
        if not candidates:
            return None

        if strategy == SurvivorshipStrategy.SOURCE_PRECEDENCE:
            return min(candidates, key=lambda x: self.source_precedence.get(x[0].source_system, 99))[1]

        elif strategy == SurvivorshipStrategy.MOST_RECENT:
            return max(candidates, key=lambda x: x[0].updated_at)[1]

        elif strategy == SurvivorshipStrategy.MOST_COMPLETE:
            return max(candidates, key=lambda x: len(str(x[1])))[1]

        elif strategy == SurvivorshipStrategy.MOST_FREQUENT:
            from collections import Counter
            freq = Counter(str(c[1]) for c in candidates)
            most_common = freq.most_common(1)[0][0]
            return next(c[1] for c in candidates if str(c[1]) == most_common)

        return None

# Usage
builder = GoldenRecordBuilder(
    attribute_strategies={
        "customer_name": SurvivorshipStrategy.SOURCE_PRECEDENCE,
        "email": SurvivorshipStrategy.MOST_RECENT,
        "address": SurvivorshipStrategy.MOST_COMPLETE,
        "phone": SurvivorshipStrategy.MOST_FREQUENT,
    },
    source_precedence={"CRM": 1, "ERP": 2, "E_COMMERCE": 3}
)

sources = [
    SourceRecord("CRM", {"customer_name": "Acme Corp", "email": "old@acme.com",
                         "address": "123 Main St", "phone": "555-1000"},
                 datetime(2024, 1, 15)),
    SourceRecord("ERP", {"customer_name": "Acme Corporation", "email": "new@acme.com",
                         "address": "123 Main Street, Suite 100", "phone": "555-1000"},
                 datetime(2024, 3, 20)),
    SourceRecord("E_COMMERCE", {"customer_name": "ACME", "email": "new@acme.com",
                                "address": "123 Main St", "phone": "555-2000"},
                 datetime(2024, 2, 10)),
]

golden = builder.build("CUST_00001", sources)
# Result: name from CRM (precedence), email from ERP (most recent),
#         address from ERP (most complete), phone "555-1000" (most frequent)
```

---

## 5. MDM Data Model

### 5.1 Cross-Reference Table Pattern

```sql
-- Master record table
CREATE TABLE md_customer (
    golden_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_name TEXT NOT NULL,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    country_code TEXT,
    status TEXT DEFAULT 'active',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    created_by TEXT DEFAULT 'system',
    updated_by TEXT DEFAULT 'system'
);

-- Cross-reference mapping table
CREATE TABLE md_customer_xref (
    golden_id UUID REFERENCES md_customer(golden_id),
    source_system TEXT NOT NULL,
    source_customer_id TEXT NOT NULL,
    confidence_score NUMERIC(5,2),  -- Match confidence (0-100)
    match_method TEXT,               -- 'deterministic', 'probabilistic', 'manual'
    matched_at TIMESTAMP DEFAULT NOW(),
    matched_by TEXT DEFAULT 'system',
    PRIMARY KEY (source_system, source_customer_id)
);

-- Audit/history table (SCD Type 2)
CREATE TABLE md_customer_history (
    history_id BIGSERIAL PRIMARY KEY,
    golden_id UUID NOT NULL,
    customer_name TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    change_reason TEXT,
    changed_by TEXT
);
```

### 5.2 Trigger for Audit Trail

```sql
CREATE OR REPLACE FUNCTION audit_customer_change()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'UPDATE') THEN
        INSERT INTO md_customer_history (
            golden_id, customer_name, email, phone, address,
            valid_from, valid_to, change_reason, changed_by
        )
        SELECT
            OLD.golden_id, OLD.customer_name, OLD.email, OLD.phone, OLD.address,
            OLD.updated_at, NEW.updated_at,
            CASE
                WHEN OLD.customer_name <> NEW.customer_name THEN 'name_change'
                WHEN OLD.email <> NEW.email THEN 'email_change'
                ELSE 'other_update'
            END,
            NEW.updated_by;
        NEW.updated_at = NOW();
        RETURN NEW;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customer_audit
    BEFORE UPDATE ON md_customer
    FOR EACH ROW
    EXECUTE FUNCTION audit_customer_change();
```

---

## 6. Governance and Stewardship

### 6.1 Data Steward Workflow

Data stewards review and resolve ambiguous matches, override survivorship rules, and approve golden record changes.

```python
from enum import Enum
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

class ReviewStatus(Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    ESCALATED = "escalated"

@dataclass
class ReviewTask:
    task_id: str
    match_id: str
    source_records: list[dict]
    proposed_golden: dict
    status: ReviewStatus = ReviewStatus.PENDING
    steward: Optional[str] = None
    resolution: Optional[dict] = None
    created_at: datetime = None
    resolved_at: Optional[datetime] = None
    notes: Optional[str] = None

class StewardshipQueue:
    """Manages data steward review tasks for MDM."""

    def __init__(self):
        self.tasks: dict[str, ReviewTask] = {}

    def create_task(self, match_id: str, source_records: list[dict],
                    proposed_golden: dict) -> ReviewTask:
        task_id = f"TASK_{len(self.tasks) + 1:06d}"
        task = ReviewTask(
            task_id=task_id,
            match_id=match_id,
            source_records=source_records,
            proposed_golden=proposed_golden,
            created_at=datetime.now()
        )
        self.tasks[task_id] = task
        return task

    def resolve_task(self, task_id: str, steward: str,
                     status: ReviewStatus, resolution: dict, notes: str = ""):
        task = self.tasks[task_id]
        task.status = status
        task.steward = steward
        task.resolution = resolution
        task.resolved_at = datetime.now()
        task.notes = notes

    def pending_tasks(self) -> list[ReviewTask]:
        return [t for t in self.tasks.values() if t.status == ReviewStatus.PENDING]

# Usage
queue = StewardshipQueue()
task = queue.create_task(
    match_id="CUST_00042",
    source_records=[
        {"source": "CRM", "name": "John Smith", "email": "j.smith@email.com"},
        {"source": "ERP", "name": "Jon Smith", "email": "john.smith@email.com"},
    ],
    proposed_golden={"name": "John Smith", "email": "john.smith@email.com"}
)

# Steward reviews and resolves
queue.resolve_task(
    task_id=task.task_id,
    steward="alice@company.com",
    status=ReviewStatus.APPROVED,
    resolution={"name": "John Smith", "email": "john.smith@email.com"},
    notes="Confirmed via LinkedIn lookup. ERP email is current."
)
```

---

## 7. Cross-System Synchronization

### 7.1 Change Data Capture (CDC) Integration

```python
import json
from datetime import datetime
from typing import Callable

class MDMSyncProcessor:
    """Process CDC events and sync changes to/from the MDM hub."""

    def __init__(self, golden_record_repo, xref_repo, steward_queue):
        self.golden_repo = golden_record_repo
        self.xref_repo = xref_repo
        self.steward_queue = steward_queue
        self.conflict_resolvers: dict[str, Callable] = {}

    def register_resolver(self, source_system: str, resolver: Callable):
        self.conflict_resolvers[source_system] = resolver

    def process_cdc_event(self, event: dict):
        """Handle a CDC event from a source system."""
        source_system = event["source_system"]
        source_id = event["source_id"]
        operation = event["operation"]
        data = event["data"]

        # Find existing golden record via cross-reference
        golden_id = self.xref_repo.find(source_system, source_id)

        if operation == "DELETE":
            if golden_id:
                self.golden_repo.deactivate(golden_id)
            return

        if golden_id is None:
            # New record — attempt matching
            match = self._attempt_match(data)
            if match:
                # Found potential match — create review task
                self.steward_queue.create_task(
                    match_id=match["match_id"],
                    source_records=match["sources"],
                    proposed_golden=match["proposed"]
                )
            else:
                # No match — create new golden record
                golden_id = self.golden_repo.create(data)
                self.xref_repo.link(golden_id, source_system, source_id)
        else:
            # Update existing golden record
            existing = self.golden_repo.get(golden_id)
            resolver = self.conflict_resolvers.get(source_system)
            if resolver:
                merged = resolver(existing, data)
                self.golden_repo.update(golden_id, merged)

    def _attempt_match(self, data: dict) -> dict | None:
        """Try to find matching golden records."""
        # Implementation would use deterministic and probabilistic matching
        return None
```

---

## 8. MDM Metrics and KPIs

| Metric | Description | Target |
|--------|-------------|--------|
| Match Rate | % of source records matched to a golden record | >90% |
| Match Accuracy | % of matches that are correct (sampled) | >98% |
| Golden Record Completeness | % of required attributes populated | >95% |
| Unresolved Tasks | Steward review tasks pending > 7 days | <5% |
| Source Coverage | % of source systems integrated into MDM | 100% |
| Sync Latency | Time from source change to golden record update | <15 min |
| Duplicate Rate | % of golden records with unresolved duplicates | <2% |

---

## 9. Best Practices Summary

1. **Start with one domain**: Begin with a single master data domain (usually Customer or Product) before expanding.
2. **Match before merge**: Always validate match quality before merging records. False merges are costly to undo.
3. **Preserve provenance**: Every golden record attribute should trace back to its source system and timestamp.
4. **Human-in-the-loop**: Automate high-confidence matches; route ambiguous cases to stewards.
5. **Version everything**: Use SCD Type 2 or event sourcing to track golden record changes over time.
6. **Define ownership**: Each master data domain should have a named data owner and steward team.
7. **Monitor match drift**: Re-run matching periodically — new source records may reveal previously hidden duplicates.
8. **Plan for unmerges**: Provide a mechanism to split incorrectly merged records. This will happen.
9. **Integrate with data governance**: MDM is a subset of data governance. Align policies, glossaries, and quality rules.
10. **Think globally**: Consider international data formats, privacy regulations (GDPR, CCPA), and multi-language support from the start.
