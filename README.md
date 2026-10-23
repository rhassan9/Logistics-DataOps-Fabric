# Logistics DataOps on Microsoft Fabric

A medallion-architecture data platform on Microsoft Fabric: a PostgreSQL operations database ingested into a lakehouse, refined, and modelled into a Kimball star schema that reconciles exactly against an independent answer key.

This is a migration of [Logistics-DataOps-Pipeline](https://github.com/rhassan9/Logistics-DataOps-Pipeline), a dbt and PostgreSQL warehouse, onto Fabric. Both versions are maintained so the two approaches can be compared directly.

## At a glance

- **541,930 source rows, 12 tables, three years** of fleet, shipment and safety data, landed append-only in bronze and refined to one row per business key in silver.
- **6 facts, 8 dimensions, 4 junk dimensions** in a Fabric Data Warehouse, loaded incrementally from silver's Delta change feed by T-SQL procedures.
- **Exact reconciliation against the dataset publisher's own monthly figures:** ten metric comparisons, four on all 4,464 driver-months and six on all 3,312 truck-months, match exactly and fail the run on any mismatch.
- **Deletes handled end to end without losing history:** soft deletes from silver through every dimension and fact, behind a mass-deletion guard.
- **A Direct Lake semantic model over the full star**: 18 tables, 34 relationships and 55 measures covering all nine requirements, with calculation groups for alternate date roles, every figure validated against warehouse SQL and the model checked with Best Practice Analyzer. A two-page report answers on-time delivery by lane and contribution after fuel by route.
- **One scheduled pipeline for the whole gold load**, every step safe to retry, failures posted to Slack, and recovery after a mid-run failure proven by breaking the real pipeline on purpose.
- **Eleven defects caught before release** by reconciliation, independent rebuilds, failure testing and review, each fixed and listed [below](#defects-caught-before-release).

The source is a static, synthetic Kaggle dataset. The incremental, deletion and Type 2 paths are exercised by controlled changes made in silver and gold during testing, not by live source activity.

---

## Architecture

```mermaid
flowchart TD
    SRC[("Neon PostgreSQL<br/>12 tables · 541,930 rows")]
    BRZ["<b>Bronze</b> · lh_logistics_bronze<br/>raw Delta, append-only"]
    SLV["<b>Silver</b> · lh_logistics_silver<br/>one row per key, current state,<br/>source deletions flagged"]
    CD["Change detection<br/>Spark notebook reads<br/>silver's change feed"]
    GLD["<b>Gold</b> · wh_logistics_gold<br/>Kimball star schema,<br/>T-SQL stored procedures"]
    REC["Reconciliation against<br/>the publisher's baselines"]
    PL["<b>Orchestration</b><br/>pl_logistics_daily → pl_gold_load<br/>schedule · retries · Slack alerts"]
    SM["<b>Semantic model</b> · sm_logistics_gold<br/>Direct Lake on OneLake<br/>→ rpt_lane_performance"]
    OPS[("lh_logistics_ops<br/>profiling, baselines, run logs")]

    SRC -->|"Spark notebook (JDBC) · Copy job"| BRZ
    BRZ -->|"Spark notebook · Dataflow Gen2"| SLV
    SLV --> CD
    CD -->|"changed business keys"| GLD
    SLV -->|"SQL analytics endpoint"| GLD
    GLD --> REC
    OPS -.-> REC
    GLD --> SM
    PL -.->|"runs"| CD
    PL -.->|"runs"| GLD
```

| Layer | What it does | Detail |
|---|---|---|
| **Bronze** | Preserves source data exactly as it arrives, append-only, with row-level lineage. Built twice, as a Spark notebook and as a Copy job, and compared. | [docs/Bronze.md](docs/Bronze.md) |
| **Silver** | One validated row for every business key the source has ever contained, carrying its current state. A key that disappears from the source is kept and flagged, and every row records when its state took effect. Those two columns are a contract with gold that does not depend on how changes are captured. | [docs/Silver.md](docs/Silver.md) |
| **Gold** | Kimball star schema in a Fabric Data Warehouse. A Spark notebook reads silver's change feed and hands each fact exactly the keys it must rebuild; every fact load is gated, transactional and logged, and the model is reconciled on every run. | [docs/Gold.md](docs/Gold.md) |
| **Orchestration** | `pl_gold_load` runs endpoint sync, dimensions, change detection, the six facts in parallel, a completeness check, reconciliation and cleanup under one run_id. `pl_logistics_daily` schedules it and posts failures to Slack. | [docs/Gold.md](docs/Gold.md#orchestration) |
| **Semantic model** | Direct Lake on OneLake over the whole star, with role-playing dates, calculation groups, named and described for people and AI tools, and a report for DP-1 and FE-1. | [docs/Gold.md](docs/Gold.md#semantic-model) |
| **Requirements** | What gold must answer, and the questions this data cannot honestly support. | [docs/Business-Requirements.md](docs/Business-Requirements.md) |

`lh_logistics_ops` sits outside the medallion flow. It holds profiling output, the validation baselines used to reconcile gold, and the bronze and silver run logs.

---

## Engineering highlights

**Incremental loading into a warehouse from a lakehouse change feed.** T-SQL cannot read a Delta change feed, so a notebook reads each silver table's feed from the version gold last consumed, derives the affected keys for each fact (a changed load reaches its trips, events and fuel purchases), and stages them in the warehouse. Each fact's checkpoint advances only after its procedure commits, and a full reconciliation takes over whenever the change window cannot be trusted.

**Deletes handled end to end without losing history.** Silver flags deletions instead of removing rows; gold carries the flag onto facts and dimensions as a soft delete. Every load stops if more than 10 percent of active rows would be deleted at once, the signature of a broken extract rather than a business event.

**Loads that fail safely.** Each fact procedure checks that change detection covered every table it depends on, asserts one row per business key, publishes its rows and consumes its worklist in one transaction, and logs success only after commit. Detection refuses a run_id a fact has already used, and the pipeline first checks that the warehouse sees the same silver data as Spark.

**Recovery proven, not assumed.** A fact that fails after change detection keeps its worklist and its checkpoint, while the other facts commit. The acceptance tests broke a fact procedure inside the real pipeline and recovered it both ways: rerunning that one procedure, and starting a fresh run that re-reads what the failed run never consumed. A completeness check fails any run that skips a step, added after a copied pipeline activity loaded one fact twice and another not at all while the run reported success.

**Correctness checked against an independent answer key.** Gold rebuilds the publisher's monthly driver and truck aggregates from its facts. Ten metric comparisons match exactly, four on all 4,464 driver-months and six on all 3,312 truck-months, including revenue attributed to the resource that delivered each load. A mismatch on any of them fails the run. Metrics that do not match are reported rather than tuned until they do.

**A semantic model validated like the warehouse.** Every measure was compared with an independent warehouse query before any column was renamed: totals, all 58 lanes, the date-role switch month by month and active trucks under filters. Best Practice Analyzer then reviewed the model, and each finding was judged against the rule's code: formats were fixed, and the rest were kept with a stated reason, including one rule whose condition contradicts its own description.

**Findings stated, not hidden.** Building the report exposed a property of the synthetic source: fuel purchases do not follow trip distance (correlation 0.0000 with fuel cost, against 0.98 for fuel actually used on the trip), so the shortest lanes show negative contribution after fuel. The page states this rather than replacing the source's figure with an estimate.

**Tested through the real pipeline.** Every fact was exercised with first loads, idle runs, genuine value changes, deletions, restores from bronze and forced full rebuilds, each checked in both directions against silver. The orchestration was then tested by failure: a cancelled detection, a broken fact, a broken dimension step and overlapping runs.

### Defects caught before release

Each was caught by reconciliation against a known number, by an independent rebuild, by a failed run or in review, and each is fixed:

| Layer | Defect |
|---|---|
| Bronze | An exclusive watermark, Copy job's comparison, drops every row sharing the boundary date: about 78 rows per incremental run on this source. The notebook compares inclusively |
| Silver | `arrival_status` misclassified boundary events twice: an inclusive threshold, then classification on a rounded, second-truncated variance (59 events) |
| Silver | Reading the earliest bronze version per key would have pinned silver to stale values after the first source update; the reference queries would have duplicated keys on the second bronze batch |
| Silver | Dataflow Gen2 cannot write to Delta tables created by Fabric Runtime 2.0 defaults; reference tables are created at a protocol both writers support |
| Silver | The Dataflow dated a key that returned to the source by its first appearance, where the notebook dates it by its return |
| Gold | A change-detection run repeated under an already successful run_id advanced the checkpoint without the changes being loaded |
| Gold | A trip or event moving to another load left the old load's lifecycle row stale |
| Gold | Rebuilding a dimension left facts pointing at surrogate keys that no longer existed |
| Gold | A load with no route was reported as a broken reference rather than a missing value |
| Gold | Taking the highest consumed version as the checkpoint would skip changes after a silver table is recreated; the checkpoint is the most recent successful window |
| Orchestration | A copied pipeline activity kept its procedure name, so one fact loaded twice and another never loaded while the run reported success; a completeness check now fails any run missing a step |

---

## Semantic model and report

![Semantic model star schema](docs/images/semantic_model_star.png)

![DP-1 On-time delivery by lane](docs/images/dp1_on_time_by_lane.png)

![FE-1 Contribution after fuel by route](docs/images/fe1_contribution_after_fuel.png)

Design, measures, validation and the Best Practice Analyzer review are in [docs/Gold.md](docs/Gold.md#semantic-model).

---

## What changes at scale

The dataset is about 125 MB. Most of the machinery here exists for correctness rather than volume: deletes, reprocessing after failure, idempotent reruns and reconciliation matter at any size. What would change with volume:

- **Partitioning.** Microsoft advises leaving tables under 1 TB unpartitioned, so none are. Large fact and bronze tables past that size would be partitioned on their load date.
- **Ingestion parallelism.** JDBC read partitioning is configured and benchmarked, and shows no gain at this size; it starts to pay once transfer time dominates connection setup.
- **Change capture.** Snapshot-based deletion detection reads every bronze batch on a full run. At volume, change data capture at the source replaces it, which the silver contract allows without touching gold.
- **Gold engine.** T-SQL procedures suit a warehouse serving Direct Lake at this size. Heavier transformation would be evaluated on Spark or materialized lake views against the same contract.
- **Reconciliation.** The publisher's baseline is a fixed answer key that holds only while the source is static. With a changing source, the run's gate becomes a silver-to-gold reconciliation computed on both sides in every run, and the baseline becomes an on-demand check.

---

## Source data

A logistics operations database covering fleet, drivers, shipments and safety across three years, from the [Logistics Operations Database](https://www.kaggle.com/datasets/yogape/logistics-operations-database) on Kaggle (MIT licence).

| Table | Rows |
|---|---:|
| fuel_purchases | 196,442 |
| delivery_events | 170,820 |
| loads | 85,410 |
| trips | 85,410 |
| maintenance_records | 2,920 |
| customers | 200 |
| trailers | 180 |
| safety_incidents | 170 |
| drivers | 150 |
| trucks | 120 |
| routes | 58 |
| facilities | 50 |

The source is a PostgreSQL database with declared primary keys, foreign keys and indexes on all foreign key columns. It is treated as a production system outside this project's control: no schema changes were made to it to simplify ingestion.

To reproduce it, see [Setting up the source](#setting-up-the-source).

---

## Stack

| Layer | Technology |
|---|---|
| Source | Neon PostgreSQL 18 |
| Ingestion | Fabric Spark notebooks (PySpark, JDBC), Fabric Copy job |
| Storage | OneLake, Delta Lake |
| Transformation | PySpark on Fabric Runtime 2.0, Dataflow Gen2, T-SQL stored procedures |
| Change detection | Delta change data feed, Spark connector for Fabric Data Warehouse, Semantic Link |
| Warehouse | Fabric Data Warehouse, T-SQL |
| Orchestration | Fabric Data Factory pipelines, Slack incoming webhook through a Fabric Web connection |
| Semantic layer | Direct Lake on OneLake, DAX, calculation groups, Power BI report, Best Practice Analyzer (semantic-link-labs) |
| Configuration | Fabric Variable Library; Azure Key Vault wired in code, provisioned with the Azure subscription in M4 |
| Source control | Fabric Git integration, GitHub |

---

## Architecture decisions

### Single workspace, one lakehouse per layer

Microsoft recommends creating each medallion lakehouse in its own workspace for control and governance at the layer level. This project deliberately uses a single workspace holding all three layers.

That recommendation is driven by regulatory requirements and separation of duties, neither of which applies to a single-developer project. Splitting would mean three Git connections and three Variable Libraries for no governance benefit. Microsoft's deployment-pattern guidance lists a single workspace with a lakehouse per layer as a valid choice where there is no need for organisational separation.

The split that will matter is by stage rather than by layer. Deployment pipelines require separate workspaces, so dev, test and prod will each hold all three layers.

### Bronze and silver as lakehouses, gold as a warehouse

Microsoft describes two medallion deployment patterns: all layers as lakehouses, or bronze and silver as lakehouses with gold as a data warehouse. This project uses the second, so gold is built with T-SQL stored procedures and consumed through the warehouse endpoint. That exercises both the Lakehouse and Warehouse engines rather than only one.

### Change detection in Spark, modelling in T-SQL

The warehouse reads silver through the SQL analytics endpoint, which exposes the current snapshot only. Timestamp watermarks were rejected because they cannot see deletions; Copy job's change data capture from a lakehouse table was rejected because it is in preview, copies whole rows, and cannot derive keys that change indirectly, such as every trip of a changed load. A notebook therefore reads the change feed and passes keys, not data, to the warehouse; every modelling rule stays in T-SQL.

### Orchestration in Fabric Data Factory, gold first

Every orchestrated item is a Fabric item, so the pipelines are Fabric Data Factory rather than a separate Azure Data Factory: native notebook and Warehouse procedure activities, one workspace, one Git sync and one monitoring surface. The pipeline starts at silver for now. Bronze and silver are rebuilt in M4 on a source with change data capture, and orchestrating today's versions would mean building that part twice; the parent pipeline gains them as a step ahead of gold.

### Direct Lake on OneLake over the full star

The semantic model reads gold's Delta tables directly from OneLake, so it never falls back to DirectQuery, and it carries every fact and dimension rather than only what the report uses: Direct Lake loads a column only when a query needs it, and the model then answers all nine requirements. Date roles are inactive relationships switched by calculation groups, so no measure is copied per date.

### Soft deletes, never physical deletes

Nothing downstream of bronze is physically deleted. Silver flags source deletions, gold carries the flag onto dimensions and facts, and reports filter on it. History and the keys that facts reference survive, and a key that returns at the source is restored rather than recreated.

### A stable contract between silver and gold

Fabric's change-capture paths do not support this source today, so silver derives deletions and change dates from complete bronze snapshots. The two contract columns carry the same meaning whatever produces them, so replacing the source with one that supports change data capture changes bronze and silver only.

---

## Setting up the source

The source database is not part of this repository. To reproduce it:

1. Download the dataset from [Kaggle](https://www.kaggle.com/datasets/yogape/logistics-operations-database) and extract the CSV files.
2. Create a PostgreSQL database. Any instance works; this project uses [Neon](https://neon.tech) on its free tier.
3. Create the schema:

   ```bash
   export NEON_URL="postgresql://user:password@host/dbname?sslmode=require"
   psql "$NEON_URL" -f sql/01_create_schema.sql
   ```

4. Load the data, running from the folder containing the CSV files:

   ```bash
   psql "$NEON_URL" -f sql/02_load_data.sql
   ```

The load script copies tables in foreign key dependency order and prints row counts on completion.

The two pre-aggregated tables in the Kaggle export are not loaded into the source database. They are OLAP artifacts rather than OLTP entities. They are loaded separately into the operations lakehouse as a validation baseline, and gold's aggregation of the transactional facts is reconciled against them.

---

## Repository structure

```
├── fabric/     Fabric item definitions (managed by Git integration)
├── sql/        Source database schema and load scripts
├── tests/      Test helpers that run outside the deployed workspace
└── docs/       Per-layer design notes and comparisons
```

---

## Status

| Milestone | Status |
|---|---|
| M1 Bronze ingestion | Complete |
| M2 Silver transformation | Complete |
| M3 Gold star schema and semantic model | Complete |
| M4 Deployment pipelines and CI/CD | Planned |
| M5 Eventhouse and KQL | Planned |
| M6 dbt vs Fabric comparison write-up | Planned |