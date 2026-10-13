# Logistics DataOps on Microsoft Fabric

A medallion-architecture data platform on Microsoft Fabric, ingesting a logistics operations database from PostgreSQL and modelling it into a dimensional warehouse that reconciles exactly against an independent answer key.

This is a migration of [Logistics-DataOps-Pipeline](https://github.com/rhassan9/Logistics-DataOps-Pipeline), a dbt and PostgreSQL warehouse, onto Fabric. Both versions are maintained so the two approaches can be compared directly.

---

## Architecture

```
Neon PostgreSQL (OLTP source)
   12 tables · 541,930 rows · 2022-2024
              │
              │  Spark notebook (JDBC)  ·  Copy job
              ▼
   ┌──────────────────────────────┐
   │  BRONZE   lh_logistics_bronze│   raw Delta, append-only
   └──────────────────────────────┘
              │  Spark notebook · Dataflow Gen2
              ▼
   ┌──────────────────────────────┐
   │  SILVER   lh_logistics_silver│   one row per key, current state,
   └──────────────────────────────┘   source deletions flagged
              │  change detection (Spark, silver change feed)
              │  T-SQL stored procedures
              ▼
   ┌──────────────────────────────┐
   │  GOLD     wh_logistics_gold  │   Kimball star schema
   └──────────────────────────────┘
              │
              ▼
   Direct Lake semantic model → Power BI
```

`lh_logistics_ops` sits outside the medallion flow. It holds profiling output, the validation baselines used to reconcile gold, and the bronze and silver run logs.

**Bronze** preserves source data exactly as it arrives, append-only, with row-level lineage. Implemented twice, as a Spark notebook and as a Fabric Copy job, and compared. See [docs/Bronze.md](docs/Bronze.md).

**Silver** holds one validated row for every business key the source has ever contained, carrying its current state. Rows are never physically deleted: a key that disappears from the source is kept with its last values and flagged, and every row records when its current state took effect at the source. Those two columns form a contract with gold that does not depend on how changes are captured upstream. Transactional tables are built with a Spark notebook, reference tables with Dataflow Gen2. See [docs/Silver.md](docs/Silver.md).

**Gold** is a Kimball star schema in a Fabric Data Warehouse: six facts, eight dimensions and four junk dimensions, built with T-SQL stored procedures and served through a Direct Lake semantic model. A Spark notebook reads silver's change data feed and hands each fact exactly the business keys it must rebuild. Every load is gated, transactional and logged, and the whole model is reconciled each run against monthly aggregates published with the source. The analytical requirements, and the questions the data cannot support, are in [docs/Business Requirements.md](docs/Business%20Requirements.md). The bus matrix, grain statements, loading and reconciliation are in [docs/Gold.md](docs/Gold.md).

---

## Engineering highlights

**Incremental loading into a warehouse from a lakehouse change feed.** T-SQL cannot read a Delta change feed, so a notebook reads each silver table's feed from the version gold last consumed, derives the affected keys for each fact (a changed load reaches its trips, events and fuel purchases), and stages them in the warehouse. Each fact's checkpoint advances only after its procedure commits, and a full reconciliation takes over whenever the change window cannot be trusted.

**Deletes handled end to end without losing history.** Silver flags deletions instead of removing rows; gold carries the flag onto facts and dimensions as a soft delete. Every load stops if more than 10 percent of active rows would be deleted at once, the signature of a broken extract rather than a business event.

**Loads that fail safely.** Each fact procedure checks that change detection covered every table it depends on, asserts one row per business key, publishes its rows and consumes its worklist in one transaction, and logs success only after commit. Detection refuses a reused run_id and first proves that the warehouse sees the same silver data as Spark.

**Correctness proven against an independent answer key.** Gold rebuilds the publisher's monthly driver and truck aggregates from its facts and matches seven metrics exactly on all 4,464 driver-months and 3,312 truck-months, including revenue attributed to the resource that delivered each load. A mismatch on any of them fails the run. Metrics that do not match are recorded as definition-unknown rather than tuned until they do.

**Tested through the real pipeline.** Every fact was exercised with first loads, idle runs, genuine value changes, deletions, restores from bronze and forced full rebuilds, each checked in both directions against silver.

### Defects caught before release

Each was caught by reconciliation against a known number, by an independent rebuild, by a failed run or in review, and each is fixed:

| Layer | Defect |
|---|---|
| Bronze | An exclusive watermark, Copy job's comparison, drops every row sharing the boundary date: about 78 rows per incremental run on this source. The notebook compares inclusively |
| Silver | `arrival_status` misclassified boundary events twice: an inclusive threshold, then classification on a rounded, second-truncated variance (59 events) |
| Silver | Reading the earliest bronze version per key would have pinned silver to stale values after the first source update; the reference queries would have duplicated keys on the second bronze batch |
| Silver | Dataflow Gen2 cannot write to Delta tables created by Fabric Runtime 2.0 defaults; reference tables are created at a protocol both writers support |
| Gold | A change-detection run repeated under an already successful run_id advanced the checkpoint without the changes being loaded |
| Gold | A trip or event moving to another load left the old load's lifecycle row stale |
| Gold | Rebuilding a dimension left facts pointing at surrogate keys that no longer existed |
| Gold | A load with no route was reported as a broken reference rather than a missing value |

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
| Semantic layer | Direct Lake, DAX |
| Configuration | Fabric Variable Library, Azure Key Vault |
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
└── docs/       Per-layer design notes and comparisons
```

---

## Status

| Milestone | Status |
|---|---|
| M1 Bronze ingestion | Complete |
| M2 Silver transformation | Complete |
| M3 Gold star schema and semantic model | In progress: star schema, loading and reconciliation complete; orchestration and semantic model in progress |
| M4 Deployment pipelines and CI/CD | Planned |
| M5 Eventhouse and KQL | Planned |
| M6 dbt vs Fabric comparison write-up | Planned |