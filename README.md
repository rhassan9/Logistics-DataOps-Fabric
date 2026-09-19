# Logistics DataOps on Microsoft Fabric

A medallion-architecture data platform on Microsoft Fabric, ingesting a logistics operations database from PostgreSQL and modelling it into a dimensional warehouse.

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
              │  notebooks · Dataflow Gen2
              ▼
   ┌──────────────────────────────┐
   │  SILVER   lh_logistics_silver│   cleaned, conformed, deduplicated
   └──────────────────────────────┘
              │  T-SQL stored procedures
              ▼
   ┌──────────────────────────────┐
   │  GOLD     wh_logistics_gold  │   Kimball star schema
   └──────────────────────────────┘
              │
              ▼
   DirectLake semantic model → Power BI
```

**Bronze** preserves source data exactly as it arrives, append-only, with row-level lineage. Implemented twice, as a Spark notebook and as a Fabric Copy job, and compared.
See [docs/bronze.md](docs/Bronze.md).

**Silver** cleans, conforms and deduplicates into one validated row per business entity,
with MERGE-based SCD Type 1 loads and change data feed enabled for gold. Transactional
tables are built with a Spark notebook, reference tables with Dataflow Gen2.
See [docs/silver.md](docs/Silver.md).

**Gold** is a Kimball star schema built with T-SQL stored procedures and served through a DirectLake semantic model. Planned.

---

## Source data

A logistics operations database covering fleet, drivers, shipments and safety across three years, from the
[Logistics Operations Database](https://www.kaggle.com/datasets/yogape/logistics-operations-database) on Kaggle (MIT licence).

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
| Transformation | PySpark, spaCy, Dataflow Gen2 |
| Warehouse | Fabric Data Warehouse, T-SQL |
| Semantic layer | DirectLake, DAX |
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

---

## Setting up the source

The source database is not part of this repository. To reproduce it:

1. Download the dataset from
   [Kaggle](https://www.kaggle.com/datasets/yogape/logistics-operations-database)
   and extract the CSV files.
2. Create a PostgreSQL database. Any instance works; this project uses
   [Neon](https://neon.tech) on its free tier.
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

The two pre-aggregated tables in the Kaggle export are not loaded. They are OLAP artifacts rather than OLTP entities, and equivalents are rebuilt in the gold layer from the transactional tables.

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
| M3 Gold star schema and semantic model | In Progress |
| M4 Deployment pipelines and CI/CD | Planned |
| M5 Eventhouse and KQL | Planned |
| M6 dbt vs Fabric comparison write-up | Planned |
