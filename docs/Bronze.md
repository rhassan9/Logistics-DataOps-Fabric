# Bronze layer

Bronze preserves source data exactly as it arrives. No cleaning, no renaming, no type
coercion, no deduplication. Four lineage columns are the only additions.

The layer is append-only. Incremental runs add rows and never rewrite or delete existing
ones, which makes bronze the audit record for everything downstream. Corrections belong
in silver.

Bronze is implemented twice: as a Spark notebook and as a Fabric Copy job. Microsoft's
guidance recommends Copy job as the default for raw ingestion. The notebook is the
primary path here for the reasons in [Comparison](#comparison) below.

---

## Notebook implementation

`nb_bronze_ingest_neon` reads all 12 tables over JDBC and writes Delta tables to
`lh_logistics_bronze`.

### Configuration

Three layers, following Microsoft's security and CI/CD guidance:

| Layer | Holds |
|---|---|
| Parameter cell | runtime arguments a pipeline overrides: load mode, ingest date, table filter |
| Variable Library | environment values with dev, test and production value sets |
| Key Vault | the database credential only |

Secrets are never passed as notebook parameters, and no credential appears in the
notebook or the repository. The Variable Library's value sets mean promoting the notebook
between environments requires no code change.

### Lineage columns

| Column | Purpose |
|---|---|
| `_ingest_date` | logical batch date, also the idempotency key for full reloads |
| `_ingested_at` | wall-clock write time |
| `_source_system` | origin identifier |
| `_load_mode` | full or incremental |

### Inclusive watermark

Incremental reads filter with `>=` rather than `>`, so rows sharing the highest watermark
value are never skipped.

On a date column this is not an edge case. 85,410 loads span roughly 1,095 dates, so
about 78 rows sit on any given boundary. An exclusive comparison drops all of them,
silently and permanently.

The re-read produces duplicates. Those are retained here and resolved in silver, because
bronze does not deduplicate.

### Write behaviour

| Situation | Behaviour |
|---|---|
| First run | create the table |
| Full load | `replaceWhere` on `_ingest_date`, replacing only that slice |
| Incremental | append |

`replaceWhere` makes a re-run for the same ingest date idempotent while leaving every
earlier batch untouched. A plain overwrite would destroy bronze history.

### No partitioning

Microsoft advises leaving tables under 1 TB unpartitioned and targeting at least 1 GB per
partition. This dataset is around 125 MB, so partitioning by ingest date would produce
many small files, hurt the SQL analytics endpoint sync and inflate Direct Lake column
segments.

Partitioning primarily exists to isolate concurrent writers. There is one writer.

### No V-Order

V-Order is a write-time Parquet optimisation that improves read performance for Direct
Lake and the SQL endpoint, at the cost of 15 to 33 percent slower writes. Bronze is read
by Spark, which gains nothing from it. V-Order is enabled in gold, where Direct Lake
consumes the data.

---

## Copy job implementation

Two Copy jobs write to `lh_logistics_bronze_copyjob`.

The read method is set per job rather than per table. The six reference tables have no
reliable watermark column, so they run as a full-copy job; the six transactional tables
run as an incremental job. One notebook handles both modes from a single manifest.

Write method is Append, Microsoft's documented default and the only option consistent
with an append-only layer. Merge and SCD Type 2 are available but both perform
corrections, which belong in silver and gold.

---

## Comparison

| | Notebook | Copy job |
|---|---|---|
| Artifacts for 12 tables | 1 | 2 |
| Watermark comparison | inclusive (`>=`) | exclusive (`>`) |
| Row lineage | 4 custom columns | audit columns, configured per table |
| Read parallelism | configurable | auto-partitioning unsupported for PostgreSQL |
| `fuel_purchases` load time | ~5s | ~60s |
| Code required | ~250 lines | none |
| Credential handling | Key Vault, in code | managed Fabric connection |

Copy job's exclusive watermark drops rows sharing the boundary value. Microsoft's
documented workaround is to switch the write method to Merge, which performs a correction
inside a layer whose purpose is to record what arrived unchanged.

Copy job's audit columns capture more than the notebook's hand-written equivalents,
notably the incremental window's lower and upper bounds. That allows verifying after the
fact that every slice was processed. The notebook has no equivalent.

Timings are not strictly like for like: Copy job provisions compute per run, while the
notebook reuses a warm Spark session.

### Where each fits

Copy job is the better choice when ingestion is straightforward, the source is supported,
and low-code maintainability matters more than control. It needs no code, no credential
handling, and comes with retry and monitoring built in.

The notebook wins here on correctness at the watermark boundary, on handling mixed load
modes in one artifact, and on being reviewable in a diff. Those outweighed the low-code
advantage for this source.

---

## Read partitioning benchmark

JDBC read partitioning splits a read across parallel connections to the source. It was
measured on the largest table, `fuel_purchases` (196,442 rows, roughly 40 MB):

| Partitions | Time |
|---:|---:|
| 0 | 5.5s |
| 4 | 5.8s |
| 8 | 5.0s |

No measurable benefit at this volume. Connection setup, TLS negotiation and query
planning across several connections offset the transfer saving.

The configuration is retained in the notebook to demonstrate the technique, with the
measurements recorded inline so the decision is auditable. Parallel reads become
worthwhile once transfer time dominates connection setup, well above this dataset's size.

`fetchsize` is a different matter and does earn its place. The JDBC default of 10 rows
per round trip turns a six-figure table into tens of thousands of network calls to the
source region.

---

## Table maintenance

`OPTIMIZE` runs after a batch load to compact the small files that parallel writes
produce. `VACUUM` is held until several batches have accumulated; the default seven-day
retention is a floor, since shorter windows break time travel and can corrupt concurrent
readers.