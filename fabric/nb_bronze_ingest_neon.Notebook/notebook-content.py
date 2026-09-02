# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "344f3ccc-7a2a-43a7-b62d-572ac6168344",
# META       "default_lakehouse_name": "lh_logistics_bronze",
# META       "default_lakehouse_workspace_id": "a77071e4-bd2a-4979-8910-91ddb8cd2a09",
# META       "known_lakehouses": [
# META         {
# META           "id": "344f3ccc-7a2a-43a7-b62d-572ac6168344"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Bronze Ingestion — Neon PostgreSQL to Fabric Lakehouse
# 
# - Source        : Neon PostgreSQL (logistics OLTP), eu-central-1
# - Destination   : lh_logistics_bronze
# - Project       : Logistics-DataOps-Fabric
# 
#  ---------------------------------------------------------------------------
# ### WHAT THIS LAYER DOES
#  ---------------------------------------------------------------------------
#  Bronze records what arrived, exactly as it arrived. No cleaning, no renaming,
#  no type coercion, no deduplication. Four ingestion metadata columns are the
#  only additions. Every correction belongs downstream in silver.
# 
#  The layer is append-only and immutable. An incremental run adds rows; it
#  never rewrites or deletes existing ones. That makes bronze the audit record:
#  any downstream figure can be traced back to the batch it came from.
# 
#  ---------------------------------------------------------------------------
# ### DESIGN DECISIONS
#  ---------------------------------------------------------------------------
#  Source treated as immutable. Neon is modelled as a production OLTP system we
#  do not control, so no schema changes were made there to simplify ingestion.
#  Every accommodation is made inside the medallion.
# 
#  Inclusive watermark. Incremental reads use >= rather than >, so rows sharing
#  the highest watermark value are never skipped. On a DATE column this is not
#  an edge case: 85,410 loads span roughly 1,095 dates, so about 78 rows share
#  any given boundary. The re-read rows are retained here and resolved in
#  silver, because bronze does not deduplicate.
# 
#  Not partitioned. Microsoft advises leaving tables under 1 TB unpartitioned
#  and targeting at least 1 GB per partition; this dataset is around 125 MB.
#  Partitioning primarily isolates concurrent writers, and there is one writer.
#  Delta statistics and file skipping handle pruning at this scale.
# 
#  V-Order left disabled. Bronze is read by Spark, which gains nothing from
#  V-Order while paying 15 to 33 percent slower writes. It is enabled in gold,
#  where Direct Lake benefits from it.
# 
# **Document read partitioning benchmark results**:
# Measured fuel_purchases (196,442 rows) at 0, 4 and 8 JDBC read
# partitions: 5.5s, 5.8s, 5.0s. No benefit at this volume. Partitioning
# retained as a demonstration with the measurements recorded inline.
# Copy job on the same table averages ~60s.
# 
#  ---------------------------------------------------------------------------
# ### CONFIGURATION
#  ---------------------------------------------------------------------------
# -   parameter cell    runtime arguments a pipeline overrides
# -   Variable Library  environment config, with dev/test/prod value sets
# -   Key Vault         the password only
# 
#  ---------------------------------------------------------------------------
# ### SETUP
#  ---------------------------------------------------------------------------
#    1. Create Variable Library "vl_logistics" (see CELL 2).
#    2. Create the notebook; attach lh_logistics_bronze as default lakehouse.
#    3. Paste each CELL block into its own cell.
#    4. On CELL 1: cell toolbar -> ... -> Toggle parameter cell.
#  


# PARAMETERS CELL ********************

# ===========================================================================
# CELL 1 — PARAMETERS   (mark with Toggle parameter cell)
# ===========================================================================
# Non-secret runtime arguments only. Secrets are never passed as notebook
# parameters. A pipeline overrides these by injecting a cell below this one.
 
load_mode           = "full"   # "full" replaces one ingest slice, "incremental" appends
ingest_date         = ""       # ISO date stamped on this batch; empty means today
tables_filter       = ""       # comma-separated subset for targeted reruns
max_read_partitions = 0        # 0 derives parallelism from the Spark session

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 2 — CONFIGURATION
# ===========================================================================
# Environment values come from a Variable Library item named "vl_logistics"
# holding neon_host, neon_database, neon_user and keyvault_uri. Value sets for
# Development, Test and Production are activated per stage by deployment
# pipelines, so promoting this notebook requires no code change.
#
# Values are read into locals once rather than re-read per use.
 
from datetime import date, datetime
 
VARIABLE_LIBRARY = "vl_logistics"
 
try:
    vl            = notebookutils.variableLibrary.getLibrary(VARIABLE_LIBRARY)
    NEON_HOST     = vl.neon_host
    NEON_DATABASE = vl.neon_database
    NEON_USER     = vl.neon_user
    KEYVAULT_URI  = vl.keyvault_uri
    config_source = f"Variable Library '{VARIABLE_LIBRARY}'"
 
except Exception as exc:
    # Non-secret defaults so the notebook runs before the library exists.
    print(f"Variable Library unavailable ({exc}). Using inline defaults.")
    NEON_HOST     = "ep-divine-cloud-b2fwle17.c-6.eu-central-1.aws.neon.tech"
    NEON_DATABASE = "neondb"
    NEON_USER     = "neondb_owner"
    KEYVAULT_URI  = ""
    config_source = "inline defaults"
 
EFFECTIVE_INGEST_DATE = ingest_date.strip() or date.today().isoformat()
JDBC_URL = f"jdbc:postgresql://{NEON_HOST}:5432/{NEON_DATABASE}?sslmode=require"
 
BRONZE_PREFIX = "bronze_"
SOURCE_SYSTEM = "neon_postgres_logistics"
 
# numPartitions is the count of simultaneous JDBC connections opened against
# Postgres. Matching it to available executor cores keeps every executor busy;
# going beyond that only queues work and adds load on the source database.
if max_read_partitions and max_read_partitions > 0:
    READ_PARTITIONS, partitions_source = int(max_read_partitions), "parameter"
else:
    READ_PARTITIONS = max(2, int(spark.sparkContext.defaultParallelism))
    partitions_source = "session parallelism"
 
# Allows replaceWhere to target _ingest_date, which is a data column rather
# than a partition column.
spark.conf.set("spark.databricks.delta.replaceWhere.dataColumns.enabled", "true")
 
print(f"config          : {config_source}")
print(f"host            : {NEON_HOST}")
print(f"load mode       : {load_mode}")
print(f"ingest date     : {EFFECTIVE_INGEST_DATE}")
print(f"read partitions : {READ_PARTITIONS} ({partitions_source})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 3 — CREDENTIAL
# ===========================================================================
# getSecret authenticates as the identity running the notebook, and notebook
# output redacts secret values automatically.
 
SECRET_NAME = "neon-logistics-password"
 
if KEYVAULT_URI:
    NEON_PASSWORD = notebookutils.credentials.getSecret(KEYVAULT_URI, SECRET_NAME)
    print("Credential source: Key Vault")
else:
    NEON_PASSWORD = globals().get("_local_password", "")
    if not NEON_PASSWORD:
        raise ValueError(
            "No credential available. Set keyvault_uri in the Variable "
            "Library, or define _local_password for this session."
        )
    print("Credential source: session variable (development only)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 4 — SOURCE MANIFEST
# ===========================================================================
# watermark_column drives incremental reads. Reference tables have none: they
# are small and always read in full.
#
# partition_read splits a full read across parallel JDBC connections. Applied
# only to the two six-figure tables. This is read parallelism against the
# source and is unrelated to Delta table partitioning.
 
SOURCE_TABLES = {
    "facilities":          {"watermark_column": None},
    "trucks":              {"watermark_column": None},
    "drivers":             {"watermark_column": None},
    "trailers":            {"watermark_column": None},
    "customers":           {"watermark_column": None},
    "routes":              {"watermark_column": None},
    "safety_incidents":    {"watermark_column": "incident_date"},
    "maintenance_records": {"watermark_column": "maintenance_date"},
    "loads":               {"watermark_column": "load_date"},
    "trips":               {"watermark_column": "dispatch_date"},
    "delivery_events":     {"watermark_column": "actual_datetime",
                            "partition_read":   True,
                            "lower": "2022-01-01 00:00:00",
                            "upper": "2025-01-01 00:00:00"},
    "fuel_purchases":      {"watermark_column": "purchase_date",
                            "partition_read":   True,
                            "lower": "2022-01-01 00:00:00",
                            "upper": "2025-01-01 00:00:00"},
}
 
if tables_filter.strip():
    wanted = {t.strip() for t in tables_filter.split(",")}
    SOURCE_TABLES = {k: v for k, v in SOURCE_TABLES.items() if k in wanted}
 
print(f"{len(SOURCE_TABLES)} tables in scope")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 5 — INGESTION FUNCTIONS
# ===========================================================================
 
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.utils import AnalysisException
 
 
def latest_watermark(table_name, watermark_column):
    """Highest watermark already held in bronze, or None for a new table."""
    if not watermark_column:
        return None
    try:
        return spark.sql(
            f"SELECT max({watermark_column}) AS wm "
            f"FROM {BRONZE_PREFIX}{table_name}"
        ).collect()[0]["wm"]
    except AnalysisException:
        return None
 
 
def read_source(table_name, cfg):
    """
    Read one table from Neon over JDBC.
 
    Incremental reads filter with >= so no row sharing the boundary value is
    lost. The predicate is pushed down to Postgres, so unchanged rows never
    cross the network.
    """
    watermark_column = cfg.get("watermark_column")
    dbtable          = table_name
 
    if load_mode == "incremental" and watermark_column:
        watermark = latest_watermark(table_name, watermark_column)
        if watermark:
            dbtable = (
                f"(SELECT * FROM {table_name} "
                f"WHERE {watermark_column} >= '{watermark}') AS src"
            )
            print(f"    incremental from {watermark} (inclusive)")
        else:
            print("    no prior watermark, reading in full")
 
    reader = (
        spark.read.format("jdbc")
        .option("url",      JDBC_URL)
        .option("dbtable",  dbtable)
        .option("user",     NEON_USER)
        .option("password", NEON_PASSWORD)
        .option("driver",   "org.postgresql.Driver")
        # The JDBC default of 10 rows per round trip turns a six-figure table
        # into tens of thousands of network calls to Frankfurt.
        .option("fetchsize", "10000")
    )

    # JDBC read partitioning: splits the read across N parallel connections
    # to the source. RETAINED AS A DEMONSTRATION, not because it helps here.
    #
    # Benchmarked on fuel_purchases (196,442 rows, ~40 MB):
    #     0 partitions  5.5s
    #     4 partitions  5.8s
    #     8 partitions  5.0s
    #
    # No measurable benefit at this volume. Connection setup, TLS negotiation
    # and query planning across N connections offset the transfer saving.
    # Parallel reads pay off once transfer time dominates setup cost, which
    # is well above this dataset's size.
 
    if cfg.get("partition_read") and load_mode == "full":
        reader = (
            reader
            .option("partitionColumn", cfg["watermark_column"])
            .option("lowerBound",      cfg["lower"])
            .option("upperBound",      cfg["upper"])
            .option("numPartitions",   str(READ_PARTITIONS))
        )
 
    return reader.load()
 
 
def add_lineage(df):
    """Ingestion metadata. The only columns bronze adds to source data."""
    return (
        df
        .withColumn("_ingest_date",   lit(EFFECTIVE_INGEST_DATE))
        .withColumn("_ingested_at",   current_timestamp())
        .withColumn("_source_system", lit(SOURCE_SYSTEM))
        .withColumn("_load_mode",     lit(load_mode))
    )
 
 
def write_bronze(df, table_name):
    """
    Land a batch in bronze.
 
    First run    creates the table.
    Full load    replaces the slice for this ingest date only, so a re-run is
                 idempotent while every earlier batch survives untouched.
    Incremental  appends. Rows re-read at the watermark boundary are kept
                 rather than deduplicated, because bronze records what
                 arrived. Silver resolves them.
    """
    target   = f"{BRONZE_PREFIX}{table_name}"
    enriched = add_lineage(df)
 
    if not spark.catalog.tableExists(target):
        enriched.write.format("delta").mode("overwrite").saveAsTable(target)
        return target, "created"
 
    if load_mode == "full":
        (
            enriched.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"_ingest_date = '{EFFECTIVE_INGEST_DATE}'")
            .saveAsTable(target)
        )
        return target, "replaced"
 
    enriched.write.format("delta").mode("append").saveAsTable(target)
    return target, "appended"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 6 — RUN
# ===========================================================================
 
results = []
 
for table_name, cfg in SOURCE_TABLES.items():
    started = datetime.now()
    print(f"[{started:%H:%M:%S}] {table_name}")
 
    try:
        df   = read_source(table_name, cfg)
        rows = df.count()
 
        if rows == 0:
            print("    no new rows")
            results.append((table_name, 0, "skipped"))
            continue
 
        target, action = write_bronze(df, table_name)
        elapsed = (datetime.now() - started).total_seconds()
        print(f"    {rows:,} rows -> {target} [{action}] ({elapsed:.1f}s)")
        results.append((table_name, rows, action))
 
    except Exception as exc:
        print(f"    FAILED: {exc}")
        results.append((table_name, 0, f"failed: {exc}"))
 
succeeded = sum(1 for r in results if not str(r[2]).startswith("failed"))
print(f"\n{succeeded}/{len(results)} tables processed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 7 — RECONCILIATION
# ===========================================================================
# A full load must reproduce the source row counts exactly; any difference is
# a defect. An incremental load legitimately exceeds them, because boundary
# rows are re-read by design and bronze retains everything it receives.
 
SOURCE_COUNTS = {
    "customers":              200,
    "facilities":              50,
    "drivers":                150,
    "trucks":                 120,
    "trailers":               180,
    "routes":                  58,
    "loads":                85410,
    "trips":                85410,
    "maintenance_records":   2920,
    "safety_incidents":       170,
    "delivery_events":     170820,
    "fuel_purchases":      196442,
}
 
print(f"{'table':<22}{'bronze':>10}{'source':>10}  status")
print("-" * 54)
 
clean = True
for table_name in SOURCE_TABLES:
    expected = SOURCE_COUNTS.get(table_name, -1)
    try:
        actual = spark.sql(
            f"SELECT count(*) AS c FROM {BRONZE_PREFIX}{table_name}"
        ).collect()[0]["c"]
    except Exception:
        actual = -1
 
    if load_mode == "full":
        ok = (actual == expected)
        label = "OK" if ok else "MISMATCH"
    else:
        ok = (actual >= expected)
        label = "OK" if ok else "SHORT"
 
    clean &= ok
    print(f"{table_name:<22}{actual:>10,}{expected:>10,}  {label}")
 
print("-" * 54)
print("Reconciled against source." if clean
      else "Reconciliation failed. Resolve before building silver.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 8 — FIDELITY CHECKS
# ===========================================================================
 
display(spark.sql("SHOW TABLES"))
 
# Batch lineage. One row per ingestion run per mode, showing how bronze
# accumulates rather than overwrites.
display(spark.sql("""
    SELECT _ingest_date, _load_mode, count(*) AS rows
    FROM bronze_trips
    GROUP BY _ingest_date, _load_mode
    ORDER BY _ingest_date
"""))
 
# The source carries an intentional 2 percent unassigned rate on trips. Bronze
# must reproduce it exactly; any drift means the layer is altering source data.
#
# These nulls are preserved, not dropped. In gold they resolve to the Unknown
# dimension member, because Fabric Warehouse does not enforce foreign keys and
# an unmatched key would silently disappear from an inner join.
display(spark.sql("""
    SELECT
        count(*)                                            AS total_trips,
        sum(CASE WHEN driver_id IS NULL THEN 1 ELSE 0 END)  AS unassigned_driver,
        round(100.0 * sum(CASE WHEN driver_id IS NULL THEN 1 ELSE 0 END)
              / count(*), 2)                                AS pct_unassigned
    FROM bronze_trips
"""))
 
# Delta transaction log: the immutable audit trail of every batch.
display(spark.sql("DESCRIBE HISTORY bronze_trips"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 9 — TABLE MAINTENANCE
# ===========================================================================
# Run after a large batch, not on every incremental run.
#
# OPTIMIZE compacts the many small files that parallel writes produce into
# larger ones, which is what keeps read performance and the SQL analytics
# endpoint sync healthy.
#
# VACUUM removes files no longer referenced by the transaction log. The
# default seven-day retention is the floor: shorter windows break time travel
# and can corrupt concurrent readers.
 
for table_name in ["loads", "trips", "delivery_events", "fuel_purchases"]:
    target = f"{BRONZE_PREFIX}{table_name}"
    print(f"OPTIMIZE {target}")
    spark.sql(f"OPTIMIZE {target}")
 
# Uncomment once the table has accumulated several batches.
# for table_name in ["loads", "trips", "delivery_events", "fuel_purchases"]:
#     spark.sql(f"VACUUM {BRONZE_PREFIX}{table_name} RETAIN 168 HOURS")
 
print("Maintenance complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
