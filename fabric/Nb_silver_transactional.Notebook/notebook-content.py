# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e781f9bd-98b2-4482-9e9d-a2731909476f",
# META       "default_lakehouse_name": "lh_logistics_silver",
# META       "default_lakehouse_workspace_id": "a77071e4-bd2a-4979-8910-91ddb8cd2a09",
# META       "known_lakehouses": [
# META         {
# META           "id": "e781f9bd-98b2-4482-9e9d-a2731909476f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Silver Transformation — Transactional Tables
#
# Source      : lh_logistics_bronze
# Destination : lh_logistics_silver
# Scope       : six transactional tables. Reference tables are built with
#               Dataflow Gen2 against the same schemas.
# Requires    : Nb_silver_create_tables has run in this environment.
#
# Silver holds one validated, non-aggregated row per business entity.
# Deduplication, derived business columns, and data quality flags. Aggregation
# and dimensional modelling belong in gold.
#
# Writes are MERGE on the business key, so loads are idempotent and silver
# holds current state (SCD Type 1). Gold applies Type 2 where a dimension
# needs history.
#
# Rows are never physically deleted. A key that no longer exists at the
# source keeps its last values with _is_deleted_in_source = true, and
# _source_changed_at dates when the current state took effect at the source.
#
# Tables are not created here. Change data feed captures nothing
# retrospectively, so tables must already exist with the property set.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# ===========================================================================
# CELL 1 — PARAMETERS   (mark with Toggle parameter cell)
# ===========================================================================
 
load_mode     = "full"   # "full" | "incremental"
ingest_from   = ""       # ISO date; incremental reads bronze rows on or after this
tables_filter = ""       # comma-separated subset for targeted reruns
run_maintenance = True   # True only on a scheduled maintenance run

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 2 — CONFIGURATION
# ===========================================================================
 
from datetime import datetime
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable
 
BRONZE_LAKEHOUSE = "lh_logistics_bronze"
SILVER_LAKEHOUSE = "lh_logistics_silver"
OPS_LAKEHOUSE    = "lh_logistics_ops"
SCHEMA           = "dbo"
 
BRONZE_PREFIX = "bronze_"
SILVER_PREFIX = "silver_"
 
RUN_ID      = datetime.now().strftime("%Y%m%d_%H%M%S")
RUN_STARTED = datetime.now()
 
# Freight convention: two hours of appointment tolerance, and two hours of
# free detention before the shipper starts paying.
FREE_DETENTION_MINUTES = 120
ON_TIME_WINDOW_MINUTES = 120
 
# Set by the MERGE rather than by the transformation, so an insert stamps both
# and an update touches only _silver_updated_at.
MERGE_MANAGED = {"_silver_created_at", "_silver_updated_at"}

LINEAGE = ["_ingest_date", "_ingested_at", "_source_system", "_load_mode"]

 
 
def bronze(table_name):
    return f"{BRONZE_LAKEHOUSE}.{SCHEMA}.{BRONZE_PREFIX}{table_name}"
 
 
def silver(table_name):
    return f"{SILVER_LAKEHOUSE}.{SCHEMA}.{SILVER_PREFIX}{table_name}"
 
 
print(f"run {RUN_ID}  ·  mode {load_mode}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 3 — TABLE MANIFEST
# ===========================================================================
# Ordered by dependency: fuel_purchases reads silver_trucks for tank capacity.
# business_key is both the MERGE join condition and the deduplication key.
 
TABLES = {
    "loads":               {"business_key": "load_id"},
    "trips":               {"business_key": "trip_id"},
    "delivery_events":     {"business_key": "event_id"},
    "maintenance_records": {"business_key": "maintenance_id"},
    "safety_incidents":    {"business_key": "incident_id"},
    "fuel_purchases":      {"business_key": "fuel_purchase_id"},
}
 
if tables_filter.strip():
    wanted = {t.strip() for t in tables_filter.split(",")}
    TABLES = {k: v for k, v in TABLES.items() if k in wanted}
 
print(f"{len(TABLES)} tables in scope")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 4 — SHARED FUNCTIONS
# ===========================================================================
def current_state(df, business_key, detect_deletes):
    """Reduce bronze rows to one current-state row per business key.
 
    The latest version observed is current. _source_changed_at is the first
    bronze batch of the unbroken run of identical versions that ends at the
    latest one. A run is broken by a change of values, or by a complete
    (full-mode) snapshot the key was missing from, so a value that changes
    and changes back, or a key that disappears and returns unchanged, is
    dated by its return.
 
    A key whose last appearance precedes the latest complete snapshot no
    longer exists at the source. Its last version is kept, flagged, and
    dated by the first snapshot that no longer contained it. Both snapshot
    rules need every batch, so they apply only when detect_deletes is true.
    """
    business = [c for c in df.columns if c not in LINEAGE]
    oldest_first = Window.partitionBy(business_key).orderBy(F.col("_ingested_at").asc())
    newest_first = Window.partitionBy(business_key).orderBy(F.col("_ingested_at").desc(),
                                                             F.col("_ingest_date").desc())
    running = oldest_first.rowsBetween(Window.unboundedPreceding, Window.currentRow)
 
    history = (
        df.withColumn("_version", F.sha2(F.to_json(F.struct(*business)), 256))
          .withColumn("_previous_version", F.lag("_version").over(oldest_first))
          .withColumn("_previous_seen_at", F.lag("_ingested_at").over(oldest_first))
    )
 
    if detect_deletes:
        # Every bronze write stamps one _ingested_at, so each distinct value
        # among full-mode rows is one complete snapshot.
        snapshots = (df.filter(F.col("_load_mode") == "full")
                       .select(F.col("_ingested_at").alias("_snapshot_at")).distinct())
        # A complete snapshot between two appearances means the key was gone.
        returned = (
            history.select(business_key, "_ingested_at", "_previous_seen_at")
                   .join(F.broadcast(snapshots),
                         (F.col("_snapshot_at") > F.col("_previous_seen_at")) &
                         (F.col("_snapshot_at") < F.col("_ingested_at")))
                   .select(business_key, "_ingested_at").distinct()
                   .withColumn("_returned", F.lit(True))
        )
        history = history.join(returned, [business_key, "_ingested_at"], "left")
    else:
        history = history.withColumn("_returned", F.lit(None).cast("boolean"))
 
    starts_version = (
        F.col("_previous_version").isNull()
        | (F.col("_previous_version") != F.col("_version"))
        | F.coalesce(F.col("_returned"), F.lit(False))
    )
 
    latest = (
        history
          .withColumn("_source_changed_at",
                      F.last(F.when(starts_version, F.col("_ingested_at")),
                             ignorenulls=True).over(running))
          .withColumn("_rn", F.row_number().over(newest_first))
          .filter(F.col("_rn") == 1)
          .drop("_version", "_previous_version", "_previous_seen_at", "_returned", "_rn")
    )
 
    if detect_deletes:
        gone = (
            latest.select(business_key, "_ingested_at")
                  .join(F.broadcast(snapshots), F.col("_snapshot_at") > F.col("_ingested_at"))
                  .groupBy(business_key)
                  .agg(F.min("_snapshot_at").alias("_deleted_seen_at"))
        )
        latest = latest.join(gone, business_key, "left")
    else:
        latest = latest.withColumn("_deleted_seen_at", F.lit(None).cast("timestamp"))
 
    return (
        latest
        .withColumn("_is_deleted_in_source", F.col("_deleted_seen_at").isNotNull())
        .withColumn("_source_changed_at", F.coalesce("_deleted_seen_at", "_source_changed_at"))
        .drop("_deleted_seen_at")
    )
 
 
def read_bronze(table_name, business_key):
    """Read one bronze table as current state per business key.
 
    Incremental runs read only recent batches. They can show what changed but
    not what disappeared, so deletions are detected on full runs. A key silver
    already holds as deleted is revived only by a bronze row newer than its
    deletion, and is then dated by that return.
    """
    df = spark.table(bronze(table_name))
 
    if load_mode == "full":
        return current_state(df, business_key, detect_deletes=True)
 
    if ingest_from.strip():
        df = df.filter(F.col("_ingest_date") >= ingest_from.strip())
 
    out = current_state(df, business_key, detect_deletes=False)
 
    deleted = (
        spark.table(silver(table_name))
             .where(F.col("_is_deleted_in_source"))
             .select(business_key, F.col("_source_changed_at").alias("_deleted_at"))
    )
    back = (
        df.join(deleted, business_key)
          .where(F.col("_ingested_at") > F.col("_deleted_at"))
          .groupBy(business_key)
          .agg(F.min("_ingested_at").alias("_back_at"))
    )
    return (
        out.join(deleted, business_key, "left")
           .join(back, business_key, "left")
           # Seen only before its deletion: leave the silver row as it is.
           .where(F.col("_deleted_at").isNull() | F.col("_back_at").isNotNull())
           # Dated by the return, unless the values changed again after it.
           .withColumn("_source_changed_at", F.greatest("_back_at", "_source_changed_at"))
           .drop("_deleted_at", "_back_at")
    )
 
 
def add_audit_columns(df):
    """Carry the bronze ingest date forward and stamp the run."""
    return (
        df
        .withColumnRenamed("_ingest_date", "_source_ingest_date")
        .withColumn("_silver_run_id", F.lit(RUN_ID))
        .drop("_ingested_at", "_source_system", "_load_mode")
    )
 
 
def set_dq_status(df, flag_columns):
    """Roll a table's data quality flags into one status column."""
    if not flag_columns:
        return df.withColumn("_dq_status", F.lit("valid"))
 
    any_flag = F.lit(False)
    for c in flag_columns:
        any_flag = any_flag | F.coalesce(F.col(c), F.lit(False))
 
    return df.withColumn(
        "_dq_status",
        F.when(any_flag, F.lit("flagged")).otherwise(F.lit("valid"))
    )
 
 
def align_to_target(df, target):
    """Project onto the target schema, failing on any mismatch.
 
    MERGE_MANAGED columns are excluded: the MERGE supplies them.
    """
    target_cols = [c for c in spark.table(target).columns if c not in MERGE_MANAGED]
    missing = set(target_cols) - set(df.columns)
    extra   = set(df.columns) - set(target_cols)
 
    if missing or extra:
        raise ValueError(
            f"{target} schema mismatch. "
            f"missing: {sorted(missing) or 'none'}; "
            f"unexpected: {sorted(extra) or 'none'}"
        )
 
    return df.select(*target_cols)
 
NON_BUSINESS = {"_silver_run_id", "_source_ingest_date", "_source_changed_at"}
 
def write_silver(df, table_name, business_key):
    """MERGE into an existing silver table."""
    target = silver(table_name)
 
    if not spark.catalog.tableExists(target):
        raise ValueError(
            f"{target} does not exist. Run Nb_silver_create_tables first: "
            f"change data feed must be enabled before the first write."
        )
 
    aligned = align_to_target(df, target)
 
    changed = " OR ".join(
        f"NOT (t.`{c}` <=> s.`{c}`)"
        for c in aligned.columns
        if c not in NON_BUSINESS and c != business_key
    )
 
    (
        DeltaTable.forName(spark, target).alias("t")
        .merge(aligned.alias("s"), f"t.{business_key} = s.{business_key}")
        .whenMatchedUpdate(
            condition=changed,
            set={
                **{c: F.col(f"s.{c}") for c in aligned.columns},
                "_silver_updated_at": F.current_timestamp(),
            },
        )
        .whenNotMatchedInsert(values={
            **{c: F.col(f"s.{c}") for c in aligned.columns},
            "_silver_created_at": F.current_timestamp(),
            "_silver_updated_at": F.current_timestamp(),
        })
        .execute()
    )
 
    return target

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 5 — TRANSFORMATIONS
# ===========================================================================
# Each returns the transformed dataframe and the names of any flag columns.
 
 
def transform_loads(df):
    """Conformance only."""
    return df, []
 
 
def transform_trips(df):
    """Flag trips reporting more idle time than elapsed time.
 
    Independently generated columns: 100 percent of trips under three hours
    fail, none above twelve. Idle time as a share of duration is unusable;
    absolute idle hours remain valid.
    """
    return (
        df.withColumn(
            "is_idle_implausible",
            F.col("idle_time_hours") > F.col("actual_duration_hours")
        ),
        ["is_idle_implausible"],
    )
 
 
def transform_delivery_events(df):
    """Derive arrival performance and billable detention.
 
    arrival_variance_minutes measures carrier punctuality; detention_minutes
    measures facility performance after arrival. They are independent in this
    source (correlation 0.0255), so both are kept.
 
    arrival_status splits the source flag's single false value: Early means
    schedules are over-padded, Late means they are missed.
    """
    # Full timestamp precision. unix_timestamp truncates to whole seconds, which
    # misclassified events within a second of the 120-minute window boundary.
    variance = (
        F.col("actual_datetime").cast("double") - F.col("scheduled_datetime").cast("double")
    ) / 60
 
    # Flagged at load level so both events of an affected load are marked.
    reversed_loads = (
        df.filter(~F.col("_is_deleted_in_source"))
          .groupBy("load_id")
          .agg(
              F.max(F.when(F.col("event_type") == "Pickup",
                           F.col("actual_datetime"))).alias("_picked"),
              F.max(F.when(F.col("event_type") == "Delivery",
                           F.col("actual_datetime"))).alias("_delivered"),
          )
          .withColumn(
              "is_timestamp_reversed",
              F.coalesce(F.col("_delivered") < F.col("_picked"), F.lit(False))
          )
          .select("load_id", "is_timestamp_reversed")
    )
 
    out = (
        df
        .withColumn("arrival_variance_minutes",
                    F.round(variance, 1).cast("decimal(10,1)"))
        .withColumn(
            "arrival_status",
            F.when(variance <= -ON_TIME_WINDOW_MINUTES, "Early")
             .when(variance >=  ON_TIME_WINDOW_MINUTES, "Late")
             .otherwise("On Time")
        )
        .withColumn(
            "billable_detention_minutes",
            F.greatest(
                F.col("detention_minutes") - F.lit(FREE_DETENTION_MINUTES),
                F.lit(0)
            ).cast("int")
        )
        .join(reversed_loads, on="load_id", how="left")
    )
 
    return out, ["is_timestamp_reversed"]
 
 
def transform_maintenance_records(df):
    """Split urgency out of the templated service description.
 
    service_description held 21 values: three urgency levels across seven
    components. The component half duplicates maintenance_type, so only the
    urgency is new. The composite is dropped; bronze retains it verbatim.
    """
    return (
        df
        .withColumn("service_urgency", F.split(F.col("service_description"), " ")[0])
        .drop("service_description"),
        [],
    )
 
 
def transform_safety_incidents(df):
    """Split the templated description and categorise the incident type.
 
    incident_type mixes safety, regulatory, asset and service concerns, so a
    count of safety incidents would otherwise include customer complaints.
 
    location_state is dropped: 25 cities map to more than one state in this
    table, so city and state were assigned independently.
    """
    return (
        df
        .withColumn("incident_severity", F.split(F.col("description"), " ")[0])
        .withColumn("incident_cause",
                    F.regexp_extract(F.col("description"), r"involving (.+)$", 1))
        .withColumn(
            "incident_category",
            F.when(F.col("incident_type").isin("Accident", "Moving Violation"), "Safety")
             .when(F.col("incident_type") == "DOT Violation",      "Regulatory")
             .when(F.col("incident_type") == "Equipment Damage",   "Asset")
             .when(F.col("incident_type") == "Customer Complaint", "Service")
             .otherwise("Other")
        )
        .drop("description", "location_state"),
        [],
    )
 
 
def transform_fuel_purchases(df):
    """Flag fills larger than the truck's tank, and drop invalid geography.
 
    Fill volume was drawn independently of tank size, so 18,105 purchases
    exceed the tank they went into, all on 150-gallon trucks. Neither column
    can be identified as the wrong one, so both are kept and the row flagged.
 
    location_state is dropped for the same reason as in safety_incidents.
    """
    tanks = (
        spark.table(silver("trucks"))
        .select("truck_id", "tank_capacity_gallons")
        .distinct()
    )
 
    out = (
        df.join(tanks, on="truck_id", how="left")
          .withColumn(
              "is_capacity_exceeded",
              F.coalesce(
                  F.col("gallons") > F.col("tank_capacity_gallons"),
                  F.lit(False)
              )
          )
          .drop("tank_capacity_gallons", "location_state")
    )
 
    return out, ["is_capacity_exceeded"]
 
 
TRANSFORMS = {
    "loads":               transform_loads,
    "trips":               transform_trips,
    "delivery_events":     transform_delivery_events,
    "maintenance_records": transform_maintenance_records,
    "safety_incidents":    transform_safety_incidents,
    "fuel_purchases":      transform_fuel_purchases,
}
 

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
 
for table_name, cfg in TABLES.items():
    started = datetime.now()
    key     = cfg["business_key"]
    print(f"[{started:%H:%M:%S}] {table_name}")
 
    try:
        raw                = read_bronze(table_name, key)
        transformed, flags = TRANSFORMS[table_name](raw)
        final              = set_dq_status(add_audit_columns(transformed), flags)
 
        rows    = final.count()
        flagged = final.filter(F.col("_dq_status") == "flagged").count()
        target  = write_silver(final, table_name, key)
        elapsed = (datetime.now() - started).total_seconds()
 
        print(f"    {rows:,} rows, {flagged:,} flagged -> {target} ({elapsed:.1f}s)")
        results.append({
            "run_id": RUN_ID, "table": table_name, "rows": rows,
            "flagged": flagged, "seconds": round(elapsed, 1), "status": "ok",
        })
 
    except Exception as exc:
        print(f"    FAILED: {exc}")
        results.append({
            "run_id": RUN_ID, "table": table_name, "rows": 0,
            "flagged": 0, "seconds": 0.0, "status": f"failed: {exc}",
        })
 
ok = sum(1 for r in results if r["status"] == "ok")
print(f"\n{ok}/{len(results)} tables written")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 
# ===========================================================================
# CELL 7 — VALIDATE
# ===========================================================================
# Silver keeps one row per business key ever seen. Deduplication is the only
# operation permitted to reduce the row count, so a shortfall means rows were
# lost. Source deletions stay in silver as flagged rows; live rows must match
# the source count.
#
# Baselines are the source counts at the time of profiling.
 
SOURCE_COUNTS = {
    "loads":                85410,
    "trips":                85410,
    "delivery_events":     170820,
    "fuel_purchases":      196442,
    "maintenance_records":   2920,
    "safety_incidents":       170,
}
 
print(f"{'table':<22}{'rows':>10}{'keys':>10}{'live':>10}{'deleted':>9}"
      f"{'nulls':>7}{'source':>10}  status")
print("-" * 86)
 
contract_failed = []
for table_name, cfg in TABLES.items():
    key = cfg["business_key"]
    r = spark.sql(f"""
        SELECT count(*)                                                   AS rows,
               count(DISTINCT {key})                                      AS keys,
               sum(CASE WHEN NOT _is_deleted_in_source THEN 1 ELSE 0 END) AS live,
               sum(CASE WHEN _is_deleted_in_source THEN 1 ELSE 0 END)     AS deleted,
               sum(CASE WHEN _is_deleted_in_source IS NULL
                          OR _source_changed_at IS NULL THEN 1 ELSE 0 END) AS nulls
        FROM {silver(table_name)}
    """).first()
    expected = SOURCE_COUNTS.get(table_name, -1)
    contract_ok = r["rows"] == r["keys"] and r["nulls"] == 0
    status = "OK" if contract_ok and r["live"] == expected else "CHECK"
    if not contract_ok:
        contract_failed.append(table_name)
    print(f"{table_name:<22}{r['rows']:>10,}{r['keys']:>10,}{r['live']:>10,}"
          f"{r['deleted']:>9,}{r['nulls']:>7,}{expected:>10,}  {status}")
 
# Expected from profiling: 7,450 implausible idle, 18,105 over capacity,
# 972 reversed events (486 loads, both events flagged).
print("\nData quality flags:")
display(spark.sql(f"""
    SELECT 'trips' AS tbl, 'is_idle_implausible' AS flag,
           sum(CASE WHEN is_idle_implausible THEN 1 ELSE 0 END) AS flagged,
           count(*) AS rows
    FROM {silver('trips')}
    UNION ALL
    SELECT 'fuel_purchases', 'is_capacity_exceeded',
           sum(CASE WHEN is_capacity_exceeded THEN 1 ELSE 0 END), count(*)
    FROM {silver('fuel_purchases')}
    UNION ALL
    SELECT 'delivery_events', 'is_timestamp_reversed',
           sum(CASE WHEN is_timestamp_reversed THEN 1 ELSE 0 END), count(*)
    FROM {silver('delivery_events')}
"""))
 
# Must reconcile exactly with the source flag: 95,095 On Time against true;
# 9,382 Early and 66,343 Late against false. Any other combination means the
# derived threshold is wrong.
print("Arrival status against the source flag:")
display(spark.sql(f"""
    SELECT
        arrival_status,
        on_time_flag,
        count(*)                                 AS events,
        round(avg(arrival_variance_minutes), 1)  AS avg_variance,
        round(avg(detention_minutes), 1)         AS avg_detention,
        sum(billable_detention_minutes)          AS billable_minutes
    FROM {silver('delivery_events')}
    GROUP BY arrival_status, on_time_flag
    ORDER BY events DESC
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 8 — RUN LOG
# ===========================================================================
# Pipeline metadata describes the pipeline, not the business, so it lives in
# the operations lakehouse rather than a medallion layer.
 
(
    spark.createDataFrame(results)
    .withColumn("run_started",  F.lit(RUN_STARTED))
    .withColumn("run_finished", F.current_timestamp())
    .write.format("delta")
    .mode("append")
    .saveAsTable(f"{OPS_LAKEHOUSE}.{SCHEMA}.etl_run_log")
)
 
print(f"run {RUN_ID} logged")
 
# Fail the run after it is logged, so an orchestrator never mistakes a partial
# silver load for a complete one.
failed = [r for r in results if r["status"] != "ok"]
if failed or contract_failed:
    raise RuntimeError(
        f"silver run {RUN_ID} incomplete. "
        f"failed tables: {[r['table'] for r in failed] or 'none'}; "
        f"contract violations: {contract_failed or 'none'}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Table maintenance. Run after a large load, not on every refresh.
#
# OPTIMIZE compacts the small files that parallel writes produce.
# VACUUM removes files no longer referenced by the transaction log, including
# change data. nb_gold_change_detection reads these tables' change feeds, so
# run VACUUM only after gold has consumed every version older than the
# retention window. If it has not, gold falls back to a full reconciliation:
# correct, but a full pass. Seven days is the floor: shorter windows break
# time travel.
 
LARGE_TABLES = ["loads", "trips", "delivery_events", "fuel_purchases"]
 
if run_maintenance:
    for t in LARGE_TABLES:
        target = f"lh_logistics_silver.dbo.silver_{t}"
        print(f"OPTIMIZE {target}")
        spark.sql(f"OPTIMIZE {target}")
 
    for t in LARGE_TABLES:
        target = f"lh_logistics_silver.dbo.silver_{t}"
        print(f"VACUUM {target}")
        spark.sql(f"VACUUM {target} RETAIN 168 HOURS")
else:
    print("Table maintenance skipped (run_maintenance = False)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
