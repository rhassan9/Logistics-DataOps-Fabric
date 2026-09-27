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
# Requires    : nb_silver_create_tables has run in this environment.
#
# Silver holds one validated, non-aggregated row per business entity.
# Deduplication, derived business columns, and data quality flags. Aggregation
# and dimensional modelling belong in gold.
#
# Writes are MERGE on the business key, so loads are idempotent and silver
# holds current state (SCD Type 1). Gold applies Type 2 where a dimension
# needs history.
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
 
 
def read_bronze(table_name, business_key):
    """Read one bronze table, reduced to the earliest row per business key.
 
    Bronze re-reads rows at the watermark boundary by design, so a key can
    appear in more than one batch.
    """
    df = spark.table(bronze(table_name))
 
    if load_mode == "incremental" and ingest_from.strip():
        df = df.filter(F.col("_ingest_date") >= ingest_from.strip())
 
    earliest = Window.partitionBy(business_key).orderBy(F.col("_ingested_at").asc())
 
    return (
        df.withColumn("_rn", F.row_number().over(earliest))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
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
 
NON_BUSINESS = {"_silver_run_id", "_source_ingest_date"} 
 
def write_silver(df, table_name, business_key):
    """MERGE into an existing silver table."""
    target = silver(table_name)
 
    if not spark.catalog.tableExists(target):
        raise ValueError(
            f"{target} does not exist. Run nb_silver_create_tables first: "
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
    variance = (
        F.unix_timestamp("actual_datetime") - F.unix_timestamp("scheduled_datetime")
    ) / 60
 
    # Flagged at load level so both events of an affected load are marked.
    reversed_loads = (
        df.groupBy("load_id")
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
            F.when(F.col("arrival_variance_minutes") <= -ON_TIME_WINDOW_MINUTES, "Early")
             .when(F.col("arrival_variance_minutes") >=  ON_TIME_WINDOW_MINUTES, "Late")
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
# Silver preserves its source's row count. Deduplication is the only operation
# permitted to reduce it, so a shortfall means rows were lost.
 
SOURCE_COUNTS = {
    "loads":                85410,
    "trips":                85410,
    "delivery_events":     170820,
    "fuel_purchases":      196442,
    "maintenance_records":   2920,
    "safety_incidents":       170,
}
 
print(f"{'table':<22}{'silver':>10}{'bronze':>10}  status")
print("-" * 54)
 
for table_name in TABLES:
    expected = SOURCE_COUNTS.get(table_name, -1)
    actual = spark.sql(
        f"SELECT count(*) c FROM {silver(table_name)}"
    ).collect()[0]["c"]
    print(f"{table_name:<22}{actual:>10,}{expected:>10,}  "
          f"{'OK' if actual == expected else 'CHECK'}")
 
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
 
# Must reconcile with the source flag: On Time near 95,095, Early plus Late
# near 75,725. A wider gap means the derived threshold is wrong.
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SHOW TBLPROPERTIES lh_logistics_silver.dbo.silver_drivers

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Table maintenance. Run after a large load, not on every refresh.
#
# OPTIMIZE compacts the small files that parallel writes produce.
# VACUUM removes files no longer referenced by the transaction log. Seven days
# is the floor: shorter windows break time travel and can remove change data
# feed history before gold has consumed it.

LARGE_TABLES = ["loads", "trips", "delivery_events", "fuel_purchases"]

for t in LARGE_TABLES:
    target = f"lh_logistics_silver.dbo.silver_{t}"
    print(f"OPTIMIZE {target}")
    spark.sql(f"OPTIMIZE {target}")

for t in LARGE_TABLES:
    target = f"lh_logistics_silver.dbo.silver_{t}"
    print(f"VACUUM {target}")
    spark.sql(f"VACUUM {target} RETAIN 168 HOURS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for t in ["customers", "drivers", "trucks", "trailers", "routes", "facilities"]:
    spark.sql(f"""
        ALTER TABLE lh_logistics_silver.dbo.silver_{t}
        SET TBLPROPERTIES (delta.enableChangeDataFeed = false)
    """)

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
