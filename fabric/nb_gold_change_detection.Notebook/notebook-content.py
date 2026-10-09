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
# META     },
# META     "warehouse": {
# META       "default_warehouse": "56d7a987-07bd-8948-4154-97e0f74f5cc3",
# META       "known_warehouses": [
# META         {
# META           "id": "56d7a987-07bd-8948-4154-97e0f74f5cc3",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Gold Change Detection
# Fabric Spark / Runtime 2.0+
#
# Purpose:
#   Detect changed Silver keys using Delta Change Data Feed (CDF) and hand a
#   deterministic worklist to the Warehouse Gold stored procedure.
#
# The notebook does NOT decide how fact rows are transformed. It answers only:
#   "Which trip keys could make fact_trip different from what is already in Gold?"
#
# For fact_trip the dependencies are:
#   silver_trips  -> trip-level attributes and source deletion state
#   silver_loads  -> route_id and other load-derived attributes
#   fact.fact_trip -> Gold-side healing (Unknown keys, keys on the wrong SCD2
#                     version) and the hard-delete fallback

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# ---------------------------------------------------------------------------
# CELL 1: PARAMETERS
# ---------------------------------------------------------------------------
run_id = "manual-20261009-08"
steps  = "fact_trip"  # comma-separated as more fact detectors are added
full_scan_steps = "fact_trip"  # comma-separated steps to force a full reconciliation
 
# Full reconciliation is a safe fallback when CDF history is unavailable.
# Set False in a development session when you want CDF failures to stop the run
# so the underlying configuration problem is visible immediately.
ALLOW_FULL_SCAN_FALLBACK = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# CELL 2: SETUP
# ---------------------------------------------------------------------------
import datetime as dt
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    BooleanType, TimestampType,
)
 
WAREHOUSE = "wh_logistics_gold"
SILVER    = "lh_logistics_silver.dbo"
 
CHANGE_TYPES = ["insert", "update_postimage", "delete"]
 
FORCED_FULL = {s.strip() for s in full_scan_steps.split(",") if s.strip()}
 
 
def gold_query(sql):
    """Run a T-SQL query in the warehouse and return the result.
 
    Pass-through keeps type handling in T-SQL: a BIT compared to 0 there is
    unambiguous, while in Spark ANSI mode a boolean compared to 0 fails.
    """
    return spark.read.option(Constants.DatabaseName, WAREHOUSE).synapsesql(sql)
 
if not run_id:
    run_id = "manual-" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
 
print(f"run_id={run_id}")
print(f"steps={steps}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# CELL 3: CHECKPOINT + CDF READER
# ---------------------------------------------------------------------------
checkpoints = {
    (r["step_name"], r["source_table"]): r["last_version"]
    for r in spark.read.synapsesql(f"{WAREHOUSE}.log.vw_cdf_checkpoint").collect()
}

def current_version(table_name):
    """Return the latest Delta table commit version."""
    row = (
        spark.sql(f"DESCRIBE HISTORY {SILVER}.{table_name} LIMIT 1")
             .select("version")
             .first()
    )
    if row is None:
        raise RuntimeError(f"No Delta history found for {SILVER}.{table_name}")
    return int(row["version"])
 
 
def empty_cdf(table_name):
    """Create a zero-row CDF-shaped dataframe without reading CDF history."""
    return (
        spark.table(f"{SILVER}.{table_name}")
             .limit(0)
             .withColumn("_change_type", F.lit(None).cast("string"))
    )
 
 
def cdf_error_is_recoverable(message):
    """Identify errors where a full current-state reconciliation is a safe fallback."""
    msg = message.lower()
    recoverable_markers = (
        "change data feed",
        "change feed",
        "readchangefeed",
        "startingversion",
        "endingversion",
        "_change_data",
        "vacuum",
        "version is not available",
        "version not found",
        "table history",
        "change history",
    )
    return any(marker in msg for marker in recoverable_markers)
 
 
def changed_rows(step, table_name):
    """Return (cdf_dataframe_or_none, version_from, version_to, full_scan_reason).
 
    None for the dataframe means "perform a full reconciliation".
    The bounded read is (version_from, version_to], implemented by starting at
    version_from + 1 and ending at version_to.
    """
    version_from = checkpoints.get((step, table_name))
    version_to = current_version(table_name)
 
    if version_from is None:
        return None, version_from, version_to, "no checkpoint"
 
    if version_from > version_to:
        return None, version_from, version_to, "table version reset/recreated"
 
    if version_from == version_to:
        return empty_cdf(table_name), version_from, version_to, None
 
    try:
        changes = (
            spark.read.format("delta")
                 .option("readChangeFeed", "true")
                 .option("startingVersion", version_from + 1)
                 .option("endingVersion", version_to)
                 .table(f"{SILVER}.{table_name}")
                 .filter(F.col("_change_type").isin(CHANGE_TYPES))
        )
 
        # Force evaluation here. A lazy DataFrame can otherwise make an old
        # CDF window look healthy until much later, after work has been staged.
        changes.limit(1).collect()
        return changes, version_from, version_to, None
 
    except Exception as exc:
        message = str(exc)
        if not ALLOW_FULL_SCAN_FALLBACK or not cdf_error_is_recoverable(message):
            raise RuntimeError(
                f"CDF read failed for {SILVER}.{table_name} "
                f"(from {version_from + 1} to {version_to}): {message}"
            ) from exc
 
        print(
            f"{table_name}: CDF window unavailable; full reconciliation fallback. "
            f"{message[:300]}"
        )
        return None, version_from, version_to, message[:4000]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# CELL 4: FACT DETECTOR
# ---------------------------------------------------------------------------
def detect_fact_trip(step):
    trips_ch, t_from, t_to, t_reason = changed_rows(step, "silver_trips")
    loads_ch, l_from, l_to, l_reason = changed_rows(step, "silver_loads")
 
    forced = step in FORCED_FULL
    full_scan = forced or trips_ch is None or loads_ch is None
    if forced:
        t_reason = t_reason or "forced"
        l_reason = l_reason or "forced"
 
    if full_scan:
        # Include ALL Silver keys, including soft-deleted rows. Gold needs the
        # deletion state in order to preserve a soft-deleted fact row.
        silver_keys = (
            spark.table(f"{SILVER}.silver_trips")
                 .select("trip_id")
        )
 
        # Defensive fallback: if Silver has unexpectedly physically lost a key,
        # keep the existing Gold key in the worklist so the Warehouse procedure
        # can soft-delete it.
        gold_keys = gold_query("SELECT f.trip_id FROM fact.fact_trip AS f")
 
        keys = silver_keys.unionByName(gold_keys)
 
    else:
        # Trip inserts/updates/deletes directly affect the trip fact.
        trip_keys = trips_ch.select("trip_id")
 
        # A changed load can change the route and therefore every trip derived
        # from that load. CDF update_postimage/delete rows are deduplicated by
        # the load_id itself before joining to current Silver trips.
        changed_load_ids = loads_ch.select("load_id").distinct()
        load_affected_trip_keys = (
            spark.table(f"{SILVER}.silver_trips").alias("t")
                 .join(changed_load_ids.alias("l"), F.col("t.load_id") == F.col("l.load_id"), "inner")
                 .select(F.col("t.trip_id"))
        )
 
        keys = trip_keys.unionByName(load_affected_trip_keys)
 
    # Gold-side healing: rows that need reprocessing although neither silver
    # table changed.
    #   1. An Unknown key, because the dimension row may have arrived since.
    #   2. A Type 2 key on the wrong version: a dimension version created after
    #      the fact was loaded can start on or before its dispatch date. Driver
    #      and truck are the Type 2 dimensions; add any other that becomes one.
    #   3. A key with no dimension row at all, left behind when a dimension is
    #      rebuilt and its surrogate keys are reassigned.
    heal_gold_keys = gold_query("""
        SELECT f.trip_id
        FROM fact.fact_trip AS f
        WHERE f.is_deleted_in_source = 0
          AND -1 IN (f.dispatch_date_key, f.driver_key, f.truck_key, f.trailer_key, f.route_key)
        UNION
        SELECT f.trip_id
        FROM fact.fact_trip AS f
        JOIN dim.dim_driver AS d ON d.driver_key = f.driver_key
        JOIN dim.dim_driver AS c
          ON c.driver_id = d.driver_id
         AND f.dispatch_date_key >= c.valid_from_date_key
         AND f.dispatch_date_key <  c.valid_to_date_key
        WHERE f.driver_key > 0 AND c.driver_key <> f.driver_key
        UNION
        SELECT f.trip_id
        FROM fact.fact_trip AS f
        JOIN dim.dim_truck AS d ON d.truck_key = f.truck_key
        JOIN dim.dim_truck AS c
          ON c.truck_id = d.truck_id
         AND f.dispatch_date_key >= c.valid_from_date_key
         AND f.dispatch_date_key <  c.valid_to_date_key
        WHERE f.truck_key > 0 AND c.truck_key <> f.truck_key
        UNION
        SELECT f.trip_id
        FROM fact.fact_trip AS f
        WHERE NOT EXISTS (SELECT 1 FROM dim.dim_driver  AS d WHERE d.driver_key  = f.driver_key)
           OR NOT EXISTS (SELECT 1 FROM dim.dim_truck   AS d WHERE d.truck_key   = f.truck_key)
           OR NOT EXISTS (SELECT 1 FROM dim.dim_trailer AS d WHERE d.trailer_key = f.trailer_key)
           OR NOT EXISTS (SELECT 1 FROM dim.dim_route   AS d WHERE d.route_key   = f.route_key)
    """)
 
    keys = (
        keys.unionByName(heal_gold_keys)
            .where(F.col("trip_id").isNotNull())
            .distinct()
    )
 
    batches = [
        {
            "step_name": step,
            "source_table": "silver_trips",
            "version_from": t_from,
            "version_to": t_to,
            "is_full_scan": full_scan,
            "fallback_reason": t_reason,
        },
        {
            "step_name": step,
            "source_table": "silver_loads",
            "version_from": l_from,
            "version_to": l_to,
            "is_full_scan": full_scan,
            "fallback_reason": l_reason,
        },
    ]
 
    return keys, batches
 
 
STEPS = {
    "fact_trip": detect_fact_trip,
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# CELL 5: WRITE WORKLIST + CDF LEDGER
# ---------------------------------------------------------------------------
batch_schema = StructType([
    StructField("run_id",       StringType(),  False),
    StructField("step_name",    StringType(),  False),
    StructField("source_table", StringType(),  False),
    StructField("version_from", LongType(),    True),
    StructField("version_to",   LongType(),    False),
    StructField("is_full_scan", BooleanType(), False),
    StructField("detected_at",  TimestampType(), False),
])
 
for step in [s.strip() for s in steps.split(",") if s.strip()]:
    if step not in STEPS:
        raise ValueError(f"Unknown step '{step}'. Known steps: {sorted(STEPS)}")
 
    keys, batches = STEPS[step](step)
 
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
 
    key_frame = (
        keys.select(
            F.lit(run_id).cast("string").alias("run_id"),
            F.col("trip_id").cast("string").alias("trip_id"),
            F.lit(now).cast("timestamp").alias("detected_at"),
        )
    )
 
    key_count = key_frame.count()
    if key_count:
        (
            key_frame.write
                     .mode("append")
                     .synapsesql(f"{WAREHOUSE}.stg.changed_trip_keys")
        )
 
    ledger_rows = [
        (
            run_id,
            b["step_name"],
            b["source_table"],
            b["version_from"],
            b["version_to"],
            b["is_full_scan"],
            now,
        )
        for b in batches
    ]
 
    (
        spark.createDataFrame(ledger_rows, batch_schema)
             .write
             .mode("append")
             .synapsesql(f"{WAREHOUSE}.log.cdf_batch")
    )
 
    reasons = "; ".join(
        f"{b['source_table']}={b['fallback_reason']}"
        for b in batches if b["fallback_reason"]
    )
 
    print(
        f"{step}: keys={key_count:,}; full_scan={batches[0]['is_full_scan']}"
        + (f"; fallback={reasons}" if reasons else "")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# CELL 6: HANDOFF CHECKS
# ---------------------------------------------------------------------------
print("\nWorklist by run_id:")
display(
    spark.read
         .synapsesql(f"{WAREHOUSE}.stg.changed_trip_keys")
         .filter(F.col("run_id") == run_id)
         .groupBy("run_id")
         .agg(F.count("trip_id").alias("trip_keys"))
)
 
print("\nCDF batches recorded:")
display(
    spark.read
         .synapsesql(f"{WAREHOUSE}.log.cdf_batch")
         .filter(F.col("run_id") == run_id)
         .select("run_id", "step_name", "source_table",
                 "version_from", "version_to", "is_full_scan", "detected_at")
         .orderBy("source_table")
)
 
print(
    "\nNext step: execute the Warehouse procedure with the same run_id. "
    "The checkpoint is not trusted until the procedure logs 'succeeded'."
)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
