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
run_id = "manual-20261010-05b"
steps = "fact_trip,fact_delivery_event,fact_fuel_purchase,fact_maintenance,fact_safety_incident"  # comma-separated as more fact detectors are added
full_scan_steps = ""  # comma-separated steps to force a full reconciliation
 
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
# CELL 2B: SILVER SQL ENDPOINT SYNC
# ---------------------------------------------------------------------------
# Change detection reads silver through Spark, straight from Delta. The gold
# procedures read the same tables through the lakehouse SQL analytics
# endpoint, whose metadata syncs in the background and can lag. This cell
# forces the sync, then proves Spark and the endpoint see the same silver
# before any checkpoint can advance.
import sempy
try:
    from sempy.fabric.sql_endpoint import refresh_sql_endpoint_metadata
except ImportError as exc:
    raise RuntimeError(
        f"semantic-link-sempy {sempy.__version__} has no sql_endpoint module; 0.13.0 or later is required."
    ) from exc

SILVER_LAKEHOUSE = "lh_logistics_silver"
SILVER_TABLES = [
    "silver_customers", "silver_drivers", "silver_trucks", "silver_trailers",
    "silver_routes", "silver_facilities", "silver_loads", "silver_trips",
    "silver_delivery_events", "silver_fuel_purchases",
    "silver_maintenance_records", "silver_safety_incidents",
]

sync = refresh_sql_endpoint_metadata(
    warehouse=SILVER_LAKEHOUSE, warehouse_type="Lakehouse"
)
display(sync)
if (sync["Status"] == "Failure").any():
    raise RuntimeError("Silver SQL endpoint sync reported a failed table; gold not loaded.")

# Same fingerprint on both sides: rows, deleted rows, latest change in epoch
# microseconds (timezone-neutral on both engines).
spark_fp = {
    t: tuple(
        spark.table(f"{SILVER}.{t}")
        .selectExpr(
            "COUNT(*)",
            "SUM(CAST(_is_deleted_in_source AS BIGINT))",
            "unix_micros(MAX(_source_changed_at))",
        )
        .first()
    )
    for t in SILVER_TABLES
}

endpoint_sql = "\nUNION ALL\n".join(
    f"""SELECT '{t}' AS table_name,
       COUNT_BIG(*) AS row_count,
       CAST(SUM(CASE WHEN _is_deleted_in_source = 1 THEN 1 ELSE 0 END) AS BIGINT) AS deleted_count,
       DATEDIFF_BIG(microsecond, '1970-01-01', MAX(_source_changed_at)) AS max_changed_us
FROM {SILVER_LAKEHOUSE}.dbo.{t}"""
    for t in SILVER_TABLES
)
endpoint_fp = {
    r["table_name"]: (r["row_count"], r["deleted_count"], r["max_changed_us"])
    for r in gold_query(endpoint_sql).collect()
}

stale = [
    f"{t}: spark={spark_fp[t]} endpoint={endpoint_fp.get(t)}"
    for t in SILVER_TABLES
    if spark_fp[t] != endpoint_fp.get(t)
]
if stale:
    raise RuntimeError(
        "SQL endpoint does not match silver Delta (rows, deleted, max_changed_us):\n"
        + "\n".join(stale)
    )
print(f"Silver SQL endpoint in sync for {len(SILVER_TABLES)} tables.")

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
# CELL 4: FACT DETECTORS
# ---------------------------------------------------------------------------
def silver_table(name):
    return spark.table(f"{SILVER}.{name}")
 
 
def heal_sql(fact, key, unknown_cols, scd2=(), orphans=()):
    """Gold-side healing: rows to reprocess although no silver table changed.
 
    unknown_cols  key columns where -1 means a lookup failed; retried on
                  active rows, because the dimension row may have arrived since.
    scd2          (dimension, key column, business id, fact date column) for
                  each Type 2 dimension: rows whose key is not the version
                  valid at the fact's own date.
    orphans       (dimension, key column) for every dimension key: rows whose
                  key has no dimension row, left behind by a rebuild.
    """
    parts = [f"""
        SELECT f.{key} FROM {fact} AS f
        WHERE f.is_deleted_in_source = 0
          AND -1 IN ({', '.join('f.' + c for c in unknown_cols)})"""]
 
    for dim, dim_key, dim_id, date_col in scd2:
        parts.append(f"""
        SELECT f.{key} FROM {fact} AS f
        JOIN {dim} AS d ON d.{dim_key} = f.{dim_key}
        JOIN {dim} AS c
          ON c.{dim_id} = d.{dim_id}
         AND f.{date_col} >= c.valid_from_date_key
         AND f.{date_col} <  c.valid_to_date_key
        WHERE f.{dim_key} > 0 AND c.{dim_key} <> f.{dim_key}""")
 
    if orphans:
        missing = "\n           OR ".join(
            f"NOT EXISTS (SELECT 1 FROM {dim} AS d WHERE d.{dim_key} = f.{dim_key})"
            for dim, dim_key in orphans)
        parts.append(f"""
        SELECT f.{key} FROM {fact} AS f
        WHERE {missing}""")
 
    return "\nUNION".join(parts)
 
 
def detect_fact(step, key, fact, own_table, sources, incremental, heal):
    """Changed keys for one fact, from every silver table the fact reads.
 
    sources      silver tables whose change feed can change the fact; each
                 gets its own checkpoint and batch row.
    incremental  function(changes by table) -> keys, used when every source
                 has a readable change window.
    A missing window on any source, or a forced step, means full
    reconciliation: every silver key plus every gold key, so keys that left
    silver reach the procedure and are soft-deleted.
    """
    reads = {t: changed_rows(step, t) for t in sources}
    forced = step in FORCED_FULL
    full_scan = forced or any(r[0] is None for r in reads.values())
 
    if full_scan:
        keys = (silver_table(own_table).select(key)
                .unionByName(gold_query(f"SELECT f.{key} FROM {fact} AS f")))
    else:
        keys = incremental({t: r[0] for t, r in reads.items()})
 
    keys = (keys.unionByName(gold_query(heal))
                .where(F.col(key).isNotNull())
                .distinct())
 
    batches = [
        {
            "step_name": step,
            "source_table": t,
            "version_from": r[1],
            "version_to": r[2],
            "is_full_scan": full_scan,
            "fallback_reason": r[3] or ("forced" if forced else None),
        }
        for t, r in reads.items()
    ]
    return keys, batches
 
 
# fact_trip: unchanged from the committed version.
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
 
 
# fact_delivery_event: the event itself, or its load (which carries the route).
def detect_fact_delivery_event(step):
    def incremental(ch):
        changed_loads = ch["silver_loads"].select("load_id").distinct()
        return (ch["silver_delivery_events"].select("event_id")
                .unionByName(silver_table("silver_delivery_events")
                             .join(changed_loads, "load_id").select("event_id")))
 
    fact = "fact.fact_delivery_event"
    return detect_fact(
        step, "event_id", fact, "silver_delivery_events",
        ["silver_delivery_events", "silver_loads"], incremental,
        heal_sql(fact, "event_id",
                 unknown_cols=["scheduled_date_key", "actual_date_key", "facility_key",
                               "route_key", "delivery_status_key"],
                 orphans=[("dim.dim_facility", "facility_key"),
                          ("dim.dim_route", "route_key"),
                          ("dim.dim_delivery_status", "delivery_status_key")]))
 
 
# fact_fuel_purchase: the purchase, its trip, or the trip's load.
def detect_fact_fuel_purchase(step):
    def incremental(ch):
        changed_loads = ch["silver_loads"].select("load_id").distinct()
        affected_trips = (ch["silver_trips"].select("trip_id")
                          .unionByName(silver_table("silver_trips")
                                       .join(changed_loads, "load_id").select("trip_id"))
                          .distinct())
        return (ch["silver_fuel_purchases"].select("fuel_purchase_id")
                .unionByName(silver_table("silver_fuel_purchases")
                             .join(affected_trips, "trip_id").select("fuel_purchase_id")))
 
    fact = "fact.fact_fuel_purchase"
    return detect_fact(
        step, "fuel_purchase_id", fact, "silver_fuel_purchases",
        ["silver_fuel_purchases", "silver_trips", "silver_loads"], incremental,
        heal_sql(fact, "fuel_purchase_id",
                 unknown_cols=["purchase_date_key", "truck_key", "driver_key",
                               "route_key", "location_key"],
                 scd2=[("dim.dim_truck", "truck_key", "truck_id", "purchase_date_key"),
                       ("dim.dim_driver", "driver_key", "driver_id", "purchase_date_key")],
                 orphans=[("dim.dim_truck", "truck_key"), ("dim.dim_driver", "driver_key"),
                          ("dim.dim_route", "route_key"), ("dim.dim_location", "location_key")]))
 
 
# fact_maintenance: the record only.
def detect_fact_maintenance(step):
    fact = "fact.fact_maintenance"
    return detect_fact(
        step, "maintenance_id", fact, "silver_maintenance_records",
        ["silver_maintenance_records"],
        lambda ch: ch["silver_maintenance_records"].select("maintenance_id"),
        heal_sql(fact, "maintenance_id",
                 unknown_cols=["maintenance_date_key", "truck_key", "location_key",
                               "maintenance_class_key"],
                 scd2=[("dim.dim_truck", "truck_key", "truck_id", "maintenance_date_key")],
                 orphans=[("dim.dim_truck", "truck_key"), ("dim.dim_location", "location_key"),
                          ("dim.dim_maintenance_class", "maintenance_class_key")]))
 
 
# fact_safety_incident: the incident only.
def detect_fact_safety_incident(step):
    fact = "fact.fact_safety_incident"
    return detect_fact(
        step, "incident_id", fact, "silver_safety_incidents",
        ["silver_safety_incidents"],
        lambda ch: ch["silver_safety_incidents"].select("incident_id"),
        heal_sql(fact, "incident_id",
                 unknown_cols=["incident_date_key", "driver_key", "truck_key",
                               "location_key", "incident_class_key"],
                 scd2=[("dim.dim_driver", "driver_key", "driver_id", "incident_date_key"),
                       ("dim.dim_truck", "truck_key", "truck_id", "incident_date_key")],
                 orphans=[("dim.dim_driver", "driver_key"), ("dim.dim_truck", "truck_key"),
                          ("dim.dim_location", "location_key"),
                          ("dim.dim_incident_class", "incident_class_key")]))
 
 
# Step name -> detector, worklist table, key column.
STEPS = {
    "fact_trip":            {"detect": detect_fact_trip,            "stg": "stg.changed_trip_keys",            "key": "trip_id"},
    "fact_delivery_event":  {"detect": detect_fact_delivery_event,  "stg": "stg.changed_delivery_event_keys",  "key": "event_id"},
    "fact_fuel_purchase":   {"detect": detect_fact_fuel_purchase,   "stg": "stg.changed_fuel_purchase_keys",   "key": "fuel_purchase_id"},
    "fact_maintenance":     {"detect": detect_fact_maintenance,     "stg": "stg.changed_maintenance_keys",     "key": "maintenance_id"},
    "fact_safety_incident": {"detect": detect_fact_safety_incident, "stg": "stg.changed_safety_incident_keys", "key": "incident_id"},
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# CELL 5: WRITE WORKLISTS + CDF LEDGER
# ---------------------------------------------------------------------------
batch_schema = StructType([
    StructField("run_id",       StringType(),    False),
    StructField("step_name",    StringType(),    False),
    StructField("source_table", StringType(),    False),
    StructField("version_from", LongType(),      True),
    StructField("version_to",   LongType(),      False),
    StructField("is_full_scan", BooleanType(),   False),
    StructField("detected_at",  TimestampType(), False),
])
 
requested = [s.strip() for s in steps.split(",") if s.strip()]
unknown_steps = [s for s in requested if s not in STEPS]
if unknown_steps:
    raise ValueError(f"Unknown step(s) {unknown_steps}. Known steps: {sorted(STEPS)}")
 
for step in requested:
    spec = STEPS[step]
    keys, batches = spec["detect"](step)
 
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
 
    key_frame = keys.select(
        F.lit(run_id).cast("string").alias("run_id"),
        F.col(spec["key"]).cast("string").alias(spec["key"]),
        F.lit(now).cast("timestamp").alias("detected_at"),
    )
 
    key_count = key_frame.count()
    if key_count:
        key_frame.write.mode("append").synapsesql(f"{WAREHOUSE}.{spec['stg']}")
 
    ledger_rows = [
        (run_id, b["step_name"], b["source_table"], b["version_from"],
         b["version_to"], b["is_full_scan"], now)
        for b in batches
    ]
    (spark.createDataFrame(ledger_rows, batch_schema)
          .write.mode("append")
          .synapsesql(f"{WAREHOUSE}.log.cdf_batch"))
 
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
print("\nWorklists for this run:")
for step in requested:
    spec = STEPS[step]
    n = gold_query(
        f"SELECT COUNT(*) AS n FROM {spec['stg']} AS k WHERE k.run_id = '{run_id}'"
    ).first()["n"]
    print(f"  {step:<22} {spec['stg']:<36} {n:,}")
 
print("\nCDF batches recorded:")
display(
    gold_query(f"""
        SELECT b.step_name, b.source_table, b.version_from, b.version_to, b.is_full_scan
        FROM log.cdf_batch AS b
        WHERE b.run_id = '{run_id}'""")
    .orderBy("step_name", "source_table")
)
 
print(
    "\nNext: run each step's procedure with the same run_id. A step's "
    "checkpoint is not trusted until its procedure logs 'succeeded'."
)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table, key in [("delivery_events", "event_id"), ("fuel_purchases", "fuel_purchase_id"),
                   ("maintenance_records", "maintenance_id"), ("safety_incidents", "incident_id")]:
    t = f"lh_logistics_silver.dbo.silver_{table}"
    spark.sql(f"""SELECT '{table}' AS tbl, {key}, _is_deleted_in_source, _source_changed_at,
                         _silver_updated_at, _silver_run_id
                  FROM {t} WHERE _is_deleted_in_source OR _is_deleted_in_source IS NULL""").show(truncate=False)
    (spark.sql(f"DESCRIBE HISTORY {t}")
          .selectExpr("version", "timestamp", "operation",
                      "operationMetrics['numTargetRowsUpdated'] AS updated")
          .show(6, truncate=False))

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
