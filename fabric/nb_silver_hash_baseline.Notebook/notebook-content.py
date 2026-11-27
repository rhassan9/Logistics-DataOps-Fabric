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

# Silver: per-key row hash baseline
#
# Reads    : lh_logistics_silver silver_* (every row, deleted rows included)
# Writes   : lh_logistics_ops silver_hash_baseline (capture only)
#
# ---------------------------------------------------------------------------
# WHAT THIS NOTEBOOK DOES
# ---------------------------------------------------------------------------
# action = "capture" stores one row per silver row of all 12 tables under a
# new label: the business key and a SHA-256 of the row's hashed columns. The
# hashed columns are every silver column except the lineage columns (where
# and when a row came from), so they cover the business and derived columns,
# _dq_status and _is_deleted_in_source. They are the same row hashes the
# silver routine's snapshot guard compares (ingestion/silver_routine.py).
#
# action = "compare" hashes current silver the same way and compares it with
# a captured label, per table: keys equal, keys whose hash changed, keys only
# in the baseline, keys only in silver, and keys repeated on either side.
# With fail_on_difference the run fails on any difference, after printing it.
#
# A label is written once, in one commit for all 12 tables; capturing an
# existing label is refused, so a baseline is never overwritten.
#
# Acceptance test A1 captures before batch 0 is applied and compares after.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# ===========================================================================
# CELL 1 - PARAMETERS
# ===========================================================================

action             = "compare"     # "capture" or "compare"
label              = ""            # required, e.g. "a1-before-batch0"
fail_on_difference = True          # compare only

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 2 - LANDING CONTRACT (verbatim copy of ingestion/landing_contract.py)
# ===========================================================================
# >>> landing-contract:begin
# Landing contract, version 1. Source of truth: ingestion/landing_contract.py.
# Copied verbatim into the batch 0 notebooks; a unit test keeps the copies equal.
import hashlib
import json
from datetime import date

CONTRACT_VERSION = "1"

MODE_SNAPSHOT = "INITIAL_SNAPSHOT"
MODE_CHANGE = "CHANGE_BATCH"
OPERATIONS = ("INSERT", "UPDATE", "DELETE")
SNAPSHOT_SEQUENCE = 0
MANIFEST_NAME = "_manifest.json"
DATA_FILE_NAME = "part-00000.parquet"

# Columns and types of sql/01_create_schema.sql, in source order, with the
# Spark types the bronze Delta tables hold. "key" is the table's primary key.
# "change_batches" marks the seven tables that appear in change batches.
TABLES = {
    "customers": {
        "key": "customer_id",
        "change_batches": True,
        "columns": [
            ("customer_id", "string"),
            ("customer_name", "string"),
            ("customer_type", "string"),
            ("credit_terms_days", "int"),
            ("primary_freight_type", "string"),
            ("account_status", "string"),
            ("contract_start_date", "date"),
            ("annual_revenue_potential", "decimal(14,2)"),
        ],
    },
    "facilities": {
        "key": "facility_id",
        "change_batches": False,
        "columns": [
            ("facility_id", "string"),
            ("facility_name", "string"),
            ("facility_type", "string"),
            ("city", "string"),
            ("state", "string"),
            ("latitude", "decimal(10,6)"),
            ("longitude", "decimal(10,6)"),
            ("dock_doors", "int"),
            ("operating_hours", "string"),
        ],
    },
    "drivers": {
        "key": "driver_id",
        "change_batches": True,
        "columns": [
            ("driver_id", "string"),
            ("first_name", "string"),
            ("last_name", "string"),
            ("hire_date", "date"),
            ("termination_date", "date"),
            ("license_number", "string"),
            ("license_state", "string"),
            ("date_of_birth", "date"),
            ("home_terminal", "string"),
            ("employment_status", "string"),
            ("cdl_class", "string"),
            ("years_experience", "int"),
        ],
    },
    "trucks": {
        "key": "truck_id",
        "change_batches": True,
        "columns": [
            ("truck_id", "string"),
            ("unit_number", "string"),
            ("make", "string"),
            ("model_year", "int"),
            ("vin", "string"),
            ("acquisition_date", "date"),
            ("acquisition_mileage", "int"),
            ("fuel_type", "string"),
            ("tank_capacity_gallons", "int"),
            ("status", "string"),
            ("home_terminal", "string"),
        ],
    },
    "trailers": {
        "key": "trailer_id",
        "change_batches": False,
        "columns": [
            ("trailer_id", "string"),
            ("trailer_number", "string"),
            ("trailer_type", "string"),
            ("length_feet", "int"),
            ("model_year", "int"),
            ("vin", "string"),
            ("acquisition_date", "date"),
            ("status", "string"),
            ("current_location", "string"),
        ],
    },
    "routes": {
        "key": "route_id",
        "change_batches": False,
        "columns": [
            ("route_id", "string"),
            ("origin_city", "string"),
            ("origin_state", "string"),
            ("destination_city", "string"),
            ("destination_state", "string"),
            ("typical_distance_miles", "int"),
            ("base_rate_per_mile", "decimal(8,4)"),
            ("fuel_surcharge_rate", "decimal(8,4)"),
            ("typical_transit_days", "int"),
        ],
    },
    "loads": {
        "key": "load_id",
        "change_batches": True,
        "columns": [
            ("load_id", "string"),
            ("customer_id", "string"),
            ("route_id", "string"),
            ("load_date", "date"),
            ("load_type", "string"),
            ("weight_lbs", "int"),
            ("pieces", "int"),
            ("revenue", "decimal(14,2)"),
            ("fuel_surcharge", "decimal(14,2)"),
            ("accessorial_charges", "decimal(14,2)"),
            ("load_status", "string"),
            ("booking_type", "string"),
        ],
    },
    "trips": {
        "key": "trip_id",
        "change_batches": True,
        "columns": [
            ("trip_id", "string"),
            ("load_id", "string"),
            ("driver_id", "string"),
            ("truck_id", "string"),
            ("trailer_id", "string"),
            ("dispatch_date", "date"),
            ("actual_distance_miles", "int"),
            ("actual_duration_hours", "decimal(10,2)"),
            ("fuel_gallons_used", "decimal(12,2)"),
            ("average_mpg", "decimal(8,2)"),
            ("idle_time_hours", "decimal(10,2)"),
            ("trip_status", "string"),
        ],
    },
    "fuel_purchases": {
        "key": "fuel_purchase_id",
        "change_batches": True,
        "columns": [
            ("fuel_purchase_id", "string"),
            ("trip_id", "string"),
            ("truck_id", "string"),
            ("driver_id", "string"),
            ("purchase_date", "timestamp"),
            ("location_city", "string"),
            ("location_state", "string"),
            ("gallons", "decimal(10,2)"),
            ("price_per_gallon", "decimal(10,3)"),
            ("total_cost", "decimal(14,2)"),
            ("fuel_card_number", "string"),
        ],
    },
    "maintenance_records": {
        "key": "maintenance_id",
        "change_batches": False,
        "columns": [
            ("maintenance_id", "string"),
            ("truck_id", "string"),
            ("maintenance_date", "date"),
            ("maintenance_type", "string"),
            ("odometer_reading", "int"),
            ("labor_hours", "decimal(8,2)"),
            ("labor_cost", "decimal(14,2)"),
            ("parts_cost", "decimal(14,2)"),
            ("total_cost", "decimal(14,2)"),
            ("facility_location", "string"),
            ("downtime_hours", "decimal(10,2)"),
            ("service_description", "string"),
        ],
    },
    "delivery_events": {
        "key": "event_id",
        "change_batches": True,
        "columns": [
            ("event_id", "string"),
            ("load_id", "string"),
            ("trip_id", "string"),
            ("event_type", "string"),
            ("facility_id", "string"),
            ("scheduled_datetime", "timestamp"),
            ("actual_datetime", "timestamp"),
            ("detention_minutes", "int"),
            ("on_time_flag", "boolean"),
            ("location_city", "string"),
            ("location_state", "string"),
        ],
    },
    "safety_incidents": {
        "key": "incident_id",
        "change_batches": False,
        "columns": [
            ("incident_id", "string"),
            ("trip_id", "string"),
            ("truck_id", "string"),
            ("driver_id", "string"),
            ("incident_date", "timestamp"),
            ("incident_type", "string"),
            ("location_city", "string"),
            ("location_state", "string"),
            ("at_fault_flag", "boolean"),
            ("injury_flag", "boolean"),
            ("vehicle_damage_cost", "decimal(14,2)"),
            ("cargo_damage_cost", "decimal(14,2)"),
            ("claim_amount", "decimal(14,2)"),
            ("preventable_flag", "boolean"),
            ("description", "string"),
        ],
    },
}

# Change metadata, appended after the source columns in every CHANGE_BATCH file.
# Underscore-prefixed because delivery_events.event_id is a source primary key.
METADATA_COLUMNS = [
    ("_operation", "string"),
    ("_event_id", "string"),
    ("_source_sequence", "bigint"),
    ("_source_changed_at", "timestamp"),
    ("_batch_id", "string"),
]

CHANGE_TABLES = [name for name, spec in TABLES.items() if spec["change_batches"]]


def batch_folder(batch_seq):
    """Folder name of one batch under landing/logistics."""
    return f"batch_seq={batch_seq:06d}"


def file_columns(table, mode):
    """(name, Spark type) pairs a landing file of this table and mode must hold, in order."""
    columns = list(TABLES[table]["columns"])
    if mode == MODE_CHANGE:
        columns += METADATA_COLUMNS
    return columns


def content_hash(manifest_tables):
    """SHA-256 over every (table, path, file sha256) in the batch, sorted.

    Identifies the batch content independently of manifest formatting, so the
    same files always give the same hash.
    """
    entries = sorted(
        (table, entry["path"], entry["sha256"])
        for table, spec in manifest_tables.items()
        for entry in spec["files"]
    )
    canonical = json.dumps(entries, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _is_iso_date(value):
    if not isinstance(value, str):
        return False
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return len(value) == 10


def _is_sha256(value):
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def manifest_problems(manifest):
    """Every way a manifest breaks the contract, as text. An empty list means valid.

    Checks structure only. Files, row counts and hashes against the data are
    checked by whoever reads the batch.
    """
    problems = []
    required = ["contract_version", "batch_id", "batch_seq", "mode", "business_date",
                "as_of_date", "previous_batch_id", "supersedes_batch_id", "producer",
                "content_hash", "tables"]
    missing = [k for k in required if k not in manifest]
    if missing:
        return [f"missing keys: {', '.join(missing)}"]
    extra = sorted(set(manifest) - set(required))
    if extra:
        problems.append(f"unexpected keys: {', '.join(extra)}")

    if manifest["contract_version"] != CONTRACT_VERSION:
        problems.append(f"contract_version {manifest['contract_version']!r}, expected {CONTRACT_VERSION!r}")
    if not isinstance(manifest["batch_id"], str) or not manifest["batch_id"]:
        problems.append("batch_id must be a non-empty string")
    if not isinstance(manifest["producer"], dict) or not manifest["producer"].get("name"):
        problems.append("producer must be an object with a name")
    if manifest["supersedes_batch_id"] is not None and not isinstance(manifest["supersedes_batch_id"], str):
        problems.append("supersedes_batch_id must be null or a string")

    seq, mode, tables = manifest["batch_seq"], manifest["mode"], manifest["tables"]
    if not _is_int(seq) or seq < 0:
        return problems + ["batch_seq must be a non-negative integer"]
    if not isinstance(tables, dict) or not tables:
        return problems + ["tables must be a non-empty object"]

    if mode == MODE_SNAPSHOT:
        if seq != 0:
            problems.append("INITIAL_SNAPSHOT must be batch_seq 0")
        if not _is_iso_date(manifest["as_of_date"]):
            problems.append("INITIAL_SNAPSHOT needs as_of_date as YYYY-MM-DD")
        if manifest["business_date"] is not None:
            problems.append("INITIAL_SNAPSHOT must have business_date null")
        if manifest["previous_batch_id"] is not None:
            problems.append("INITIAL_SNAPSHOT must have previous_batch_id null")
        if set(tables) != set(TABLES):
            problems.append(f"INITIAL_SNAPSHOT must list all {len(TABLES)} tables; "
                            f"missing {sorted(set(TABLES) - set(tables))}, extra {sorted(set(tables) - set(TABLES))}")
    elif mode == MODE_CHANGE:
        if seq < 1:
            problems.append("CHANGE_BATCH must have batch_seq 1 or higher")
        if not _is_iso_date(manifest["business_date"]):
            problems.append("CHANGE_BATCH needs business_date as YYYY-MM-DD")
        if manifest["as_of_date"] is not None:
            problems.append("CHANGE_BATCH must have as_of_date null")
        if not isinstance(manifest["previous_batch_id"], str) or not manifest["previous_batch_id"]:
            problems.append("CHANGE_BATCH needs previous_batch_id")
        unknown = sorted(set(tables) - set(CHANGE_TABLES))
        if unknown:
            problems.append(f"CHANGE_BATCH lists tables without change batches: {unknown}")
    else:
        return problems + [f"mode {mode!r} is not {MODE_SNAPSHOT} or {MODE_CHANGE}"]

    for table, spec in sorted(tables.items()):
        where = f"tables.{table}"
        if not isinstance(spec, dict):
            problems.append(f"{where} must be an object")
            continue
        keys = {"files", "row_count", "min_sequence", "max_sequence", "operation_counts"}
        if set(spec) != keys:
            problems.append(f"{where} keys must be {sorted(keys)}")
            continue
        files = spec["files"]
        if not isinstance(files, list) or not files:
            problems.append(f"{where}.files must be a non-empty list")
            files = []
        for entry in files:
            if not isinstance(entry, dict) or set(entry) != {"path", "bytes", "sha256"}:
                problems.append(f"{where}.files entries need exactly path, bytes, sha256")
                continue
            path = entry["path"]
            if not (isinstance(path, str) and path.startswith(f"{table}/") and path.endswith(".parquet")
                    and "/" not in path[len(table) + 1:]):
                problems.append(f"{where}: file path {path!r} must be {table}/<name>.parquet")
            if not _is_int(entry["bytes"]) or entry["bytes"] <= 0:
                problems.append(f"{where}: file {path!r} bytes must be a positive integer")
            if not _is_sha256(entry["sha256"]):
                problems.append(f"{where}: file {path!r} sha256 must be 64 lowercase hex characters")
        rows = spec["row_count"]
        if not _is_int(rows) or rows < 0:
            problems.append(f"{where}.row_count must be a non-negative integer")
            continue
        low, high, ops = spec["min_sequence"], spec["max_sequence"], spec["operation_counts"]
        if mode == MODE_SNAPSHOT:
            if low != SNAPSHOT_SEQUENCE or high != SNAPSHOT_SEQUENCE:
                problems.append(f"{where}: snapshot sequences must be {SNAPSHOT_SEQUENCE}")
            if ops is not None:
                problems.append(f"{where}: snapshot operation_counts must be null")
            if rows == 0:
                problems.append(f"{where}: snapshot table has no rows")
        else:
            if rows == 0:
                problems.append(f"{where}: a change batch lists only tables with changes")
            if not (_is_int(low) and _is_int(high) and 1 <= low <= high):
                problems.append(f"{where}: sequences must be integers with 1 <= min <= max")
            elif high - low + 1 < rows:
                problems.append(f"{where}: {rows} rows cannot have unique sequences in [{low}, {high}]")
            if not isinstance(ops, dict) or set(ops) != set(OPERATIONS):
                problems.append(f"{where}: operation_counts must hold exactly {list(OPERATIONS)}")
            elif not all(_is_int(v) and v >= 0 for v in ops.values()) or sum(ops.values()) != rows:
                problems.append(f"{where}: operation_counts must be non-negative and sum to row_count")

    if not problems:
        expected = content_hash(tables)
        if manifest["content_hash"] != expected:
            problems.append(f"content_hash {manifest['content_hash']!r} does not match the files ({expected})")
    return problems


def manifest_json(manifest):
    """Canonical manifest text: sorted keys, two-space indent, trailing newline."""
    return json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
# <<< landing-contract:end

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 3 - SILVER ROUTINE (verbatim copy of ingestion/silver_routine.py)
# ===========================================================================
# >>> silver-routine:begin
# Silver routine, version 1. Source of truth: ingestion/silver_routine.py.
# Copied verbatim into nb_silver_apply_batch and nb_silver_hash_baseline; a unit
# test keeps the copies equal. Uses the names of the landing contract block.
from pyspark.sql import functions as F

ROUTINE_VERSION = "1"

SILVER_PREFIX = "silver_"

# Derived ages and tenures are measured to this fixed date, as the silver build
# always has: facts end on 2024-12-31, and an age measured to today would grow
# on every run without anything having happened.
DERIVED_AS_OF = "2024-12-31"

# Freight convention: two hours of appointment tolerance, and two hours of free
# detention before the shipper starts paying.
ON_TIME_WINDOW_MINUTES = 120
FREE_DETENTION_MINUTES = 120

# Conformance and write order. fuel_purchases needs the trucks' tank capacities.
APPLY_ORDER = (
    "customers", "facilities", "drivers", "trucks", "trailers", "routes",
    "loads", "trips", "delivery_events", "maintenance_records",
    "safety_incidents", "fuel_purchases",
)

# Silver columns that say where and when a row came from, not what it states.
# Row hashes leave them out, so one business state always has one hash.
LINEAGE_COLUMNS = (
    "_source_ingest_date", "_silver_created_at", "_silver_updated_at",
    "_silver_run_id", "_source_changed_at", "_source_batch_id", "_source_sequence",
)

# Stamped by the writer at write time, not by the routine.
WRITE_STAMPED = ("_silver_created_at", "_silver_updated_at")

PLAN_LOAD = "load"
PLAN_UNCHANGED = "unchanged"
PLAN_REJECT = "reject"

# Microseconds and an explicit zone, so no timestamp difference is lost to the
# default millisecond format; nulls are written out rather than dropped.
HASH_JSON_OPTIONS = {
    "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX",
    "dateFormat": "yyyy-MM-dd",
    "ignoreNullFields": "false",
}

UTC_ZONES = ("UTC", "Etc/UTC", "GMT", "Z", "+00:00")


def require_utc(spark):
    """Refuse to run unless the Spark session time zone is UTC.

    Timestamps in row hashes and the as-of time are rendered in the session
    zone, so hashes taken in two sessions agree only when both are UTC.
    """
    zone = spark.conf.get("spark.sql.session.timeZone")
    if zone not in UTC_ZONES:
        raise RuntimeError(f"spark.sql.session.timeZone is {zone!r}; set it to 'UTC' first")


def silver_name(table):
    return f"{SILVER_PREFIX}{table}"


# --- transforms -------------------------------------------------------------
# Each takes the table's source columns and returns (frame, flagged), where
# flagged is a boolean column: true sets _dq_status to 'flagged'. A null counts
# as not flagged.

def transform_customers(df, lookups):
    """Conformance only."""
    return df, F.lit(False)


def transform_facilities(df, lookups):
    """Coordinates are city centroids, so every row says so."""
    return df.withColumn("is_centroid_coordinate", F.lit(True)), F.lit(False)


def transform_drivers(df, lookups):
    """Tenure to the termination date or the as-of date, age at hire, and a flag
    for a termination date before the hire date."""
    as_of = F.lit(DERIVED_AS_OF).cast("date")
    out = (
        df.withColumn("tenure_days", F.datediff(F.coalesce(F.col("termination_date"), as_of), F.col("hire_date")))
          .withColumn("age_at_hire", F.year("hire_date") - F.year("date_of_birth"))
    )
    flagged = F.col("termination_date").isNotNull() & (F.col("termination_date") < F.col("hire_date"))
    return out, flagged


def transform_trucks(df, lookups):
    """Age in whole years at the as-of year."""
    return (df.withColumn("truck_age_years_as_of", F.lit(int(DERIVED_AS_OF[:4])) - F.col("model_year")),
            F.lit(False))


def transform_trailers(df, lookups):
    """Age in whole years at the as-of year."""
    return (df.withColumn("trailer_age_years_as_of", F.lit(int(DERIVED_AS_OF[:4])) - F.col("model_year")),
            F.lit(False))


def transform_routes(df, lookups):
    """Route name from origin and destination, joined by the arrow (U+2192) the
    silver build writes."""
    name = F.concat(F.col("origin_city"), F.lit(" → "), F.col("destination_city"))
    return df.withColumn("route_name", name), F.lit(False)


def transform_loads(df, lookups):
    """Conformance only."""
    return df, F.lit(False)


def transform_trips(df, lookups):
    """Flag trips reporting more idle time than elapsed time."""
    out = df.withColumn("is_idle_implausible", F.col("idle_time_hours") > F.col("actual_duration_hours"))
    return out, F.col("is_idle_implausible")


def transform_delivery_events(df, lookups):
    """Arrival variance and status, billable detention, and a load-level flag for
    a delivery timestamped before its pickup.

    The frame holds every live event of the table: the reversal flag compares
    a load's pickup and delivery events with each other.
    """
    # Full timestamp precision: whole seconds misclassify events within a
    # second of the 120-minute window boundary.
    variance = (F.col("actual_datetime").cast("double") - F.col("scheduled_datetime").cast("double")) / 60
    reversed_loads = (
        df.groupBy("load_id")
          .agg(F.max(F.when(F.col("event_type") == "Pickup", F.col("actual_datetime"))).alias("_picked"),
               F.max(F.when(F.col("event_type") == "Delivery", F.col("actual_datetime"))).alias("_delivered"))
          .withColumn("is_timestamp_reversed",
                      F.coalesce(F.col("_delivered") < F.col("_picked"), F.lit(False)))
          .select("load_id", "is_timestamp_reversed")
    )
    out = (
        df.withColumn("arrival_variance_minutes", F.round(variance, 1).cast("decimal(10,1)"))
          .withColumn("arrival_status",
                      F.when(variance <= -ON_TIME_WINDOW_MINUTES, "Early")
                       .when(variance >= ON_TIME_WINDOW_MINUTES, "Late")
                       .otherwise("On Time"))
          .withColumn("billable_detention_minutes",
                      F.greatest(F.col("detention_minutes") - F.lit(FREE_DETENTION_MINUTES), F.lit(0)).cast("int"))
          .join(reversed_loads, on="load_id", how="left")
    )
    return out, F.col("is_timestamp_reversed")


def transform_maintenance_records(df, lookups):
    """Keep the urgency word of the templated service description; drop the rest."""
    out = (df.withColumn("service_urgency", F.split(F.col("service_description"), " ")[0])
             .drop("service_description"))
    return out, F.lit(False)


def transform_safety_incidents(df, lookups):
    """Split the templated description, categorise the incident type, and drop
    location_state (city and state were assigned independently)."""
    out = (
        df.withColumn("incident_severity", F.split(F.col("description"), " ")[0])
          .withColumn("incident_cause", F.regexp_extract(F.col("description"), r"involving (.+)$", 1))
          .withColumn("incident_category",
                      F.when(F.col("incident_type").isin("Accident", "Moving Violation"), "Safety")
                       .when(F.col("incident_type") == "DOT Violation", "Regulatory")
                       .when(F.col("incident_type") == "Equipment Damage", "Asset")
                       .when(F.col("incident_type") == "Customer Complaint", "Service")
                       .otherwise("Other"))
          .drop("description", "location_state")
    )
    return out, F.lit(False)


def transform_fuel_purchases(df, lookups):
    """Flag fills larger than the truck's tank, and drop location_state.

    lookups["truck_tanks"] holds truck_id and tank_capacity_gallons.
    """
    tanks = lookups["truck_tanks"].select("truck_id", "tank_capacity_gallons").distinct()
    out = (
        df.join(tanks, on="truck_id", how="left")
          .withColumn("is_capacity_exceeded",
                      F.coalesce(F.col("gallons") > F.col("tank_capacity_gallons"), F.lit(False)))
          .drop("tank_capacity_gallons", "location_state")
    )
    return out, F.col("is_capacity_exceeded")


TRANSFORMS = {
    "customers": transform_customers,
    "facilities": transform_facilities,
    "drivers": transform_drivers,
    "trucks": transform_trucks,
    "trailers": transform_trailers,
    "routes": transform_routes,
    "loads": transform_loads,
    "trips": transform_trips,
    "delivery_events": transform_delivery_events,
    "maintenance_records": transform_maintenance_records,
    "safety_incidents": transform_safety_incidents,
    "fuel_purchases": transform_fuel_purchases,
}


# --- validation ---------------------------------------------------------------

PARQUET_MAGIC = b"PAR1"


def data_read_problem(content, listed_size):
    """Why a read of a Parquet file is not complete, or None when it is.

    A read right after a write can return part of the file or nothing, so a
    read counts only when its length equals the listed size and it starts and
    ends with the Parquet magic bytes. A short read says nothing about the
    batch: the reader retries, and gives up with an error, not a rejection. A
    read of the full length that still lacks the magic bytes after every retry
    is a damaged file, which the sha256 against the manifest then rejects.
    """
    if len(content) != listed_size:
        return f"read {len(content):,} bytes, listed size {listed_size:,}"
    if content[:4] != PARQUET_MAGIC or content[-4:] != PARQUET_MAGIC:
        return f"starts {content[:4]!r}, ends {content[-4:]!r}, expected {PARQUET_MAGIC!r} at both ends"
    return None


def layout_problems(manifest, batch_entries, table_entries):
    """Folder layout against the manifest.

    batch_entries: names listed in the batch folder.
    table_entries: {table: {file name: listed size}} for each table folder present.
    """
    problems = []
    expected = set(manifest["tables"]) | {MANIFEST_NAME}
    present = set(batch_entries)
    if present != expected:
        problems.append(f"batch folder: missing {sorted(expected - present)}, extra {sorted(present - expected)}")
    for table, spec in sorted(manifest["tables"].items()):
        listed = {entry["path"].split("/", 1)[1]: entry["bytes"] for entry in spec["files"]}
        found = table_entries.get(table, {})
        if set(found) != set(listed):
            problems.append(f"{table}: files present {sorted(found)}, listed {sorted(listed)}")
            continue
        for name, size in sorted(listed.items()):
            if found[name] != size:
                problems.append(f"{table}/{name}: {found[name]} bytes listed by storage, manifest {size}")
    return problems


def file_problems(manifest, table, digests):
    """Files of one table against the manifest. digests: {file name: (bytes, sha256)},
    or {file name: error text} for a file that never read completely."""
    problems = []
    for entry in manifest["tables"][table]["files"]:
        name = entry["path"].split("/", 1)[1]
        got = digests.get(name)
        if got is None:
            problems.append(f"{table}/{name}: not read")
        elif isinstance(got, str):
            problems.append(f"{table}/{name}: {got}")
        else:
            size, digest = got
            if size != entry["bytes"]:
                problems.append(f"{table}/{name}: {size} bytes read, manifest {entry['bytes']}")
            if digest != entry["sha256"]:
                problems.append(f"{table}/{name}: sha256 {digest[:12]}, manifest {entry['sha256'][:12]}")
    return problems


def frame_problems(table, df, manifest, mode):
    """Schema, row count and keys of one table's batch rows against the contract
    and the manifest."""
    expected = file_columns(table, mode)
    actual = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
    if actual != expected:
        return [f"{table}: columns {actual} do not equal the contract {expected}"]
    key = TABLES[table]["key"]
    stats = df.agg(F.count(F.lit(1)).alias("rows"),
                   F.count_if(F.col(key).isNull()).alias("null_keys"),
                   F.countDistinct(F.col(key)).alias("keys")).first()
    problems = []
    declared = manifest["tables"][table]["row_count"]
    if stats["rows"] != declared:
        problems.append(f"{table}: {stats['rows']} rows read, manifest {declared}")
    if stats["null_keys"]:
        problems.append(f"{table}: {stats['null_keys']} null {key}")
    duplicates = stats["rows"] - stats["null_keys"] - stats["keys"]
    if mode == MODE_SNAPSHOT and duplicates:
        problems.append(f"{table}: {duplicates} rows repeat a {key} already present")
    return problems


# --- conformance ---------------------------------------------------------------

def conform(df, table, target_schema):
    """Project onto the silver table's columns and types, failing on any mismatch.

    Write-stamped columns are excluded: the writer supplies them.
    """
    wanted = [f for f in target_schema.fields if f.name not in WRITE_STAMPED]
    names = {f.name for f in wanted}
    missing, extra = sorted(names - set(df.columns)), sorted(set(df.columns) - names)
    if missing or extra:
        raise ValueError(f"{silver_name(table)}: missing {missing or 'none'}; unexpected {extra or 'none'}")
    return df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in wanted])


def snapshot_silver_rows(frames, manifest, target_schemas, run_id, source_ingest_date):
    """{table: silver rows} for an INITIAL_SNAPSHOT batch.

    frames: {table: batch rows, source columns}. target_schemas: {table: the
    silver table's StructType}. Every row is live, dated by the manifest's
    as-of date at 00:00 UTC, and carries the batch id and sequence 0.
    """
    if manifest["mode"] != MODE_SNAPSHOT:
        raise ValueError(f"mode {manifest['mode']} is not applied by silver routine version {ROUTINE_VERSION}")
    as_of = F.to_timestamp(F.lit(manifest["as_of_date"]))
    lookups = {"truck_tanks": frames["trucks"]}
    rows = {}
    for table in APPLY_ORDER:
        df, flagged = TRANSFORMS[table](frames[table], lookups)
        df = (
            df.withColumn("_dq_status",
                          F.when(F.coalesce(flagged, F.lit(False)), F.lit("flagged")).otherwise(F.lit("valid")))
              .withColumn("_is_deleted_in_source", F.lit(False))
              .withColumn("_source_changed_at", as_of)
              .withColumn("_source_ingest_date", F.lit(source_ingest_date))
              .withColumn("_silver_run_id", F.lit(run_id))
              .withColumn("_source_batch_id", F.lit(manifest["batch_id"]))
              .withColumn("_source_sequence", F.lit(SNAPSHOT_SEQUENCE).cast("bigint"))
        )
        rows[table] = conform(df, table, target_schemas[table])
    return rows


# --- row hashes and the snapshot guard -------------------------------------------

def hash_columns(target_schema):
    """Columns a row hash covers: every silver column except the lineage columns."""
    return [f.name for f in target_schema.fields if f.name not in LINEAGE_COLUMNS]


def row_hashes(df, table, columns):
    """business_key and a SHA-256 of the row's hashed columns, as JSON."""
    key = TABLES[table]["key"]
    payload = F.to_json(F.struct(*[F.col(c) for c in columns]), HASH_JSON_OPTIONS)
    return df.select(F.col(key).cast("string").alias("business_key"), F.sha2(payload, 256).alias("row_hash"))


def compare_hashes(left, right, sample=5):
    """Compare two (business_key, row_hash) frames.

    Returns counts of keys equal, changed, only in left, only in right, keys
    repeated on either side, and up to `sample` example keys of each difference.
    """
    lh = left.withColumnRenamed("row_hash", "left_hash")
    rh = right.withColumnRenamed("row_hash", "right_hash")
    joined = lh.join(rh, "business_key", "full_outer")
    both = F.col("left_hash").isNotNull() & F.col("right_hash").isNotNull()
    stats = joined.agg(
        F.count_if(both & (F.col("left_hash") == F.col("right_hash"))).alias("equal"),
        F.count_if(both & (F.col("left_hash") != F.col("right_hash"))).alias("changed"),
        F.count_if(F.col("right_hash").isNull()).alias("only_left"),
        F.count_if(F.col("left_hash").isNull()).alias("only_right"),
    ).first().asDict()
    stats["repeated_keys"] = sum(
        side.groupBy("business_key").count().filter(F.col("count") > 1).count() for side in (left, right))
    examples = {}
    for label, condition in (("changed", both & (F.col("left_hash") != F.col("right_hash"))),
                             ("only_left", F.col("right_hash").isNull()),
                             ("only_right", F.col("left_hash").isNull())):
        if stats[label]:
            examples[label] = sorted(r["business_key"] for r in
                                     joined.filter(condition).select("business_key").limit(sample).collect())
    stats["examples"] = examples
    return stats


def snapshot_plan(table, batch_rows, silver_rows, target_schema):
    """Decide what one table needs before anything is written.

    Empty silver table: load the batch rows. Otherwise every key and row hash
    must be equal (unchanged); any difference rejects the batch, because a
    snapshot that disagrees with silver cannot say which side is right.
    """
    if silver_rows.limit(1).count() == 0:
        return {"table": table, "decision": PLAN_LOAD}
    stats = compare_hashes(row_hashes(batch_rows, table, hash_columns(target_schema)),
                           row_hashes(silver_rows, table, hash_columns(target_schema)))
    differs = stats["changed"] or stats["only_left"] or stats["only_right"] or stats["repeated_keys"]
    sides = {"changed": "changed", "only_left": "only_batch", "only_right": "only_silver"}
    return {
        "table": table,
        "decision": PLAN_REJECT if differs else PLAN_UNCHANGED,
        "equal": stats["equal"],
        "changed": stats["changed"],
        "only_batch": stats["only_left"],
        "only_silver": stats["only_right"],
        "repeated_keys": stats["repeated_keys"],
        "examples": {sides[k]: v for k, v in stats["examples"].items()},
    }


def apply_snapshot(plans, rows, write):
    """Write the tables planned as load, in order, only if no table is rejected.

    write(table, rows) appends one table and returns the rows written.
    Returns {"outcome": "rejected" | "applied" | "unchanged", "written": {table: rows}}.
    """
    missing = [t for t in APPLY_ORDER if t not in plans]
    if missing:
        raise ValueError(f"no plan for {missing}")
    rejected = [t for t in APPLY_ORDER if plans[t]["decision"] == PLAN_REJECT]
    if rejected:
        return {"outcome": "rejected", "written": {}, "rejected": rejected}
    written = {}
    for table in APPLY_ORDER:
        if plans[table]["decision"] == PLAN_LOAD:
            written[table] = write(table, rows[table])
    return {"outcome": "applied" if written else "unchanged", "written": written, "rejected": []}
# <<< silver-routine:end

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 4 - CONFIGURATION
# ===========================================================================
spark.conf.set("spark.sql.session.timeZone", "UTC")
require_utc(spark)

SILVER = "lh_logistics_silver.dbo"
BASELINE = "lh_logistics_ops.dbo.silver_hash_baseline"

if action not in ("capture", "compare"):
    raise ValueError(f"action must be 'capture' or 'compare', not {action!r}")
if not label:
    raise ValueError("label is required")


def current_hashes(table):
    df = spark.table(f"{SILVER}.{silver_name(table)}")
    return row_hashes(df, table, hash_columns(df.schema))


def stored_labels():
    if not spark.catalog.tableExists(BASELINE):
        return set()
    return {r["label"] for r in spark.table(BASELINE).select("label").distinct().collect()}


print(f"action={action} label={label} routine version {ROUTINE_VERSION}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 5 - CAPTURE OR COMPARE
# ===========================================================================

if action == "capture":
    if label in stored_labels():
        raise ValueError(f"label {label!r} is already captured; choose a new label")
    frames = [current_hashes(t).select(F.lit(label).alias("label"), F.lit(t).alias("table_name"),
                                       "business_key", "row_hash")
              for t in APPLY_ORDER]
    capture = frames[0]
    for frame in frames[1:]:
        capture = capture.unionByName(frame)
    (capture.withColumn("captured_at", F.current_timestamp())
            .withColumn("routine_version", F.lit(ROUTINE_VERSION))
            .write.format("delta").mode("append").saveAsTable(BASELINE))
    counts = {r["table_name"]: r["count"] for r in
              spark.table(BASELINE).filter(F.col("label") == label).groupBy("table_name").count().collect()}
    for t in APPLY_ORDER:
        print(f"  {t:<22}{counts.get(t, 0):>9,} rows captured")
    print(f"captured {sum(counts.values()):,} row hashes under {label!r}")
    result = {"action": action, "label": label, "rows": counts}

else:
    if label not in stored_labels():
        raise ValueError(f"label {label!r} has not been captured")
    stored = spark.table(BASELINE).filter(F.col("label") == label)
    report, differing = {}, []
    print(f"{'table':<22}{'equal':>10}{'changed':>9}{'only base':>11}{'only silver':>13}{'repeated':>10}")
    for t in APPLY_ORDER:
        before = stored.filter(F.col("table_name") == t).select("business_key", "row_hash")
        stats = compare_hashes(before, current_hashes(t))
        report[t] = stats
        print(f"{t:<22}{stats['equal']:>10,}{stats['changed']:>9,}{stats['only_left']:>11,}"
              f"{stats['only_right']:>13,}{stats['repeated_keys']:>10,}"
              + (f"   examples {stats['examples']}" if stats["examples"] else ""))
        if stats["changed"] or stats["only_left"] or stats["only_right"] or stats["repeated_keys"]:
            differing.append(t)
    print("silver equals the baseline" if not differing else f"differences in: {', '.join(differing)}")
    result = {"action": action, "label": label, "differing_tables": differing,
              "equal_keys": {t: report[t]["equal"] for t in APPLY_ORDER}}
    if differing and fail_on_difference:
        raise AssertionError(f"silver differs from baseline {label!r} in: {', '.join(differing)}")

notebookutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
