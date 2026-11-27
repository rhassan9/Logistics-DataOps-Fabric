"""Silver routine: one metadata-driven path from a landing batch to the 12 silver tables.

Steps for an INITIAL_SNAPSHOT batch:
  1. validate the batch against its manifest (layout, files, schema, counts, keys)
  2. conform every table: the per-table transforms the silver build has always
     applied, the data quality status, and the lineage columns
  3. plan every table against current silver before anything is written:
       silver table empty                  -> load the batch rows
       every key and row hash equal        -> unchanged, nothing is written
       any difference                      -> the whole batch is rejected
  4. write only when no table is rejected, one append per empty table

A table is either empty or holds the complete batch (one Delta append is one
commit), so a run that fails part-way is retried by running it again: tables
already written plan as unchanged and the rest load.

This version applies INITIAL_SNAPSHOT batches. CHANGE_BATCH batches are refused.

The transforms are ports of nb_silver_transactional (six transactional tables)
and df_lh_logistics_bronze_to_silver (six reference tables), taken from their
running code, with parity tests on known rows in ingestion/tests.

The block between the marker lines is copied verbatim into nb_silver_apply_batch
and nb_silver_hash_baseline, because a Fabric notebook cannot import this file.
ingestion/tests/test_silver_routine.py fails if a copy differs. The block uses
the landing contract's names (TABLES, MODE_SNAPSHOT, file_columns and others),
which those notebooks also hold, so the module imports them here.
"""

from __future__ import annotations

from ingestion.landing_contract import (  # noqa: F401  (names the block uses)
    DATA_FILE_NAME,
    MANIFEST_NAME,
    MODE_CHANGE,
    MODE_SNAPSHOT,
    SNAPSHOT_SEQUENCE,
    TABLES,
    file_columns,
)

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


BEGIN_MARKER = "# >>> silver-routine:begin"
END_MARKER = "# <<< silver-routine:end"


def block_text(text):
    """The silver routine block of a file, markers included."""
    start = text.index(BEGIN_MARKER)
    end = text.index(END_MARKER, start) + len(END_MARKER)
    return text[start:end]
