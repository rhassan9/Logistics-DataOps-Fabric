# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f34bc028-a89a-46c4-a3ef-6b69a368f9c6",
# META       "default_lakehouse_name": "lh_logistics_ops",
# META       "default_lakehouse_workspace_id": "a77071e4-bd2a-4979-8910-91ddb8cd2a09",
# META       "known_lakehouses": [
# META         {
# META           "id": "f34bc028-a89a-46c4-a3ef-6b69a368f9c6"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Landing batch 0 export
#
# Source      : lh_logistics_bronze, the twelve bronze_* Delta tables
# Destination : Files/landing_staging/batch_seq=000000 in lh_logistics_ops
# Cadence     : once. The working account then uploads the folder to
#               stlogisticslake/landing/logistics with AzCopy (last cell).
#
# ---------------------------------------------------------------------------
# WHAT BATCH 0 IS
# ---------------------------------------------------------------------------
# The INITIAL_SNAPSHOT of the landing contract (ingestion/landing_contract.py):
# all twelve tables as of 2024-12-31, source columns only, no change metadata,
# one Parquet file per table, and _manifest.json written last. A batch folder
# without a manifest does not exist for any reader.
#
# Bronze is append-only, so a key can hold several versions. The export keeps
# what silver keeps: the newest version per key by _ingested_at, then
# _ingest_date (the order nb_silver_transactional uses), and only keys present
# in the latest full-mode slice. A key missing from that slice no longer
# exists at the source and is not part of a snapshot.
#
# Types are not converted. Every bronze column must already hold the contract
# type, or the notebook stops before writing anything.
#
# Timestamps are written as Parquet TIMESTAMP_MICROS rather than Spark's
# INT96 default, so the generator and any other reader get a standard,
# UTC-adjusted microsecond timestamp. The session time zone must be UTC so
# that the instants written are the source wall-clock values.
#
# Nothing in the staging folder is ever overwritten. To export again, delete
# the folder first; this notebook refuses to run when it exists.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# ===========================================================================
# CELL 1 - PARAMETERS
# ===========================================================================

as_of_date   = "2024-12-31"           # contract as-of date of batch 0
batch_id     = "logistics-000000"     # batch 0 identifier
staging_root = "Files/landing_staging"

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
# CELL 3 - CONFIGURATION AND PRECONDITIONS
# ===========================================================================

from datetime import datetime, timezone
import hashlib

from pyspark.sql import functions as F, Window

BRONZE_TABLE = "lh_logistics_bronze.dbo.bronze_{}"
LINEAGE = ["_ingest_date", "_ingested_at", "_source_system", "_load_mode"]

# Source row counts, as reconciled by nb_bronze_ingest_neon (CELL 7).
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

BATCH_DIR = f"{staging_root}/{batch_folder(0)}"

session_tz = spark.conf.get("spark.sql.session.timeZone")
if session_tz not in ("UTC", "Etc/UTC", "+00:00", "Z"):
    raise RuntimeError(f"Session time zone is {session_tz}; set it to UTC before exporting.")
spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

if notebookutils.fs.exists(BATCH_DIR):
    raise RuntimeError(f"{BATCH_DIR} already exists. Batch folders are never overwritten; "
                       "delete it first if this export must run again.")
if set(SOURCE_COUNTS) != set(TABLES):
    raise RuntimeError("SOURCE_COUNTS must list exactly the contract tables")

print(f"session time zone : {session_tz}")
print(f"target            : {BATCH_DIR}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 4 - BRONZE SCHEMAS AGAINST THE CONTRACT (fails before any write)
# ===========================================================================

def bronze_columns(table):
    """(name, Spark type) of a bronze table, lineage columns removed, in table order."""
    schema = spark.table(BRONZE_TABLE.format(table)).schema
    names = [f.name for f in schema.fields]
    missing = [c for c in LINEAGE if c not in names]
    if missing:
        raise RuntimeError(f"bronze_{table} lacks lineage columns {missing}")
    return [(f.name, f.dataType.simpleString()) for f in schema.fields if f.name not in LINEAGE]


schema_problems = []
for table, spec in TABLES.items():
    actual = bronze_columns(table)
    if actual != spec["columns"]:
        schema_problems.append(f"{table}: bronze {actual} != contract {spec['columns']}")

if schema_problems:
    raise RuntimeError("Bronze does not match the contract:\n" + "\n".join(schema_problems))
print(f"{len(TABLES)} bronze schemas match the contract")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 5 - CURRENT STATE PER KEY, AND CHECKS BEFORE WRITING
# ===========================================================================

def current_snapshot(table):
    """Bronze reduced to the live current row per key, contract columns only.

    Same order as silver's current_state: newest _ingested_at, then newest
    _ingest_date. A key whose newest row predates the latest full-mode slice
    was absent from that slice, so it no longer exists at the source.
    """
    key = TABLES[table]["key"]
    df = spark.table(BRONZE_TABLE.format(table))
    newest_first = Window.partitionBy(key).orderBy(F.col("_ingested_at").desc(), F.col("_ingest_date").desc())
    latest = df.withColumn("_rn", F.row_number().over(newest_first)).filter(F.col("_rn") == 1)
    last_full = (df.filter(F.col("_load_mode") == "full")
                   .agg(F.max("_ingested_at").alias("_last_full_at")))
    if last_full.first()["_last_full_at"] is None:
        raise RuntimeError(f"bronze_{table} has no full-mode slice, so deleted keys cannot be judged")
    flagged = latest.crossJoin(F.broadcast(last_full)).withColumn(
        "_live", F.col("_ingested_at") >= F.col("_last_full_at"))
    stats = flagged.agg(F.count(F.lit(1)).alias("keys"),
                        F.sum(F.col("_live").cast("int")).alias("live")).first()
    live = flagged.filter(F.col("_live")).select(*[c for c, _ in TABLES[table]["columns"]])
    return live, {"bronze_rows": df.count(), "keys": stats["keys"], "live": stats["live"],
                  "gone": stats["keys"] - stats["live"]}


snapshots, stats = {}, {}
problems = []
for table in TABLES:
    df, s = current_snapshot(table)
    key = TABLES[table]["key"]
    s["null_keys"] = df.filter(F.col(key).isNull()).count()
    s["distinct_keys"] = df.select(key).distinct().count()
    snapshots[table], stats[table] = df, s
    if s["live"] != SOURCE_COUNTS[table]:
        problems.append(f"{table}: {s['live']} live keys, source has {SOURCE_COUNTS[table]} "
                        f"({s['gone']} keys absent from the latest full slice)")
    if s["null_keys"]:
        problems.append(f"{table}: {s['null_keys']} null keys")
    if s["distinct_keys"] != s["live"]:
        problems.append(f"{table}: {s['live']} rows but {s['distinct_keys']} distinct keys")

print(f"{'table':<22}{'bronze rows':>12}{'keys':>9}{'live':>9}{'gone':>7}")
for table, s in stats.items():
    print(f"{table:<22}{s['bronze_rows']:>12,}{s['keys']:>9,}{s['live']:>9,}{s['gone']:>7,}")
if problems:
    raise RuntimeError("Snapshot checks failed, nothing written:\n" + "\n".join(problems))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 6 - WRITE ONE PARQUET FILE PER TABLE, THEN VERIFY IT
# ===========================================================================

def write_single_parquet(df, batch_dir, table):
    """Write df as batch_dir/<table>/part-00000.parquet and return that path.

    Spark names its files itself, so it writes into a temporary folder that
    is then reduced to the one file the contract names.
    """
    key = TABLES[table]["key"]
    tmp = f"{batch_dir}/_tmp_{table}"
    df.repartition(1).sortWithinPartitions(key).write.mode("errorifexists").parquet(tmp)
    parts = [f.name for f in notebookutils.fs.ls(tmp) if f.name.endswith(".parquet")]
    if len(parts) != 1:
        raise RuntimeError(f"{table}: expected one Parquet file in {tmp}, found {parts}")
    final = f"{batch_dir}/{table}/{DATA_FILE_NAME}"
    notebookutils.fs.mv(f"{tmp}/{parts[0]}", final, create_path=True, overwrite=False)
    notebookutils.fs.rm(tmp, recurse=True)
    return final


def file_digest(path):
    """(bytes, sha256) of one file, read whole through Spark's binaryFile source."""
    content = bytes(spark.read.format("binaryFile").load(path).select("content").first()["content"])
    return len(content), hashlib.sha256(content).hexdigest()


manifest_tables = {}
for table, df in snapshots.items():
    path = write_single_parquet(df, BATCH_DIR, table)
    written = spark.read.parquet(path)
    actual = [(f.name, f.dataType.simpleString()) for f in written.schema.fields]
    if actual != file_columns(table, MODE_SNAPSHOT):
        raise RuntimeError(f"{table}: written file schema {actual} differs from the contract")
    rows = written.count()
    if rows != stats[table]["live"]:
        raise RuntimeError(f"{table}: written file has {rows} rows, expected {stats[table]['live']}")
    size, digest = file_digest(path)
    manifest_tables[table] = {
        "files": [{"path": f"{table}/{DATA_FILE_NAME}", "bytes": size, "sha256": digest}],
        "row_count": rows,
        "min_sequence": SNAPSHOT_SEQUENCE,
        "max_sequence": SNAPSHOT_SEQUENCE,
        "operation_counts": None,
    }
    print(f"{table:<22}{rows:>9,} rows  {size:>11,} bytes  {digest[:12]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 7 - MANIFEST, WRITTEN LAST
# ===========================================================================

manifest = {
    "contract_version": CONTRACT_VERSION,
    "batch_id": batch_id,
    "batch_seq": 0,
    "mode": MODE_SNAPSHOT,
    "business_date": None,
    "as_of_date": as_of_date,
    "previous_batch_id": None,
    "supersedes_batch_id": None,
    "producer": {
        "name": "nb_landing_export_batch0",
        "source": "lh_logistics_bronze bronze_* Delta tables",
        "reduction": "newest _ingested_at then _ingest_date per key; keys absent from the latest full slice excluded",
        "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    },
    "content_hash": content_hash(manifest_tables),
    "tables": manifest_tables,
}
problems = manifest_problems(manifest)
if problems:
    raise RuntimeError("Manifest is invalid, not written:\n" + "\n".join(problems))

leftovers = [f.name for f in notebookutils.fs.ls(BATCH_DIR) if f.name.strip("/") not in TABLES]
if leftovers:
    raise RuntimeError(f"Unexpected entries in {BATCH_DIR}: {leftovers}")

notebookutils.fs.put(f"{BATCH_DIR}/{MANIFEST_NAME}", manifest_json(manifest), False)
print(f"manifest written: {BATCH_DIR}/{MANIFEST_NAME}")
print(f"content hash    : {manifest['content_hash']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 8 - UPLOAD COMMANDS (run on your machine as the working account)
# ===========================================================================
# Data files first, manifest last, never overwriting. AzCopy uses the identity
# you signed in with for both OneLake and the storage account.

workspace = "logistics-dataops-fabric"
source = (f"https://onelake.dfs.fabric.microsoft.com/{workspace}/lh_logistics_ops.Lakehouse/"
          f"{BATCH_DIR}")
target = "https://stlogisticslake.dfs.core.windows.net/landing/logistics"
print("azcopy login")
print(f'azcopy copy "{source}" "{target}/" --recursive --exclude-pattern "{MANIFEST_NAME}" '
      f'--overwrite false --trusted-microsoft-suffixes "fabric.microsoft.com"')
print(f'azcopy copy "{source}/{MANIFEST_NAME}" "{target}/{batch_folder(0)}/{MANIFEST_NAME}" '
      f'--overwrite false --trusted-microsoft-suffixes "fabric.microsoft.com"')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
