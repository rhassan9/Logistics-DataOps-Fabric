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

# CELL ********************

# Landing batch 0 gate
#
# Reads    : Files/landing in lh_logistics_bronze, the OneLake shortcut to
#            stlogisticslake/landing/logistics (workspace identity, read-only)
# Compares : lh_logistics_bronze bronze_* schemas and lh_logistics_silver silver_* rows
# Writes   : nothing, except a scratch copy in lh_logistics_ops for the self-test
#
# ---------------------------------------------------------------------------
# WHAT THE GATE PROVES
# ---------------------------------------------------------------------------
# Batch 0 gate steps 1 to 4 of the M4 ingestion design: the export exists,
# its twelve schemas and types are the contract's and bronze's, its row
# counts and business keys match the source and the current silver rows, and
# the shortcut reads every file. Every check reads through the shortcut, so a
# pass is also proof that the workspace identity can read landing.
#
# Checks, per table unless marked batch:
#   manifest            batch: the manifest satisfies the contract rules
#   batch_layout        batch: exactly the twelve table folders plus the manifest
#   files               the files listed are the files present, same size and sha256
#   readable            Spark reads the files as Parquet; later checks need this
#   schema_vs_contract  column names, order and Spark types equal the contract
#   schema_vs_bronze    the same against the bronze table without lineage columns
#   count_vs_manifest   rows read equal the manifest row_count
#   count_vs_source     rows equal the reconciled source count
#   count_vs_silver     rows equal silver rows not flagged deleted
#   key_not_null        no null business key
#   key_unique          no business key twice
#   keys_vs_silver      the key sets of landing and live silver are equal
#
# ---------------------------------------------------------------------------
# SELF-TEST
# ---------------------------------------------------------------------------
# A gate that never fails proves nothing. With run_self_test on, after a clean
# pass the notebook copies batch 0 into a scratch folder with five planted
# defects and runs the same checks on the copy. It passes only when exactly
# the expected eleven failures occur, no more and no fewer. Real landing data
# is never modified.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# ===========================================================================
# CELL 1 - PARAMETERS
# ===========================================================================

landing_root     = "Files/landing"     # the bronze shortcut to landing/logistics
expected_as_of   = "2024-12-31"
run_self_test    = True
self_test_folder = "Files/landing_gate_self_test"   # scratch, in lh_logistics_ops

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
import time
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
# CELL 3 - CONFIGURATION
# ===========================================================================

import hashlib

from pyspark.sql import functions as F

BRONZE_TABLE = "lh_logistics_bronze.dbo.bronze_{}"
SILVER_TABLE = "lh_logistics_silver.dbo.silver_{}"
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

ops = notebookutils.lakehouse.get("lh_logistics_ops")
SELF_TEST_ROOT = (f"abfss://{ops.workspaceId}@onelake.dfs.fabric.microsoft.com/"
                  f"{ops.id}/{self_test_folder}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 4 - GATE CHECKS
# ===========================================================================

def entry_name(info):
    return info.name.rstrip("/")


PARQUET_MAGIC = b"PAR1"


def listed_size(path):
    """Size in bytes of one file as the file system lists it."""
    parent, name = path.rsplit("/", 1)
    sizes = {i.name.rstrip("/"): i.size for i in notebookutils.fs.ls(parent)}
    if name not in sizes:
        raise RuntimeError(f"{path} is not listed in {parent}")
    return sizes[name]


def file_digest(path, attempts=4, wait_seconds=15):
    """(bytes, sha256) of one complete Parquet file.

    A read right after a write can return only part of the file, so a read
    counts only when its length equals the listed size and it starts and ends
    with the Parquet magic bytes. Otherwise the cached file status is refreshed
    and the read is retried; after the last attempt the run stops rather than
    record a hash of a partial file.
    """
    problem = ""
    for attempt in range(1, attempts + 1):
        expected = listed_size(path)
        spark.catalog.refreshByPath(path)
        content = bytes(spark.read.format("binaryFile").load(path).select("content").first()["content"])
        if len(content) == expected and content[:4] == PARQUET_MAGIC and content[-4:] == PARQUET_MAGIC:
            return len(content), hashlib.sha256(content).hexdigest()
        problem = (f"read {len(content):,} bytes, listed size {expected:,}, "
                   f"starts {content[:4]!r}, ends {content[-4:]!r}")
        print(f"  {path}: incomplete read on attempt {attempt}: {problem}")
        if attempt < attempts:
            time.sleep(wait_seconds * attempt)
    raise RuntimeError(f"{path}: no complete read after {attempts} attempts ({problem})")

def spark_columns(schema):
    return [(f.name, f.dataType.simpleString()) for f in schema.fields]


def read_manifest(batch_dir):
    import json
    return json.loads(notebookutils.fs.head(f"{batch_dir}/{MANIFEST_NAME}", 10 * 1024 * 1024))


def run_gate(root):
    """Run every check on root/batch_seq=000000. Returns a list of result dicts."""
    batch_dir = f"{root}/{batch_folder(0)}"
    results = []

    def record(table, check, ok, detail=""):
        results.append({"table": table, "check": check, "ok": bool(ok), "detail": detail})

    try:
        manifest = read_manifest(batch_dir)
    except Exception as exc:
        record("*", "manifest", False, f"cannot read {MANIFEST_NAME}: {exc}")
        return results
    problems = manifest_problems(manifest)
    if manifest.get("mode") != MODE_SNAPSHOT:
        problems.append(f"mode is {manifest.get('mode')}, expected {MODE_SNAPSHOT}")
    if manifest.get("as_of_date") != expected_as_of:
        problems.append(f"as_of_date is {manifest.get('as_of_date')}, expected {expected_as_of}")
    record("*", "manifest", not problems, "; ".join(problems))
    if problems:
        return results

    entries = {entry_name(i) for i in notebookutils.fs.ls(batch_dir)}
    expected_entries = set(TABLES) | {MANIFEST_NAME}
    record("*", "batch_layout", entries == expected_entries,
           f"missing {sorted(expected_entries - entries)}, extra {sorted(entries - expected_entries)}")

    for table, spec in TABLES.items():
        key = spec["key"]
        listed = manifest["tables"][table]
        table_dir = f"{batch_dir}/{table}"

        # files: listed equals present; size and sha256 equal
        present = {entry_name(i): i.size for i in notebookutils.fs.ls(table_dir)}
        wanted = {e["path"].split("/", 1)[1]: e for e in listed["files"]}
        issues = []
        if set(present) != set(wanted):
            issues.append(f"present {sorted(present)}, listed {sorted(wanted)}")
        for name, entry in wanted.items():
            if name not in present:
                continue
            try:
                size, digest = file_digest(f"{table_dir}/{name}")
            except RuntimeError as exc:
                issues.append(str(exc))
                continue
            if size != entry["bytes"]:
                issues.append(f"{name}: {size} bytes, manifest {entry['bytes']}")
            if digest != entry["sha256"]:
                issues.append(f"{name}: sha256 {digest[:12]}, manifest {entry['sha256'][:12]}")
        record(table, "files", not issues, "; ".join(issues))

        try:
            df = spark.read.parquet(*[f"{table_dir}/{name}" for name in sorted(wanted)])
            actual = spark_columns(df.schema)
            rows = df.count()
        except Exception as exc:
            first_line = (str(exc).strip().splitlines() or [""])[0][:200]
            record(table, "readable", False, f"{type(exc).__name__}: {first_line}")
            continue
        record(table, "readable", True)
        contract = file_columns(table, MODE_SNAPSHOT)
        record(table, "schema_vs_contract", actual == contract,
               "" if actual == contract else f"{[c for c in actual if c not in contract]} vs {[c for c in contract if c not in actual]}")
        bronze = [c for c in spark_columns(spark.table(BRONZE_TABLE.format(table)).schema) if c[0] not in LINEAGE]
        record(table, "schema_vs_bronze", actual == bronze,
               "" if actual == bronze else f"{[c for c in actual if c not in bronze]} vs {[c for c in bronze if c not in actual]}")

        silver = (spark.table(SILVER_TABLE.format(table))
                  .filter(~F.coalesce(F.col("_is_deleted_in_source"), F.lit(False))))
        silver_rows = silver.count()
        record(table, "count_vs_manifest", rows == listed["row_count"], f"{rows} vs {listed['row_count']}")
        record(table, "count_vs_source", rows == SOURCE_COUNTS[table], f"{rows} vs {SOURCE_COUNTS[table]}")
        record(table, "count_vs_silver", rows == silver_rows, f"{rows} vs {silver_rows} live silver rows")

        nulls = df.filter(F.col(key).isNull()).count()
        record(table, "key_not_null", nulls == 0, f"{nulls} null keys")
        dupes = df.filter(F.col(key).isNotNull()).groupBy(key).count().filter(F.col("count") > 1).count()
        record(table, "key_unique", dupes == 0, f"{dupes} keys appear more than once")

        landing_keys = df.select(key).filter(F.col(key).isNotNull()).distinct()
        silver_keys = silver.select(key).distinct()
        missing = silver_keys.join(landing_keys, key, "left_anti").count()
        extra = landing_keys.join(silver_keys, key, "left_anti").count()
        record(table, "keys_vs_silver", missing == 0 and extra == 0,
               f"{missing} live silver keys missing from landing, {extra} landing keys not live in silver")
    return results


def show(results, title):
    failed = [r for r in results if not r["ok"]]
    print(f"{title}: {len(results) - len(failed)} passed, {len(failed)} failed")
    for r in results:
        mark = "PASS" if r["ok"] else "FAIL"
        detail = "" if r["ok"] else f"  {r['detail']}"
        print(f"  {mark}  {r['table']:<22}{r['check']:<20}{detail}")
    return failed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 5 - GATE ON THE REAL BATCH 0
# ===========================================================================

silver_deleted = {t: spark.table(SILVER_TABLE.format(t)).filter(F.col("_is_deleted_in_source")).count()
                  for t in TABLES}
print("silver rows flagged deleted (not expected in a snapshot):",
      {t: n for t, n in silver_deleted.items() if n} or "none")

gate_results = run_gate(landing_root)
gate_failures = show(gate_results, "Batch 0 gate")
if gate_failures:
    raise RuntimeError(f"Batch 0 gate failed: {len(gate_failures)} checks. Nothing downstream may use this batch.")
print("Batch 0 gate passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 6 - SELF-TEST: PLANTED DEFECTS MUST FAIL EXACTLY THE EXPECTED CHECKS
# ===========================================================================

EXPECTED_FAILURES = {
    ("customers", "schema_vs_contract"), ("customers", "schema_vs_bronze"),   # decimal turned into double
    ("routes", "count_vs_source"), ("routes", "count_vs_silver"), ("routes", "keys_vs_silver"),  # row dropped
    ("facilities", "count_vs_source"), ("facilities", "count_vs_silver"), ("facilities", "key_unique"),  # row doubled
    ("drivers", "key_not_null"), ("drivers", "keys_vs_silver"),            # one key set to null
    ("trailers", "files"),                                                 # manifest sha256 wrong
}


def write_single_parquet(df, batch_dir, table):
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


def build_defective_copy(source_root, target_root):
    source_dir = f"{source_root}/{batch_folder(0)}"
    target_dir = f"{target_root}/{batch_folder(0)}"
    if notebookutils.fs.exists(target_root):
        notebookutils.fs.rm(target_root, recurse=True)
    manifest = read_manifest(source_dir)
    tables = manifest["tables"]

    def table_df(table):
        return spark.read.parquet(f"{source_dir}/{table}/{DATA_FILE_NAME}")

    def low_key(table):
        key = TABLES[table]["key"]
        return table_df(table).agg(F.min(key)).first()[0]

    defects = {
        "customers": lambda df: df.withColumn("annual_revenue_potential",
                                              F.col("annual_revenue_potential").cast("double")),
        "routes": lambda df: df.filter(F.col("route_id") != F.lit(low_key("routes"))),
        "facilities": lambda df: df.unionByName(df.filter(F.col("facility_id") == F.lit(low_key("facilities")))),
        "drivers": lambda df: df.withColumn("driver_id", F.when(F.col("driver_id") == F.lit(low_key("drivers")),
                                                                F.lit(None).cast("string"))
                                                          .otherwise(F.col("driver_id"))),
    }
    for table in TABLES:
        if table in defects:
            path = write_single_parquet(defects[table](table_df(table)), target_dir, table)
            size, digest = file_digest(path)
            tables[table] = {**tables[table], "row_count": spark.read.parquet(path).count(),
                             "files": [{"path": f"{table}/{DATA_FILE_NAME}", "bytes": size, "sha256": digest}]}
        else:
            notebookutils.fs.mkdirs(f"{target_dir}/{table}")
            notebookutils.fs.cp(f"{source_dir}/{table}/{DATA_FILE_NAME}", f"{target_dir}/{table}/{DATA_FILE_NAME}")
    tables["trailers"]["files"][0]["sha256"] = "0" * 64
    manifest["content_hash"] = content_hash(tables)
    notebookutils.fs.put(f"{target_dir}/{MANIFEST_NAME}", manifest_json(manifest), False)


if run_self_test:
    build_defective_copy(landing_root, SELF_TEST_ROOT)
    try:
        test_results = run_gate(SELF_TEST_ROOT)
    finally:
        notebookutils.fs.rm(SELF_TEST_ROOT, recurse=True)
    show(test_results, "Self-test on planted defects")
    observed = {(r["table"], r["check"]) for r in test_results if not r["ok"]}
    unexpected, missed = observed - EXPECTED_FAILURES, EXPECTED_FAILURES - observed
    if unexpected or missed:
        raise RuntimeError(f"Self-test failed. Unexpected failures {sorted(unexpected)}; "
                           f"defects not caught {sorted(missed)}")
    print(f"Self-test passed: all {len(EXPECTED_FAILURES)} planted failures caught, no others.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
