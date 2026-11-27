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

# Batch discovery
#
# Reads    : Files/landing in lh_logistics_bronze, the OneLake shortcut to
#            stlogisticslake/landing/logistics (workspace identity, read-only)
# Writes   : wh_logistics_gold log.batch_manifest, one row per newly landed batch
#
# ---------------------------------------------------------------------------
# WHAT DISCOVERY DOES
# ---------------------------------------------------------------------------
# Registers every batch folder whose manifest has landed, once. A folder
# without _manifest.json has not landed yet, because the manifest is written
# last. Each new manifest is checked against the landing contract and
# recorded with its batch id, content hash, validity and two times:
#
#   landed_at      the manifest's last-modified time in storage: when the
#                  batch became complete
#   discovered_at  when this notebook first saw it
#
# Discovery records facts about landing only. Every state change after that
# (claimed, silver_applied, completed, failed, rejected, waived) is made by a
# warehouse procedure inside one transaction that also updates the registry
# lock row, so claims cannot race. log.vw_batch_state derives each batch's
# state, and log.vw_batch_next the decision the next run takes.
#
# ---------------------------------------------------------------------------
# WHAT HAPPENS TO A BATCH THAT IS NOT CLEAN
# ---------------------------------------------------------------------------
#   - an invalid manifest is registered as invalid: the batch reads as
#     rejected, and every later batch waits for it to be superseded or waived
#   - the same batch_id and content landing again under a new batch_seq reads
#     as duplicate_ignored
#   - a batch_id registered again with different content, or a folder whose
#     manifest changed after registration, reads as conflict: the next run
#     stops until an operator waives it
#   - a manifest that does not read completely is not registered; the next
#     run reads it again
#
# Running the notebook again registers nothing new: a registration is new only
# when its folder, batch_id and content hash are not registered yet.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# ===========================================================================
# CELL 1 - PARAMETERS
# ===========================================================================

landing_root = "Files/landing"   # the bronze shortcut to landing/logistics
run_id       = ""                # the pipeline run id; a manual run gets its own

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
# CELL 3 - BATCH DISCOVERY (verbatim copy of ingestion/batch_discovery.py)
# ===========================================================================
# >>> batch-discovery:begin
# Batch discovery, version 1. Source of truth: ingestion/batch_discovery.py.
# Copied verbatim into nb_batch_discover; a unit test keeps the copy equal.
# Uses the names of the landing contract block.
import json
import re
from datetime import date

DISCOVERY_VERSION = "1"

BATCH_FOLDER_PATTERN = re.compile(r"batch_seq=(\d{6})")

REGISTRATION_COLUMNS = (
    "batch_seq", "batch_id", "batch_mode", "business_date", "as_of_date",
    "previous_batch_id", "supersedes_batch_id", "content_hash", "is_valid",
    "problems", "table_count", "row_count", "producer", "manifest_path",
    "landed_at", "discovered_at", "discovery_run_id",
)

TEXT_LIMITS = {"batch_id": 128, "batch_mode": 20, "previous_batch_id": 128,
               "supersedes_batch_id": 128, "problems": 4000, "producer": 256,
               "manifest_path": 1024, "discovery_run_id": 64}


def folder_seq(name):
    """batch_seq of a batch folder name, or None when the name is not a batch folder."""
    match = BATCH_FOLDER_PATTERN.fullmatch(name.rstrip("/"))
    return int(match.group(1)) if match else None


def read_problem(content, listed_size, file_length):
    """Why a manifest read is not complete, or None when it is.

    content: the bytes read. listed_size: the size the folder listing reports.
    file_length: the length the reader reports for the file.
    """
    if listed_size is None:
        return "not listed"
    if len(content) != listed_size or file_length != listed_size:
        return f"read {len(content)} bytes, reader length {file_length}, listed size {listed_size}"
    return None


def _text(value):
    return value if isinstance(value, str) and value else None


def _date(value):
    try:
        return date.fromisoformat(value) if isinstance(value, str) and len(value) == 10 else None
    except ValueError:
        return None


def _clip(row):
    for column, limit in TEXT_LIMITS.items():
        if isinstance(row[column], str) and len(row[column]) > limit:
            row[column] = row[column][:limit - 3] + "..."
    return row


def registration(folder_name, manifest_path, content, landed_at, discovered_at, run_id):
    """One log.batch_manifest row for a completely read manifest.

    Any way the manifest breaks the contract, or disagrees with the folder it
    is in, makes the row invalid; the reasons go into problems.
    """
    seq = folder_seq(folder_name)
    if seq is None:
        raise ValueError(f"{folder_name} is not a batch folder")
    problems, manifest = [], None
    try:
        manifest = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        problems.append(f"{MANIFEST_NAME} is not UTF-8 JSON: {exc}")
    if manifest is not None and not isinstance(manifest, dict):
        problems.append(f"{MANIFEST_NAME} is not a JSON object")
        manifest = None
    if manifest is not None:
        problems += manifest_problems(manifest)
        if manifest.get("batch_seq") != seq:
            problems.append(f"manifest batch_seq {manifest.get('batch_seq')!r} is not the folder's {seq}")
    m = manifest or {}
    tables = m.get("tables") if isinstance(m.get("tables"), dict) else {}
    counts = [spec.get("row_count") for spec in tables.values() if isinstance(spec, dict)]
    declared_hash = m.get("content_hash")
    producer = m.get("producer")
    row = {
        "batch_seq": seq,
        "batch_id": _text(m.get("batch_id")) or batch_folder(seq),
        "batch_mode": _text(m.get("mode")),
        "business_date": _date(m.get("business_date")),
        "as_of_date": _date(m.get("as_of_date")),
        "previous_batch_id": _text(m.get("previous_batch_id")),
        "supersedes_batch_id": _text(m.get("supersedes_batch_id")),
        "content_hash": declared_hash if isinstance(declared_hash, str) and len(declared_hash) == 64 else None,
        "is_valid": not problems,
        "problems": "; ".join(problems) or None,
        "table_count": len(tables) if manifest is not None else None,
        "row_count": (sum(counts) if counts and all(isinstance(c, int) and not isinstance(c, bool)
                                                     for c in counts) else None),
        "producer": json.dumps(producer, sort_keys=True) if isinstance(producer, dict) else None,
        "manifest_path": manifest_path,
        "landed_at": landed_at,
        "discovered_at": discovered_at,
        "discovery_run_id": run_id,
    }
    return _clip(row)


def registration_key(row):
    """What makes a registration new: the folder, the batch_id and the content."""
    return (row["batch_seq"], row["batch_id"], row["content_hash"] or "")


def new_registrations(rows, registered_keys):
    """The rows whose registration_key is not registered yet, in batch_seq order."""
    seen = set(registered_keys)
    out = []
    for row in sorted(rows, key=lambda r: r["batch_seq"]):
        key = registration_key(row)
        if key not in seen:
            seen.add(key)
            out.append(row)
    return out
# <<< batch-discovery:end

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 4 - CONFIGURATION
# ===========================================================================
import datetime as dt
import time

import com.microsoft.spark.fabric  # noqa: F401  (registers synapsesql)
from com.microsoft.spark.fabric.Constants import Constants
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark.conf.set("spark.sql.session.timeZone", "UTC")

WAREHOUSE = "wh_logistics_gold"
READ_ATTEMPTS = 4
READ_WAIT_SECONDS = 15

if not run_id:
    run_id = "discover-" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")

SPARK_TYPES = {
    "batch_seq": T.IntegerType(), "batch_id": T.StringType(), "batch_mode": T.StringType(),
    "business_date": T.DateType(), "as_of_date": T.DateType(), "previous_batch_id": T.StringType(),
    "supersedes_batch_id": T.StringType(), "content_hash": T.StringType(), "is_valid": T.BooleanType(),
    "problems": T.StringType(), "table_count": T.IntegerType(), "row_count": T.LongType(),
    "producer": T.StringType(), "manifest_path": T.StringType(), "landed_at": T.TimestampType(),
    "discovered_at": T.TimestampType(), "discovery_run_id": T.StringType(),
}
REGISTRATION_SCHEMA = T.StructType([T.StructField(c, SPARK_TYPES[c], True) for c in REGISTRATION_COLUMNS])


def gold_query(sql):
    """Run a T-SQL query in the warehouse and return the result."""
    return spark.read.option(Constants.DatabaseName, WAREHOUSE).synapsesql(sql)


def absolute(path):
    """An absolute path for a path relative to the bronze lakehouse (Files/...)."""
    if not path.startswith("Files/"):
        return path
    bronze = notebookutils.lakehouse.get("lh_logistics_bronze")
    return f"abfss://{bronze.workspaceId}@onelake.dfs.fabric.microsoft.com/{bronze.id}/{path}"


def modified_at(path):
    """A file's last-modified time in storage, as a UTC datetime.

    Read from the Hadoop file status: Spark's file sources skip files whose
    names start with an underscore, so binaryFile cannot read _manifest.json.
    """
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    status = jvm_path.getFileSystem(spark._jsc.hadoopConfiguration()).getFileStatus(jvm_path)
    return dt.datetime.fromtimestamp(status.getModificationTime() / 1000, tz=dt.timezone.utc)


def read_manifest(path, listed_size):
    """(content, landed_at) of a completely read manifest, or (None, reason).

    A read whose length is not the listed size is retried; after the last
    attempt the manifest is left for the next run.
    """
    reason = ""
    for attempt in range(1, READ_ATTEMPTS + 1):
        content = notebookutils.fs.head(path, listed_size + 1).encode("utf-8")
        landed_at = modified_at(path)
        reason = read_problem(content, listed_size, listed_size)
        if reason is None:
            return content, landed_at
        print(f"  {path}: incomplete read on attempt {attempt}: {reason}")
        if attempt < READ_ATTEMPTS:
            time.sleep(READ_WAIT_SECONDS * attempt)
    return None, reason


landing_root = absolute(landing_root)
print(f"run_id={run_id}")
print(f"landing_root={landing_root}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 5 - READ LANDED MANIFESTS
# ===========================================================================

discovered_at = dt.datetime.now(dt.timezone.utc)
rows, not_landed, unread, ignored = [], [], [], []

for info in notebookutils.fs.ls(landing_root):
    name = info.name.rstrip("/")
    if folder_seq(name) is None:
        ignored.append(name)
        continue
    folder = f"{landing_root}/{name}"
    listed = {i.name.rstrip("/"): i.size for i in notebookutils.fs.ls(folder)}
    if MANIFEST_NAME not in listed:
        not_landed.append(name)
        continue
    path = f"{folder}/{MANIFEST_NAME}"
    content, landed_at = read_manifest(path, listed[MANIFEST_NAME])
    if content is None:
        unread.append(f"{name} ({landed_at})")
        continue
    rows.append(registration(name, path, content, landed_at, discovered_at, run_id))

print(f"manifests read completely: {len(rows)}")
print(f"folders without a manifest (not landed yet): {not_landed or 'none'}")
print(f"manifests not read completely (left for the next run): {unread or 'none'}")
if ignored:
    print(f"entries that are not batch folders, ignored: {ignored}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 6 - REGISTER NEW BATCHES
# ===========================================================================
# Append only. Rows are never updated or deleted.

registered_keys = {
    (r["batch_seq"], r["batch_id"], r["content_hash"])
    for r in gold_query("""
        SELECT m.batch_seq, m.batch_id, COALESCE(m.content_hash, '') AS content_hash
        FROM log.batch_manifest AS m""").collect()
}
new_rows = new_registrations(rows, registered_keys)

for r in new_rows:
    label = "valid" if r["is_valid"] else "invalid"
    print(f"  new: batch_seq {r['batch_seq']:>6}  {r['batch_id']:<24} {r['batch_mode'] or '':<17} "
          f"landed {r['landed_at']:%Y-%m-%d %H:%M:%S} UTC  {label}")
    if r["problems"]:
        print(f"       problems: {r['problems']}")

if new_rows:
    (spark.createDataFrame([tuple(r[c] for c in REGISTRATION_COLUMNS) for r in new_rows], REGISTRATION_SCHEMA)
          .write.mode("append")
          .synapsesql(f"{WAREHOUSE}.log.batch_manifest"))
    stored = gold_query(f"""
        SELECT COUNT(*) AS n FROM log.batch_manifest AS m
        WHERE m.discovery_run_id = '{run_id}'""").first()["n"]
    if stored != len(new_rows):
        raise RuntimeError(f"registered {len(new_rows)} batches but the warehouse holds {stored} "
                           f"rows for discovery run {run_id}")
    print(f"registered {len(new_rows)} batch(es)")
else:
    print("nothing new to register")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 7 - REGISTRY STATE AND THE NEXT DECISION
# ===========================================================================

display(gold_query("""
    SELECT s.batch_seq, s.batch_id, s.batch_mode, s.state, s.state_at, s.owner_run_id,
           s.landed_at, s.discovered_at, s.state_message
    FROM log.vw_batch_state AS s""").orderBy("batch_seq"))

decision = gold_query("SELECT n.decision, n.batch_seq, n.batch_id, n.reason FROM log.vw_batch_next AS n").first()
print(f"next: {decision['decision']} batch_seq={decision['batch_seq']} ({decision['reason']})")

notebookutils.notebook.exit(json.dumps({
    "run_id": run_id,
    "registered": [r["batch_seq"] for r in new_rows],
    "next_decision": decision["decision"],
    "next_batch_seq": decision["batch_seq"],
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
