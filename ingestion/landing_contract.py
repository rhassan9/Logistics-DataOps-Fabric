"""Landing contract for the logistics ADLS Gen2 landing zone.

One definition of what a landing batch is: the 12 source tables, their columns
and types, their business keys, the change metadata columns, and the manifest
rules. The generator, its tests and the two batch 0 notebooks all use it.

The block between the two marker lines is copied verbatim into
nb_landing_export_batch0 and nb_landing_gate_batch0, because a Fabric notebook
cannot import this file. ingestion/tests/test_landing_contract.py fails if any
copy differs from this one, so edit it here and paste it into both notebooks.

Column types are written as Spark SQL type strings (DataType.simpleString), so
the notebooks compare them with a Spark schema directly. The Arrow mapping
below the block is used by the generator only.
"""

from __future__ import annotations

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


# Arrow mapping, generator only ------------------------------------------------

def arrow_type(spark_type):
    """Arrow type that Spark reads back as the given Spark type.

    Timestamps are microsecond and UTC-adjusted, which Spark reads as
    TimestampType (not TimestampNTZType).
    """
    import pyarrow as pa

    simple = {
        "string": pa.string(),
        "int": pa.int32(),
        "bigint": pa.int64(),
        "date": pa.date32(),
        "timestamp": pa.timestamp("us", tz="UTC"),
        "boolean": pa.bool_(),
    }
    if spark_type in simple:
        return simple[spark_type]
    if spark_type.startswith("decimal(") and spark_type.endswith(")"):
        precision, scale = (int(p) for p in spark_type[8:-1].split(","))
        return pa.decimal128(precision, scale)
    raise ValueError(f"no Arrow mapping for Spark type {spark_type!r}")


def arrow_schema(table, mode):
    """Arrow schema of a landing file of this table and mode."""
    import pyarrow as pa

    return pa.schema([pa.field(name, arrow_type(kind)) for name, kind in file_columns(table, mode)])


def block_text(source):
    """The marked contract block inside a file's text, markers included."""
    begin = "# >>> landing-contract:begin"
    end = "# <<< landing-contract:end"
    start = source.index(begin)
    stop = source.index(end, start) + len(end)
    return source[start:stop]
