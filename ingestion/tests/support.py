"""Synthetic batch 0 for the generator tests: a few rows per table, built to the contract."""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion import landing_contract as lc

AS_OF = "2024-12-31"
KEY_PREFIX = {"customers": "CUST", "drivers": "DRV", "trucks": "TRK"}
ROWS_PER_TABLE = 6


def key_for(table, i):
    return f"{KEY_PREFIX.get(table, table[:3].upper())}{i:05d}"


def value_for(table, column, kind, i):
    if column == lc.TABLES[table]["key"]:
        return key_for(table, i)
    if column == "termination_date":
        return None          # a nullable column that is null in batch 0
    if kind == "string":
        return f"{column}-{i}"
    if kind in ("int", "bigint"):
        return i * 10
    if kind == "boolean":
        return i % 2 == 0
    if kind == "date":
        return date(2024, 1, i)
    if kind == "timestamp":
        return datetime(2024, 1, i, 10, 30, tzinfo=timezone.utc)
    if kind.startswith("decimal("):
        scale = int(kind[8:-1].split(",")[1])
        return Decimal(f"{i}.5").quantize(Decimal(1).scaleb(-scale))
    raise AssertionError(kind)


def snapshot_rows(table, count=ROWS_PER_TABLE):
    columns = lc.TABLES[table]["columns"]
    return [{name: value_for(table, name, kind, i) for name, kind in columns} for i in range(1, count + 1)]


def write_snapshot(landing_root, overrides=None, batch_id="logistics-000000"):
    """Write batch_seq=000000 like the export notebook does. overrides: table -> list of row dicts."""
    overrides = overrides or {}
    root = Path(landing_root) / lc.batch_folder(0)
    tables = {}
    for table in lc.TABLES:
        rows = overrides.get(table, snapshot_rows(table))
        schema = lc.arrow_schema(table, lc.MODE_SNAPSHOT)
        data = pa.Table.from_pylist(rows, schema=schema)
        path = root / table / lc.DATA_FILE_NAME
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(data, path, compression="snappy")
        payload = path.read_bytes()
        tables[table] = {
            "files": [{"path": f"{table}/{lc.DATA_FILE_NAME}", "bytes": len(payload),
                       "sha256": hashlib.sha256(payload).hexdigest()}],
            "row_count": len(rows),
            "min_sequence": lc.SNAPSHOT_SEQUENCE,
            "max_sequence": lc.SNAPSHOT_SEQUENCE,
            "operation_counts": None,
        }
    manifest = {
        "contract_version": lc.CONTRACT_VERSION,
        "batch_id": batch_id,
        "batch_seq": 0,
        "mode": lc.MODE_SNAPSHOT,
        "business_date": None,
        "as_of_date": AS_OF,
        "previous_batch_id": None,
        "supersedes_batch_id": None,
        "producer": {"name": "ingestion/tests/support.py"},
        "content_hash": lc.content_hash(tables),
        "tables": tables,
    }
    (root / lc.MANIFEST_NAME).write_text(lc.manifest_json(manifest), encoding="utf-8")
    return manifest


def read_batch(landing_root, batch_seq):
    """(manifest dict, {table: list of row dicts}) of a written batch."""
    import json

    root = Path(landing_root) / lc.batch_folder(batch_seq)
    manifest = json.loads((root / lc.MANIFEST_NAME).read_text(encoding="utf-8"))
    rows = {table: pq.read_table(root / table / lc.DATA_FILE_NAME).to_pylist() for table in manifest["tables"]}
    return manifest, rows


def tree_bytes(folder):
    """{relative path: bytes} of every file under a folder."""
    folder = Path(folder)
    return {str(p.relative_to(folder)): p.read_bytes() for p in sorted(folder.rglob("*")) if p.is_file()}
