"""Batch discovery: turn landed batch folders into log.batch_manifest registrations.

Discovery reads only manifests. A folder without a manifest has not landed yet
(the manifest is written last), and a manifest that did not read completely is
left for the next run rather than registered, because a registration is
permanent: a valid batch registered as invalid would stay rejected. The files a
manifest lists are checked later, by nb_silver_apply_batch, before anything is
written to silver.

Registrations are insert-only. A folder is registered again only when its
manifest content differs from every earlier registration of that folder, which
log.vw_batch_state reports as a conflict.

The block between the marker lines is copied verbatim into nb_batch_discover,
because a Fabric notebook cannot import this file.
ingestion/tests/test_batch_discovery.py fails if the copy differs. The block
uses the landing contract's names, which the notebook also holds.
"""

from __future__ import annotations

from ingestion.landing_contract import (  # noqa: F401  (names the block uses)
    MANIFEST_NAME,
    MODE_SNAPSHOT,
    batch_folder,
    manifest_problems,
)

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


BEGIN_MARKER = "# >>> batch-discovery:begin"
END_MARKER = "# <<< batch-discovery:end"


def block_text(text):
    """The batch discovery block of a file, markers included."""
    start = text.index(BEGIN_MARKER)
    end = text.index(END_MARKER, start) + len(END_MARKER)
    return text[start:end]
