"""Deterministic change-batch fixture generator for the logistics landing zone.

Development and test tooling, not production ingestion code.

What it is: a labelled test fixture. It writes CHANGE_BATCH folders (INSERT,
UPDATE and DELETE events) that follow ingestion/landing_contract.py, starting
from a local copy of batch 0. It is not a live source integration and not
PostgreSQL change data capture.

Determinism: the output bytes depend only on the batch 0 files, the config file,
the generator version and the pyarrow version. All four are recorded in each
manifest. No wall-clock time or unseeded randomness is used.

Rules it enforces (each one a contract rule):
  * batch_seq values run 1, 2, 3 ... with no gaps, after batch 0
  * business dates are after the batch 0 as-of date and strictly increase
  * _source_sequence starts at 1 per table and increases by 1 per event
  * UPDATE and DELETE need a live key; INSERT needs a key that is not live
    (a deleted key may be inserted again, which revives it)
  * an INSERT key is literal (to revive that deleted key) or omitted (the
    next new key); {pick: random} selects live keys, so it applies to UPDATE
    and DELETE keys and to copy_from, never to an INSERT key
  * UPDATE carries the full new row image; DELETE carries the last row image
  * a drivers or trucks event happens on the batch business date
  * no event happens after the end of its batch business date
  * a batch folder that already exists is never written to
  * all batches are built and checked in memory before any file is written,
    and each batch's _manifest.json is written after its data files

deliberate_violations on a batch switches off named rules, so negative test
fixtures can be produced on purpose and visibly:
  business_date_not_increasing, type2_changed_at_not_business_date

Usage, from the repository root:
  python -m ingestion.fixtures.generator --config <config.yaml> --landing-root <dir>
<dir> holds batch_seq=000000 (downloaded from landing) and receives the new
batch folders. --dry-run builds and checks everything and writes nothing.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from ingestion import landing_contract as lc

GENERATOR_NAME = "ingestion/fixtures/generator.py"
GENERATOR_VERSION = "1.0.1"
TYPE2_TABLES = ("drivers", "trucks")
VIOLATIONS = ("business_date_not_increasing", "type2_changed_at_not_business_date")
INT32_RANGE = (-(2**31), 2**31 - 1)
KEY_PATTERN = re.compile(r"^(?P<prefix>\D*)(?P<number>\d+)$")


class FixtureError(Exception):
    """A config, base snapshot or rule problem. Nothing is written when raised."""


# Config ----------------------------------------------------------------------

def _as_date(value, where):
    if isinstance(value, datetime):
        raise FixtureError(f"{where}: expected a date, got a timestamp")
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            pass
    raise FixtureError(f"{where}: expected a date as YYYY-MM-DD, got {value!r}")


def _as_timestamp(value, where):
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise FixtureError(f"{where}: expected an ISO timestamp, got {value!r}") from exc
    else:
        raise FixtureError(f"{where}: expected an ISO timestamp, got {value!r}")
    # Naive values are UTC by contract; aware values are converted to UTC.
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def load_config(path):
    """Read and shape-check a generator config. Returns (config, sha256 of the file bytes)."""
    raw = Path(path).read_bytes()
    try:
        config = yaml.safe_load(raw)
    except (yaml.YAMLError, ValueError) as exc:
        raise FixtureError(f"config is not valid YAML: {exc}") from exc
    if not isinstance(config, dict):
        raise FixtureError("config must be a mapping")
    allowed = {"seed", "batch_id_prefix", "batches"}
    unknown = set(config) - allowed
    if unknown:
        raise FixtureError(f"config: unknown keys {sorted(unknown)}")
    if not isinstance(config.get("seed"), int) or isinstance(config.get("seed"), bool):
        raise FixtureError("config: seed must be an integer")
    config.setdefault("batch_id_prefix", "logistics")
    if not isinstance(config["batch_id_prefix"], str) or not config["batch_id_prefix"]:
        raise FixtureError("config: batch_id_prefix must be a non-empty string")
    batches = config.get("batches")
    if not isinstance(batches, list) or not batches:
        raise FixtureError("config: batches must be a non-empty list")
    batch_keys = {"batch_seq", "business_date", "batch_id", "supersedes_batch_id",
                  "deliberate_violations", "changes"}
    change_keys = {"table", "op", "key", "copy_from", "set", "changed_at"}
    for b, batch in enumerate(batches):
        where = f"batches[{b}]"
        if not isinstance(batch, dict):
            raise FixtureError(f"{where} must be a mapping")
        unknown = set(batch) - batch_keys
        if unknown:
            raise FixtureError(f"{where}: unknown keys {sorted(unknown)}")
        for name in batch.get("deliberate_violations", []) or []:
            if name not in VIOLATIONS:
                raise FixtureError(f"{where}: unknown deliberate violation {name!r}; known {list(VIOLATIONS)}")
        changes = batch.get("changes")
        if not isinstance(changes, list) or not changes:
            raise FixtureError(f"{where}: changes must be a non-empty list")
        for c, change in enumerate(changes):
            if not isinstance(change, dict):
                raise FixtureError(f"{where}.changes[{c}] must be a mapping")
            unknown = set(change) - change_keys
            if unknown:
                raise FixtureError(f"{where}.changes[{c}]: unknown keys {sorted(unknown)}")
    return config, hashlib.sha256(raw).hexdigest()


# Base snapshot ---------------------------------------------------------------

def _sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _conform(table, data):
    """Return the base table cast to the contract schema, or raise.

    Only representation differences are accepted: large_string for string,
    and a UTC timestamp whose zone is spelled differently. Anything else
    (a missing or reordered column, another type) is a contract break.
    """
    expected = lc.arrow_schema(table, lc.MODE_SNAPSHOT)
    if data.schema.names != expected.names:
        raise FixtureError(f"batch 0 {table}: columns {data.schema.names} differ from the contract {expected.names}")
    for actual, wanted in zip(data.schema, expected):
        same = actual.type == wanted.type
        same = same or (pa.types.is_large_string(actual.type) and pa.types.is_string(wanted.type))
        same = same or (pa.types.is_timestamp(actual.type) and pa.types.is_timestamp(wanted.type)
                        and actual.type.unit in ("us", "ns")
                        and actual.type.tz in ("UTC", "+00:00", "Etc/UTC"))
        if not same:
            raise FixtureError(f"batch 0 {table}.{actual.name}: type {actual.type} differs from the contract {wanted.type}")
    return data.cast(expected)


@dataclass
class BaseSnapshot:
    root: Path
    manifest: dict
    _tables: dict = field(default_factory=dict)

    @classmethod
    def open(cls, landing_root):
        root = Path(landing_root) / lc.batch_folder(0)
        manifest_path = root / lc.MANIFEST_NAME
        if not manifest_path.is_file():
            raise FixtureError(f"batch 0 manifest not found at {manifest_path}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        problems = lc.manifest_problems(manifest)
        if problems:
            raise FixtureError("batch 0 manifest is invalid: " + "; ".join(problems))
        if manifest["mode"] != lc.MODE_SNAPSHOT:
            raise FixtureError("batch_seq=000000 is not an INITIAL_SNAPSHOT")
        return cls(root=root, manifest=manifest)

    @property
    def as_of(self):
        return date.fromisoformat(self.manifest["as_of_date"])

    def table(self, name):
        """Batch 0 rows of one table, after checking every file hash and the schema."""
        if name not in self._tables:
            parts = []
            for entry in self.manifest["tables"][name]["files"]:
                path = self.root / entry["path"]
                if not path.is_file():
                    raise FixtureError(f"batch 0 file missing: {path}")
                if _sha256_file(path) != entry["sha256"]:
                    raise FixtureError(f"batch 0 file {entry['path']} does not match its manifest sha256")
                parts.append(pq.read_table(path))
            data = _conform(name, pa.concat_tables(parts))
            if data.num_rows != self.manifest["tables"][name]["row_count"]:
                raise FixtureError(f"batch 0 {name}: {data.num_rows} rows, manifest says "
                                   f"{self.manifest['tables'][name]['row_count']}")
            self._tables[name] = data
        return self._tables[name]


# Table state -----------------------------------------------------------------

class TableState:
    """Current state of one table: batch 0 plus every event applied so far."""

    def __init__(self, table, base):
        self.table = table
        self.key = lc.TABLES[table]["key"]
        self.columns = [name for name, _ in lc.TABLES[table]["columns"]]
        self._base = base
        keys = base.column(self.key).to_pylist()
        if any(k is None for k in keys):
            raise FixtureError(f"batch 0 {table}: null business key")
        self._index = {k: i for i, k in enumerate(keys)}
        if len(self._index) != len(keys):
            raise FixtureError(f"batch 0 {table}: duplicate business keys")
        self._overlay = {}      # key -> row dict, or None when deleted
        self._tombstones = {}   # key -> last row image of a deleted key
        self.sequence = lc.SNAPSHOT_SEQUENCE

    def row(self, key):
        """Current row of a live key, or None when the key is deleted or unknown."""
        if key in self._overlay:
            current = self._overlay[key]
            return dict(current) if current is not None else None
        if key in self._index:
            return self._base.slice(self._index[key], 1).to_pylist()[0]
        return None

    def last_image(self, key):
        """Last known row of a key, live or deleted, or None if never seen."""
        if key in self._tombstones and self._overlay.get(key, 0) is None:
            return dict(self._tombstones[key])
        return self.row(key)

    def is_live(self, key):
        return self.row(key) is not None

    def is_known(self, key):
        return key in self._index or key in self._overlay

    def live_keys(self):
        deleted = {k for k, v in self._overlay.items() if v is None}
        return sorted((set(self._index) | set(self._overlay)) - deleted)

    def all_keys(self):
        return set(self._index) | set(self._overlay)

    def matching_live_keys(self, wanted):
        """Sorted live keys whose current row equals every (column, value) in wanted."""
        import pyarrow.compute as pc

        mask = None
        for column, value in wanted.items():
            col = self._base.column(column)
            test = pc.is_null(col) if value is None else pc.equal(col, pa.scalar(value, type=col.type))
            test = pc.fill_null(test, False)
            mask = test if mask is None else pc.and_(mask, test)
        base_hits = set(self._base.filter(mask).column(self.key).to_pylist())
        base_hits -= set(self._overlay)
        overlay_hits = {k for k, row in self._overlay.items()
                        if row is not None and all(row[c] == v for c, v in wanted.items())}
        return sorted(base_hits | overlay_hits)

    def put(self, key, row):
        self._overlay[key] = dict(row)

    def delete(self, key, last_row):
        self._tombstones[key] = dict(last_row)
        self._overlay[key] = None

    def next_key(self):
        """Next key in the table's existing pattern: same prefix, same digit width, max + 1."""
        prefixes, width, highest = set(), 0, -1
        for key in self.all_keys():
            match = KEY_PATTERN.match(key)
            if not match:
                raise FixtureError(f"{self.table}: key {key!r} has no prefix-plus-number pattern; "
                                   "give the INSERT an explicit key")
            prefixes.add(match["prefix"])
            width = max(width, len(match["number"]))
            highest = max(highest, int(match["number"]))
        if len(prefixes) != 1:
            raise FixtureError(f"{self.table}: keys use several prefixes {sorted(prefixes)}; "
                               "give the INSERT an explicit key")
        number = str(highest + 1)
        if len(number) > width:
            raise FixtureError(f"{self.table}: next key number {number} exceeds the {width}-digit width")
        return prefixes.pop() + number.zfill(width)


# Values ------------------------------------------------------------------------

def coerce(table, column, value):
    """Turn a config value into the Python value the contract type needs, or raise."""
    types = dict(lc.TABLES[table]["columns"])
    if column not in types:
        raise FixtureError(f"{table}: no column {column!r} in the contract")
    kind = types[column]
    where = f"{table}.{column}"
    if value is None:
        if column == lc.TABLES[table]["key"]:
            raise FixtureError(f"{where}: the business key cannot be null")
        return None
    if kind == "string":
        if not isinstance(value, str):
            raise FixtureError(f"{where}: expected text, got {value!r} (quote it in YAML)")
        return value
    if kind in ("int", "bigint"):
        if not isinstance(value, int) or isinstance(value, bool):
            raise FixtureError(f"{where}: expected an integer, got {value!r}")
        if kind == "int" and not INT32_RANGE[0] <= value <= INT32_RANGE[1]:
            raise FixtureError(f"{where}: {value} is outside the int range")
        return value
    if kind == "boolean":
        if not isinstance(value, bool):
            raise FixtureError(f"{where}: expected true or false, got {value!r}")
        return value
    if kind == "date":
        return _as_date(value, where)
    if kind == "timestamp":
        return _as_timestamp(value, where)
    if kind.startswith("decimal("):
        precision, scale = (int(p) for p in kind[8:-1].split(","))
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            raise FixtureError(f"{where}: expected a number, got {value!r}")
        try:
            number = Decimal(str(value))
        except InvalidOperation as exc:
            raise FixtureError(f"{where}: {value!r} is not a number") from exc
        if not number.is_finite():
            raise FixtureError(f"{where}: {value!r} is not a finite number")
        exponent = number.as_tuple().exponent
        if exponent < -scale:
            raise FixtureError(f"{where}: {value} has more than {scale} decimal places")
        quantized = number.quantize(Decimal(1).scaleb(-scale))
        if len(quantized.as_tuple().digits) > precision:
            raise FixtureError(f"{where}: {value} does not fit {kind}")
        return quantized
    raise FixtureError(f"{where}: unsupported contract type {kind}")


def _normalise_row(table, row):
    """Bring every value of a row to the exact Python type the contract column expects."""
    out = {}
    for name, kind in lc.TABLES[table]["columns"]:
        value = row.get(name)
        if kind == "timestamp" and isinstance(value, datetime):
            value = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
        out[name] = value
    return out


# Generation ------------------------------------------------------------------

@dataclass
class Event:
    table: str
    operation: str
    row: dict
    sequence: int
    changed_at: datetime


@dataclass
class Batch:
    batch_seq: int
    batch_id: str
    business_date: date
    previous_batch_id: str
    supersedes_batch_id: str | None
    events: list


def _rng(seed, batch_seq, index, purpose):
    # A string seed is hashed with SHA-512 by random.Random, so the stream is
    # the same on every platform and Python 3 version.
    return random.Random(f"{seed}:{batch_seq}:{index}:{purpose}")


def _resolve_key(state, spec, rng, where):
    """A literal key, or a live key drawn by {pick: random, where: {column: value}} with a seeded RNG."""
    if isinstance(spec, str):
        return spec
    if isinstance(spec, dict) and spec.get("pick") == "random":
        unknown = set(spec) - {"pick", "where"}
        if unknown:
            raise FixtureError(f"{where}: unknown pick options {sorted(unknown)}")
        filters = spec.get("where") or {}
        if not isinstance(filters, dict):
            raise FixtureError(f"{where}: pick where must be a mapping")
        wanted = {col: coerce(state.table, col, val) for col, val in filters.items()}
        if wanted:
            candidates = state.matching_live_keys(wanted)
        else:
            candidates = state.live_keys()
        if not candidates:
            raise FixtureError(f"{where}: no live {state.table} key matches {filters}")
        return rng.choice(candidates)
    raise FixtureError(f"{where}: key must be a literal or {{pick: random}}, got {spec!r}")


def build_batches(config, base):
    """Apply every configured change in order. Returns the batches; writes nothing."""
    seed = config["seed"]
    states = {}
    batches = []
    previous_id = base.manifest["batch_id"]
    previous_date = base.as_of
    expected_seq = 1

    def state(table):
        if table not in states:
            states[table] = TableState(table, base.table(table))
        return states[table]

    for b, spec in enumerate(config["batches"]):
        where_b = f"batches[{b}]"
        seq = spec.get("batch_seq")
        if seq != expected_seq:
            raise FixtureError(f"{where_b}: batch_seq {seq!r}, expected {expected_seq} (no gaps, starting at 1)")
        business_date = _as_date(spec.get("business_date"), f"{where_b}.business_date")
        violations = set(spec.get("deliberate_violations") or [])
        if business_date <= previous_date and "business_date_not_increasing" not in violations:
            raise FixtureError(f"{where_b}: business_date {business_date} must be after {previous_date}")
        batch_id = spec.get("batch_id") or f"{config['batch_id_prefix']}-{seq:06d}"
        supersedes = spec.get("supersedes_batch_id")
        if supersedes is not None and not isinstance(supersedes, str):
            raise FixtureError(f"{where_b}: supersedes_batch_id must be text")
        day_start = datetime.combine(business_date, time(0, 0), tzinfo=timezone.utc)
        day_end = day_start + timedelta(days=1)

        events = []
        for c, change in enumerate(spec["changes"]):
            where = f"{where_b}.changes[{c}]"
            table, op = change.get("table"), change.get("op")
            if table not in lc.CHANGE_TABLES:
                raise FixtureError(f"{where}: table {table!r} has no change batches; allowed {lc.CHANGE_TABLES}")
            if op not in lc.OPERATIONS:
                raise FixtureError(f"{where}: op {op!r} must be one of {list(lc.OPERATIONS)}")
            st = state(table)
            values = change.get("set") or {}
            if not isinstance(values, dict):
                raise FixtureError(f"{where}: set must be a mapping")
            if st.key in values:
                raise FixtureError(f"{where}: set the business key with key, not set")
            rng = _rng(seed, seq, c, "key")

            if op == "INSERT":
                if "key" in change:
                    if not isinstance(change["key"], str):
                        raise FixtureError(f"{where}: an INSERT key must be a literal key (to revive that "
                                           "deleted key) or omitted (next new key); random picks select "
                                           "live keys and apply to UPDATE, DELETE and copy_from")
                    key = change["key"]
                else:
                    key = st.next_key()
                if st.is_live(key):
                    raise FixtureError(f"{where}: INSERT on live {table} key {key}")
                if "copy_from" in change:
                    source_key = _resolve_key(st, change["copy_from"], _rng(seed, seq, c, "copy"), where)
                    template = st.row(source_key)
                    if template is None:
                        raise FixtureError(f"{where}: copy_from {source_key} is not a live {table} key")
                elif st.is_known(key):
                    template = st.last_image(key)
                else:
                    raise FixtureError(f"{where}: INSERT of a new key needs copy_from for the other columns")
                row = dict(template)
            else:
                if "copy_from" in change:
                    raise FixtureError(f"{where}: copy_from applies to INSERT only")
                if "key" not in change:
                    raise FixtureError(f"{where}: {op} needs a key")
                key = _resolve_key(st, change["key"], rng, where)
                current = st.row(key)
                if current is None:
                    raise FixtureError(f"{where}: {op} on {table} key {key}, which is not live")
                row = current
                if op == "DELETE" and values:
                    raise FixtureError(f"{where}: DELETE carries the last row image; set is not allowed")

            for column, value in values.items():
                row[column] = coerce(table, column, value)
            row[st.key] = coerce(table, st.key, key)
            row = _normalise_row(table, row)

            if "changed_at" in change:
                changed_at = _as_timestamp(change["changed_at"], f"{where}.changed_at")
            else:
                changed_at = day_start + timedelta(seconds=c + 1)
            if changed_at >= day_end:
                raise FixtureError(f"{where}: changed_at {changed_at.isoformat()} is after business date {business_date}")
            if (table in TYPE2_TABLES and changed_at.date() != business_date
                    and "type2_changed_at_not_business_date" not in violations):
                raise FixtureError(f"{where}: a {table} change must happen on the business date {business_date}")

            st.sequence += 1
            if op == "DELETE":
                st.delete(key, row)
            else:
                st.put(key, row)
            events.append(Event(table, op, row, st.sequence, changed_at))

        batches.append(Batch(seq, batch_id, business_date, previous_id, supersedes, events))
        previous_id, previous_date, expected_seq = batch_id, max(previous_date, business_date), expected_seq + 1
    return batches


# Writing ---------------------------------------------------------------------

def _table_bytes(table, events, batch_id):
    """Parquet bytes for one table's events, rows in sequence order."""
    schema = lc.arrow_schema(table, lc.MODE_CHANGE)
    columns = {name: [] for name in schema.names}
    for event in sorted(events, key=lambda e: e.sequence):
        for name, _ in lc.TABLES[table]["columns"]:
            columns[name].append(event.row[name])
        columns["_operation"].append(event.operation)
        columns["_event_id"].append(f"{table}-{event.sequence:010d}")
        columns["_source_sequence"].append(event.sequence)
        columns["_source_changed_at"].append(event.changed_at)
        columns["_batch_id"].append(batch_id)
    data = pa.table(columns, schema=schema)
    sink = pa.BufferOutputStream()
    pq.write_table(data, sink, compression="snappy", version="2.6", store_schema=False,
                   write_statistics=True, use_dictionary=True)
    return sink.getvalue().to_pybytes()


def render_batch(batch, producer):
    """(relative path, bytes) for every data file, and the manifest text, of one batch."""
    files, tables = [], {}
    for table in sorted({e.table for e in batch.events}):
        events = [e for e in batch.events if e.table == table]
        payload = _table_bytes(table, events, batch.batch_id)
        path = f"{table}/{lc.DATA_FILE_NAME}"
        files.append((path, payload))
        counts = {op: 0 for op in lc.OPERATIONS}
        for e in events:
            counts[e.operation] += 1
        tables[table] = {
            "files": [{"path": path, "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}],
            "row_count": len(events),
            "min_sequence": min(e.sequence for e in events),
            "max_sequence": max(e.sequence for e in events),
            "operation_counts": counts,
        }
    manifest = {
        "contract_version": lc.CONTRACT_VERSION,
        "batch_id": batch.batch_id,
        "batch_seq": batch.batch_seq,
        "mode": lc.MODE_CHANGE,
        "business_date": batch.business_date.isoformat(),
        "as_of_date": None,
        "previous_batch_id": batch.previous_batch_id,
        "supersedes_batch_id": batch.supersedes_batch_id,
        "producer": producer,
        "content_hash": lc.content_hash(tables),
        "tables": tables,
    }
    problems = lc.manifest_problems(manifest)
    if problems:
        raise FixtureError(f"batch {batch.batch_seq}: generated manifest is invalid: {'; '.join(problems)}")
    return files, lc.manifest_json(manifest)


def generate(config_path, landing_root, dry_run=False):
    """Build, check and write every configured batch. Returns the manifest texts by batch_seq."""
    landing_root = Path(landing_root)
    config, config_sha = load_config(config_path)
    base = BaseSnapshot.open(landing_root)
    batches = build_batches(config, base)

    targets = [landing_root / lc.batch_folder(b.batch_seq) for b in batches]
    existing = [str(t) for t in targets if t.exists()]
    if existing:
        raise FixtureError(f"batch folders already exist and are never overwritten: {existing}")

    producer = {
        "name": GENERATOR_NAME,
        "version": GENERATOR_VERSION,
        "seed": config["seed"],
        "config_sha256": config_sha,
        "base_batch_id": base.manifest["batch_id"],
        "base_content_hash": base.manifest["content_hash"],
        "pyarrow_version": pa.__version__,
    }
    rendered = [(target, *render_batch(batch, producer)) for target, batch in zip(targets, batches)]
    if dry_run:
        return {b.batch_seq: text for b, (_, _, text) in zip(batches, rendered)}

    written = {}
    for batch, (target, files, manifest_text) in zip(batches, rendered):
        for relative, payload in files:
            path = target / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "xb") as handle:
                handle.write(payload)
        # The manifest goes last: a batch folder without it does not exist yet.
        with open(target / lc.MANIFEST_NAME, "x", encoding="utf-8", newline="\n") as handle:
            handle.write(manifest_text)
        written[batch.batch_seq] = manifest_text
    return written


def main(argv=None):
    parser = argparse.ArgumentParser(description="Generate deterministic landing change batches.")
    parser.add_argument("--config", required=True, help="generator config (YAML)")
    parser.add_argument("--landing-root", required=True,
                        help="local folder holding batch_seq=000000; new batch folders are written here")
    parser.add_argument("--dry-run", action="store_true", help="build and check everything, write nothing")
    args = parser.parse_args(argv)
    try:
        manifests = generate(args.config, args.landing_root, dry_run=args.dry_run)
    except FixtureError as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1
    verb = "checked" if args.dry_run else "written"
    for seq in sorted(manifests):
        print(f"{lc.batch_folder(seq)} {verb}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
