"""The landing contract matches the source schema, its notebook copies, and its own manifest rules."""

from __future__ import annotations

import copy
import re
import unittest
from pathlib import Path

from ingestion import landing_contract as lc
from ingestion.tests import support

REPO = Path(__file__).resolve().parents[2]
SOURCE_SCHEMA = REPO / "sql" / "01_create_schema.sql"
NOTEBOOKS = [
    REPO / "fabric" / "nb_landing_export_batch0.Notebook" / "notebook-content.py",
    REPO / "fabric" / "nb_landing_gate_batch0.Notebook" / "notebook-content.py",
]

# PostgreSQL type -> Spark type that a JDBC read produces and bronze holds.
PG_TO_SPARK = [
    (re.compile(r"^(VARCHAR\(\d+\)|TEXT)$"), lambda m: "string"),
    (re.compile(r"^INTEGER$"), lambda m: "int"),
    (re.compile(r"^NUMERIC\((\d+),(\d+)\)$"), lambda m: f"decimal({m[1]},{m[2]})"),
    (re.compile(r"^DATE$"), lambda m: "date"),
    (re.compile(r"^TIMESTAMP$"), lambda m: "timestamp"),
    (re.compile(r"^BOOLEAN$"), lambda m: "boolean"),
]


def parse_source_schema():
    """{table: (primary key, [(column, spark type)])} from sql/01_create_schema.sql."""
    text = SOURCE_SCHEMA.read_text(encoding="utf-8")
    tables = {}
    for name, body in re.findall(r"CREATE TABLE (\w+) \((.*?)\n\);", text, re.S):
        columns, key = [], None
        for line in body.splitlines():
            line = line.split("--")[0].strip().rstrip(",")
            if not line:
                continue
            parts = line.split()
            column, pg_type = parts[0], parts[1]
            for pattern, to_spark in PG_TO_SPARK:
                match = pattern.match(pg_type)
                if match:
                    columns.append((column, to_spark(match)))
                    break
            else:
                raise AssertionError(f"{name}.{column}: unmapped type {pg_type}")
            if "PRIMARY KEY" in line:
                key = column
        tables[name] = (key, columns)
    return tables


class ContractMatchesSource(unittest.TestCase):
    def test_tables_columns_types_and_keys(self):
        source = parse_source_schema()
        self.assertEqual(list(source), list(lc.TABLES), "table set or order differs from the source schema")
        for table, (key, columns) in source.items():
            with self.subTest(table=table):
                self.assertEqual(lc.TABLES[table]["key"], key)
                self.assertEqual(lc.TABLES[table]["columns"], columns)

    def test_change_tables_are_the_approved_seven(self):
        self.assertEqual(sorted(lc.CHANGE_TABLES), sorted(
            ["drivers", "trucks", "customers", "loads", "trips", "delivery_events", "fuel_purchases"]))

    def test_metadata_columns_do_not_collide_with_source_columns(self):
        names = {name for name, _ in lc.METADATA_COLUMNS}
        for table, spec in lc.TABLES.items():
            self.assertFalse(names & {c for c, _ in spec["columns"]}, table)
        self.assertTrue(all(name.startswith("_") for name in names))

    def test_change_file_schema_appends_metadata(self):
        schema = lc.arrow_schema("drivers", lc.MODE_CHANGE)
        self.assertEqual(schema.names[-5:], [n for n, _ in lc.METADATA_COLUMNS])
        self.assertEqual(str(schema.field("_source_changed_at").type), "timestamp[us, tz=UTC]")


class NotebookCopiesMatch(unittest.TestCase):
    def test_each_notebook_holds_the_block_verbatim(self):
        block = lc.block_text((REPO / "ingestion" / "landing_contract.py").read_text(encoding="utf-8"))
        for path in NOTEBOOKS:
            with self.subTest(notebook=path.parent.name):
                self.assertTrue(path.is_file(), f"{path} is missing")
                self.assertEqual(lc.block_text(path.read_text(encoding="utf-8")), block,
                                 "paste the block from ingestion/landing_contract.py into this notebook")


def snapshot_manifest():
    tables = {}
    for i, table in enumerate(lc.TABLES):
        tables[table] = {"files": [{"path": f"{table}/part-00000.parquet", "bytes": 100 + i,
                                    "sha256": f"{i:064x}"}],
                         "row_count": 5, "min_sequence": 0, "max_sequence": 0, "operation_counts": None}
    return {"contract_version": lc.CONTRACT_VERSION, "batch_id": "logistics-000000", "batch_seq": 0,
            "mode": lc.MODE_SNAPSHOT, "business_date": None, "as_of_date": support.AS_OF,
            "previous_batch_id": None, "supersedes_batch_id": None, "producer": {"name": "test"},
            "content_hash": lc.content_hash(tables), "tables": tables}


def change_manifest():
    tables = {"drivers": {"files": [{"path": "drivers/part-00000.parquet", "bytes": 10, "sha256": "a" * 64}],
                          "row_count": 2, "min_sequence": 1, "max_sequence": 2,
                          "operation_counts": {"INSERT": 0, "UPDATE": 2, "DELETE": 0}}}
    return {"contract_version": lc.CONTRACT_VERSION, "batch_id": "logistics-000001", "batch_seq": 1,
            "mode": lc.MODE_CHANGE, "business_date": "2025-01-02", "as_of_date": None,
            "previous_batch_id": "logistics-000000", "supersedes_batch_id": None, "producer": {"name": "test"},
            "content_hash": lc.content_hash(tables), "tables": tables}


def rehash(manifest):
    manifest["content_hash"] = lc.content_hash(manifest["tables"])
    return manifest


class ManifestRules(unittest.TestCase):
    def test_valid_manifests_pass(self):
        self.assertEqual(lc.manifest_problems(snapshot_manifest()), [])
        self.assertEqual(lc.manifest_problems(change_manifest()), [])

    def test_content_hash_ignores_order_and_formatting(self):
        tables = snapshot_manifest()["tables"]
        reordered = dict(reversed(list(tables.items())))
        self.assertEqual(lc.content_hash(tables), lc.content_hash(reordered))

    def assert_rejected(self, manifest, fragment):
        problems = lc.manifest_problems(manifest)
        self.assertTrue(any(fragment in p for p in problems), f"expected {fragment!r} in {problems}")

    def test_rejections(self):
        cases = []

        m = snapshot_manifest(); m["content_hash"] = "0" * 64
        cases.append((m, "content_hash"))
        m = snapshot_manifest(); del m["tables"]["routes"]; rehash(m)
        cases.append((m, "must list all 12 tables"))
        m = snapshot_manifest(); m["batch_seq"] = 1
        cases.append((m, "must be batch_seq 0"))
        m = snapshot_manifest(); m["tables"]["routes"]["max_sequence"] = 3
        cases.append((m, "snapshot sequences"))
        m = snapshot_manifest(); m["tables"]["routes"]["files"][0]["path"] = "trips/part-00000.parquet"; rehash(m)
        cases.append((m, "must be routes/<name>.parquet"))
        m = snapshot_manifest(); m["as_of_date"] = "31-12-2024"
        cases.append((m, "as_of_date"))
        m = change_manifest(); m["tables"]["routes"] = copy.deepcopy(m["tables"]["drivers"]); rehash(m)
        cases.append((m, "without change batches"))
        m = change_manifest(); m["tables"]["drivers"]["operation_counts"]["INSERT"] = 1
        cases.append((m, "sum to row_count"))
        m = change_manifest(); m["tables"]["drivers"]["max_sequence"] = 1
        cases.append((m, "cannot have unique sequences"))
        m = change_manifest(); m["previous_batch_id"] = None
        cases.append((m, "previous_batch_id"))
        m = change_manifest(); m["mode"] = "FULL"
        cases.append((m, "is not INITIAL_SNAPSHOT"))
        m = change_manifest(); m["extra"] = 1
        cases.append((m, "unexpected keys"))
        m = change_manifest(); del m["producer"]
        cases.append((m, "missing keys"))
        m = change_manifest(); m["tables"]["drivers"]["files"][0]["sha256"] = "XYZ"
        cases.append((m, "sha256 must be"))

        for i, (manifest, fragment) in enumerate(cases):
            with self.subTest(case=i, expect=fragment):
                self.assert_rejected(manifest, fragment)


if __name__ == "__main__":
    unittest.main()
