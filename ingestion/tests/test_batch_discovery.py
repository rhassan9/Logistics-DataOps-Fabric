"""Batch discovery: folder names, complete reads, registrations, and the notebook copy."""

from __future__ import annotations

import json
import re
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from ingestion import batch_discovery as bd
from ingestion import landing_contract as lc
from ingestion.tests.test_landing_contract import change_manifest, rehash, snapshot_manifest

REPO = Path(__file__).resolve().parents[2]
NOTEBOOK = REPO / "fabric" / "nb_batch_discover.Notebook" / "notebook-content.py"
REGISTRATION_TABLE = (REPO / "fabric" / "wh_logistics_gold.Warehouse" / "log" / "Tables"
                      / "batch_manifest.sql")

LANDED = datetime(2026, 11, 27, 9, 30, tzinfo=timezone.utc)
SEEN = datetime(2026, 11, 27, 10, 0, tzinfo=timezone.utc)


def register(manifest, folder="batch_seq=000000", raw=None):
    content = raw if raw is not None else lc.manifest_json(manifest).encode("utf-8")
    return bd.registration(folder, f"Files/landing/{folder}/{lc.MANIFEST_NAME}", content, LANDED, SEEN, "run-1")


class FolderNames(unittest.TestCase):
    def test_batch_folders(self):
        self.assertEqual(bd.folder_seq("batch_seq=000000"), 0)
        self.assertEqual(bd.folder_seq("batch_seq=000042/"), 42)
        for name in ("batch_seq=42", "batch_seq=0000001", "landing_staging", "batch_seq=00000a", "_tmp"):
            with self.subTest(name=name):
                self.assertIsNone(bd.folder_seq(name))

    def test_folder_names_round_trip_with_the_contract(self):
        for seq in (0, 1, 999999):
            self.assertEqual(bd.folder_seq(lc.batch_folder(seq)), seq)


class CompleteReads(unittest.TestCase):
    def test_complete(self):
        self.assertIsNone(bd.read_problem(b"x" * 10, 10, 10))

    def test_short_read_or_wrong_length_or_unlisted(self):
        self.assertIn("read 4 bytes", bd.read_problem(b"x" * 4, 10, 10))
        self.assertIn("reader length 4", bd.read_problem(b"x" * 10, 10, 4))
        self.assertEqual(bd.read_problem(b"", None, 0), "not listed")


class Registrations(unittest.TestCase):
    def test_columns_follow_the_warehouse_table_in_order(self):
        ddl = REGISTRATION_TABLE.read_text(encoding="utf-8")
        columns = tuple(re.findall(r"^\s*\[(\w+)\]\s", ddl, re.M))
        self.assertEqual(columns, bd.REGISTRATION_COLUMNS)

    def test_text_limits_follow_the_warehouse_table(self):
        ddl = REGISTRATION_TABLE.read_text(encoding="utf-8")
        widths = {c: int(w) for c, w in re.findall(r"^\s*\[(\w+)\]\s+VARCHAR \((\d+)\)", ddl, re.M)}
        self.assertEqual({c: widths[c] for c in bd.TEXT_LIMITS}, bd.TEXT_LIMITS)
        self.assertLessEqual(set(widths) - set(bd.TEXT_LIMITS), {"content_hash"})

    def test_valid_snapshot(self):
        manifest = snapshot_manifest()
        row = register(manifest)
        self.assertEqual(set(row), set(bd.REGISTRATION_COLUMNS))
        self.assertTrue(row["is_valid"])
        self.assertIsNone(row["problems"])
        self.assertEqual((row["batch_seq"], row["batch_id"], row["batch_mode"]),
                         (0, "logistics-000000", lc.MODE_SNAPSHOT))
        self.assertEqual(row["as_of_date"], date(2024, 12, 31))
        self.assertIsNone(row["business_date"])
        self.assertEqual(row["content_hash"], manifest["content_hash"])
        self.assertEqual((row["table_count"], row["row_count"]), (12, 60))
        self.assertEqual(json.loads(row["producer"]), {"name": "test"})
        self.assertEqual((row["landed_at"], row["discovered_at"], row["discovery_run_id"]),
                         (LANDED, SEEN, "run-1"))

    def test_valid_change_batch(self):
        row = register(change_manifest(), folder="batch_seq=000001")
        self.assertTrue(row["is_valid"], row["problems"])
        self.assertEqual((row["business_date"], row["previous_batch_id"]), (date(2025, 1, 2), "logistics-000000"))

    def test_manifest_breaking_the_contract_is_registered_invalid(self):
        manifest = snapshot_manifest()
        manifest["content_hash"] = "0" * 64
        row = register(manifest)
        self.assertFalse(row["is_valid"])
        self.assertIn("content_hash", row["problems"])
        self.assertEqual(row["content_hash"], "0" * 64)

    def test_manifest_in_the_wrong_folder_is_invalid(self):
        manifest = change_manifest()
        row = register(manifest, folder="batch_seq=000002")
        self.assertFalse(row["is_valid"])
        self.assertIn("not the folder's 2", row["problems"])
        self.assertEqual(row["batch_seq"], 2)

    def test_unparseable_manifest_is_invalid_and_named_by_its_folder(self):
        for raw in (b"{not json", b"\xff\xfe", b"[1, 2]"):
            with self.subTest(raw=raw):
                row = register(None, folder="batch_seq=000003", raw=raw)
                self.assertFalse(row["is_valid"])
                self.assertEqual(row["batch_id"], "batch_seq=000003")
                self.assertIsNone(row["content_hash"])
                self.assertIsNone(row["table_count"])

    def test_long_text_is_clipped_to_the_column(self):
        manifest = change_manifest()
        manifest["batch_id"] = "b" * 300
        manifest["producer"] = {"name": "p" * 400}
        row = register(rehash(manifest), folder="batch_seq=000001")
        self.assertEqual(len(row["batch_id"]), 128)
        self.assertEqual(len(row["producer"]), 256)

    def test_not_a_batch_folder_is_refused(self):
        with self.assertRaises(ValueError):
            register(snapshot_manifest(), folder="landing_staging")


class NewRegistrations(unittest.TestCase):
    def test_only_unregistered_folder_content_is_new(self):
        first = register(snapshot_manifest())
        second = register(change_manifest(), folder="batch_seq=000001")
        self.assertEqual(bd.new_registrations([second, first], set()), [first, second])
        self.assertEqual(bd.new_registrations([first, second], {bd.registration_key(first)}), [second])

    def test_changed_manifest_in_a_registered_folder_is_new(self):
        first = register(snapshot_manifest())
        changed = snapshot_manifest()
        changed["producer"] = {"name": "other"}
        changed["tables"]["routes"]["files"][0]["sha256"] = "f" * 64
        again = register(rehash(changed))
        self.assertEqual(bd.new_registrations([again], {bd.registration_key(first)}), [again])

    def test_invalid_registration_is_registered_once(self):
        row = register(None, folder="batch_seq=000003", raw=b"{not json")
        self.assertEqual(bd.registration_key(row), (3, "batch_seq=000003", ""))
        self.assertEqual(bd.new_registrations([row], {bd.registration_key(row)}), [])


class NotebookCopy(unittest.TestCase):
    def test_notebook_holds_the_block_verbatim(self):
        block = bd.block_text((REPO / "ingestion" / "batch_discovery.py").read_text(encoding="utf-8"))
        self.assertTrue(NOTEBOOK.is_file(), f"{NOTEBOOK} is missing")
        self.assertEqual(bd.block_text(NOTEBOOK.read_text(encoding="utf-8")), block,
                         "paste the block from ingestion/batch_discovery.py into nb_batch_discover")


if __name__ == "__main__":
    unittest.main()
