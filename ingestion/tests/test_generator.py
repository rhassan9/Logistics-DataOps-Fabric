"""The generator writes contract-conformant, deterministic batches and refuses rule breaks."""

from __future__ import annotations

import hashlib
import shutil
import tempfile
import textwrap
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock

import pyarrow.parquet as pq

from ingestion.fixtures import generator as gen
from ingestion import landing_contract as lc
from ingestion.tests import support

BASE_CONFIG = """
seed: 20250102
batches:
  - batch_seq: 1
    business_date: 2025-01-02
    changes:
      - {table: customers, op: UPDATE, key: CUST00002, set: {account_status: Inactive}}
      - {table: customers, op: UPDATE, key: CUST00002, set: {credit_terms_days: 45}}
      - {table: drivers, op: UPDATE, key: DRV00003, set: {employment_status: Terminated, termination_date: 2025-01-02}}
      - {table: loads, op: INSERT, copy_from: LOA00001, set: {load_date: 2025-01-02, revenue: 1234.5}}
      - {table: trips, op: DELETE, key: TRI00004}
  - batch_seq: 2
    business_date: 2025-01-03
    changes:
      - {table: trips, op: INSERT, key: TRI00004}
      - {table: fuel_purchases, op: UPDATE, key: {pick: random}, set: {gallons: 12.25}}
      - {table: delivery_events, op: UPDATE, key: DEL00001, set: {detention_minutes: 15}, changed_at: "2024-06-01T08:00:00"}
"""


class GeneratorCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)
        self.landing = self.tmp / "landing"
        support.write_snapshot(self.landing)

    def config(self, text, name="config.yaml"):
        path = self.tmp / name
        path.write_text(textwrap.dedent(text), encoding="utf-8")
        return path

    def run_generator(self, text, landing=None):
        return gen.generate(self.config(text), landing or self.landing)

    def assert_fails(self, text, fragment):
        with self.assertRaises(gen.FixtureError) as caught:
            self.run_generator(text)
        self.assertIn(fragment, str(caught.exception))
        # Nothing at all is written when a rule fails.
        self.assertEqual(sorted(p.name for p in self.landing.iterdir()), [lc.batch_folder(0)])


class Determinism(GeneratorCase):
    def test_same_inputs_give_identical_bytes(self):
        other = self.tmp / "landing_copy"
        shutil.copytree(self.landing, other)
        self.run_generator(BASE_CONFIG)
        self.run_generator(BASE_CONFIG, landing=other)
        for seq in (1, 2):
            first = support.tree_bytes(self.landing / lc.batch_folder(seq))
            second = support.tree_bytes(other / lc.batch_folder(seq))
            self.assertEqual(first, second, f"batch {seq} differs between runs")

    def test_different_seed_changes_random_pick_only_where_used(self):
        self.run_generator(BASE_CONFIG)
        other = self.tmp / "landing_seed"
        support.write_snapshot(other)
        self.run_generator(BASE_CONFIG.replace("seed: 20250102", "seed: 7"), landing=other)
        first = support.tree_bytes(self.landing / lc.batch_folder(1))
        second = support.tree_bytes(other / lc.batch_folder(1))
        # Batch 1 has no random pick, so only the manifest (which records the seed) differs.
        self.assertEqual({k: v for k, v in first.items() if k != lc.MANIFEST_NAME},
                         {k: v for k, v in second.items() if k != lc.MANIFEST_NAME})


class Conformance(GeneratorCase):
    def setUp(self):
        super().setUp()
        self.manifests = self.run_generator(BASE_CONFIG)

    def test_files_match_contract_schema_and_manifest(self):
        for seq in (1, 2):
            manifest, _ = support.read_batch(self.landing, seq)
            self.assertEqual(lc.manifest_problems(manifest), [])
            root = self.landing / lc.batch_folder(seq)
            listed = {e["path"] for t in manifest["tables"].values() for e in t["files"]}
            on_disk = {str(p.relative_to(root)) for p in root.rglob("*.parquet")}
            self.assertEqual(listed, on_disk)
            for table, spec in manifest["tables"].items():
                entry = spec["files"][0]
                payload = (root / entry["path"]).read_bytes()
                self.assertEqual(entry["sha256"], hashlib.sha256(payload).hexdigest())
                self.assertEqual(entry["bytes"], len(payload))
                data = pq.read_table(root / entry["path"])
                self.assertEqual(data.schema, lc.arrow_schema(table, lc.MODE_CHANGE))
                self.assertEqual(data.num_rows, spec["row_count"])

    def test_manifest_chain_and_producer(self):
        m1, _ = support.read_batch(self.landing, 1)
        m2, _ = support.read_batch(self.landing, 2)
        self.assertEqual(m1["previous_batch_id"], "logistics-000000")
        self.assertEqual(m2["previous_batch_id"], m1["batch_id"])
        self.assertEqual(m1["business_date"], "2025-01-02")
        self.assertEqual(m1["producer"]["version"], gen.GENERATOR_VERSION)
        base, _ = support.read_batch(self.landing, 0)
        self.assertEqual(m1["producer"]["base_content_hash"], base["content_hash"])
        self.assertEqual(sorted(m1["tables"]), ["customers", "drivers", "loads", "trips"])

    def test_sequences_start_at_one_and_increase_per_table(self):
        seen = {}
        for seq in (1, 2):
            manifest, rows = support.read_batch(self.landing, seq)
            for table, table_rows in rows.items():
                sequences = [r["_source_sequence"] for r in table_rows]
                self.assertEqual(sequences, sorted(sequences))
                self.assertEqual(manifest["tables"][table]["min_sequence"], sequences[0])
                self.assertEqual(manifest["tables"][table]["max_sequence"], sequences[-1])
                expected_start = seen.get(table, 0) + 1
                self.assertEqual(sequences, list(range(expected_start, expected_start + len(sequences))))
                seen[table] = sequences[-1]
        self.assertEqual(seen["trips"], 2)

    def test_update_carries_full_image_and_second_update_builds_on_first(self):
        _, rows = support.read_batch(self.landing, 1)
        first, second = rows["customers"]
        original = {r["customer_id"]: r for r in support.snapshot_rows("customers")}["CUST00002"]
        self.assertEqual(first["account_status"], "Inactive")
        self.assertEqual(first["credit_terms_days"], original["credit_terms_days"])
        self.assertEqual(second["account_status"], "Inactive")
        self.assertEqual(second["credit_terms_days"], 45)
        self.assertEqual(second["customer_name"], original["customer_name"])
        self.assertEqual([r["_operation"] for r in rows["customers"]], ["UPDATE", "UPDATE"])
        self.assertEqual({r["_batch_id"] for r in rows["customers"]}, {"logistics-000001"})
        self.assertEqual(len({r["_event_id"] for r in rows["customers"]}), 2)

    def test_insert_takes_next_key_and_copied_values(self):
        _, rows = support.read_batch(self.landing, 1)
        (load,) = rows["loads"]
        self.assertEqual(load["load_id"], "LOA00007")
        self.assertEqual(load["_operation"], "INSERT")
        self.assertEqual(load["load_date"], date(2025, 1, 2))
        self.assertEqual(load["revenue"], Decimal("1234.50"))
        self.assertEqual(load["customer_id"], "customer_id-1")

    def test_delete_carries_last_image_and_reinsert_revives_it(self):
        _, rows1 = support.read_batch(self.landing, 1)
        _, rows2 = support.read_batch(self.landing, 2)
        (deleted,) = rows1["trips"]
        (revived,) = rows2["trips"]
        original = {r["trip_id"]: r for r in support.snapshot_rows("trips")}["TRI00004"]
        self.assertEqual(deleted["_operation"], "DELETE")
        self.assertEqual({k: deleted[k] for k in original}, original)
        self.assertEqual(revived["_operation"], "INSERT")
        self.assertEqual({k: revived[k] for k in original}, original)

    def test_changed_at_defaults_inside_the_business_day_and_late_change_kept(self):
        _, rows1 = support.read_batch(self.landing, 1)
        _, rows2 = support.read_batch(self.landing, 2)
        (driver,) = rows1["drivers"]
        self.assertEqual(driver["_source_changed_at"].date(), date(2025, 1, 2))
        self.assertEqual(driver["termination_date"], date(2025, 1, 2))
        (late,) = rows2["delivery_events"]
        self.assertEqual(late["_source_changed_at"], datetime(2024, 6, 1, 8, 0, tzinfo=timezone.utc))


class Rules(GeneratorCase):
    def test_batch_seq_must_start_at_one_without_gaps(self):
        self.assert_fails("""
            seed: 1
            batches:
              - {batch_seq: 2, business_date: 2025-01-02, changes: [{table: trips, op: DELETE, key: TRI00001}]}
            """, "expected 1")

    def test_first_business_date_must_follow_as_of(self):
        self.assert_fails("""
            seed: 1
            batches:
              - {batch_seq: 1, business_date: 2024-12-31, changes: [{table: trips, op: DELETE, key: TRI00001}]}
            """, "must be after 2024-12-31")

    def test_business_dates_must_increase_unless_declared(self):
        text = """
            seed: 1
            batches:
              - {batch_seq: 1, business_date: 2025-01-03, changes: [{table: trips, op: DELETE, key: TRI00001}]}
              - {batch_seq: 2, business_date: 2025-01-02, VIOLATION changes: [{table: trips, op: DELETE, key: TRI00002}]}
            """
        self.assert_fails(text.replace("VIOLATION ", ""), "must be after 2025-01-03")
        self.run_generator(text.replace("VIOLATION ", "deliberate_violations: [business_date_not_increasing], "))
        self.assertTrue((self.landing / lc.batch_folder(2) / lc.MANIFEST_NAME).is_file())

    def test_type2_change_off_business_date_is_rejected_unless_declared(self):
        text = """
            seed: 1
            batches:
              - batch_seq: 1
                business_date: 2025-01-02
                VIOLATION
                changes:
                  - {table: drivers, op: UPDATE, key: DRV00001, set: {home_terminal: Dallas}, changed_at: "2025-01-01T09:00:00"}
            """
        self.assert_fails(text.replace("VIOLATION", ""), "must happen on the business date")
        self.run_generator(text.replace("VIOLATION", "deliberate_violations: [type2_changed_at_not_business_date]"))
        _, rows = support.read_batch(self.landing, 1)
        self.assertEqual(rows["drivers"][0]["_source_changed_at"].date(), date(2025, 1, 1))

    def test_change_after_the_business_day_is_rejected(self):
        self.assert_fails("""
            seed: 1
            batches:
              - batch_seq: 1
                business_date: 2025-01-02
                changes:
                  - {table: loads, op: UPDATE, key: LOA00001, set: {pieces: 3}, changed_at: "2025-01-03T00:00:00"}
            """, "is after business date")

    def test_update_and_delete_need_a_live_key(self):
        self.assert_fails("""
            seed: 1
            batches:
              - {batch_seq: 1, business_date: 2025-01-02, changes: [{table: trips, op: UPDATE, key: TRI09999, set: {trip_status: x}}]}
            """, "which is not live")
        self.assert_fails("""
            seed: 1
            batches:
              - batch_seq: 1
                business_date: 2025-01-02
                changes:
                  - {table: trips, op: DELETE, key: TRI00001}
                  - {table: trips, op: UPDATE, key: TRI00001, set: {trip_status: x}}
            """, "which is not live")

    def test_insert_on_live_key_is_rejected(self):
        self.assert_fails("""
            seed: 1
            batches:
              - {batch_seq: 1, business_date: 2025-01-02, changes: [{table: trips, op: INSERT, key: TRI00001, copy_from: TRI00002}]}
            """, "INSERT on live")

    def test_new_key_insert_needs_copy_from(self):
        self.assert_fails("""
            seed: 1
            batches:
              - {batch_seq: 1, business_date: 2025-01-02, changes: [{table: trips, op: INSERT}]}
            """, "needs copy_from")

    def test_delete_cannot_set_values(self):
        self.assert_fails("""
            seed: 1
            batches:
              - {batch_seq: 1, business_date: 2025-01-02, changes: [{table: trips, op: DELETE, key: TRI00001, set: {trip_status: x}}]}
            """, "set is not allowed")

    def test_reference_only_tables_have_no_change_batches(self):
        self.assert_fails("""
            seed: 1
            batches:
              - {batch_seq: 1, business_date: 2025-01-02, changes: [{table: routes, op: DELETE, key: ROU00001}]}
            """, "has no change batches")

    def test_values_are_checked_against_contract_types(self):
        cases = [
            ("{revenue: 1.234}", "more than 2 decimal places"),
            ("{revenue: 1234567890123.5}", "does not fit decimal(14,2)"),
            ("{pieces: '3'}", "expected an integer"),
            ("{pieces: 3000000000}", "outside the int range"),
            ("{load_status: 7}", "expected text"),
            ("{load_date: '2025-13-01'}", "expected a date"),
            ("{load_date: 2025-13-01}", "not valid YAML"),
            ("{no_such_column: 1}", "no column"),
        ]
        for values, fragment in cases:
            with self.subTest(values=values):
                self.assert_fails(f"""
                    seed: 1
                    batches:
                      - {{batch_seq: 1, business_date: 2025-01-02, changes: [{{table: loads, op: UPDATE, key: LOA00001, set: {values}}}]}}
                    """, fragment)

    def test_unknown_config_keys_and_violations_are_rejected(self):
        self.assert_fails("seed: 1\nbatchez: []\n", "unknown keys")
        self.assert_fails("""
            seed: 1
            batches:
              - {batch_seq: 1, business_date: 2025-01-02, deliberate_violations: [anything], changes: [{table: trips, op: DELETE, key: TRI00001}]}
            """, "unknown deliberate violation")


class Picks(GeneratorCase):
    def test_random_pick_respects_where_and_is_repeatable(self):
        text = """
            seed: 42
            batches:
              - batch_seq: 1
                business_date: 2025-01-02
                changes:
                  - {table: drivers, op: UPDATE, key: {pick: random, where: {years_experience: 30}}, set: {cdl_class: B}}
            """
        self.run_generator(text)
        _, rows = support.read_batch(self.landing, 1)
        self.assertEqual(rows["drivers"][0]["driver_id"], "DRV00003")

    def test_random_pick_is_refused_as_an_insert_key(self):
        for key in ("{pick: random}", "{pick: random, where: {trip_status: trip_status-1}}"):
            with self.subTest(key=key):
                self.assert_fails(f"""
                    seed: 1
                    batches:
                      - {{batch_seq: 1, business_date: 2025-01-02, changes: [{{table: trips, op: INSERT, key: {key}, copy_from: TRI00001}}]}}
                    """, "an INSERT key must be a literal key")

    def test_random_copy_from_on_insert_works_for_every_seed(self):
        for seed in range(20):
            with self.subTest(seed=seed):
                landing = self.tmp / f"landing_{seed}"
                support.write_snapshot(landing)
                gen.generate(self.config(f"""
                    seed: {seed}
                    batches:
                      - batch_seq: 1
                        business_date: 2025-01-02
                        changes:
                          - {{table: trips, op: INSERT, copy_from: {{pick: random, where: {{trip_status: trip_status-2}}}}}}
                    """, name=f"seed_{seed}.yaml"), landing)
                _, rows = support.read_batch(landing, 1)
                (trip,) = rows["trips"]
                self.assertEqual(trip["trip_id"], "TRI00007")
                self.assertEqual(trip["load_id"], "load_id-2")

    def test_pick_with_no_candidate_fails(self):
        self.assert_fails("""
            seed: 1
            batches:
              - batch_seq: 1
                business_date: 2025-01-02
                changes:
                  - {table: drivers, op: UPDATE, key: {pick: random, where: {years_experience: 999}}, set: {cdl_class: B}}
            """, "no live drivers key matches")


class Safety(GeneratorCase):
    def test_existing_batch_folder_is_never_overwritten(self):
        (self.landing / lc.batch_folder(2)).mkdir()
        with self.assertRaises(gen.FixtureError) as caught:
            self.run_generator(BASE_CONFIG)
        self.assertIn("never overwritten", str(caught.exception))
        self.assertFalse((self.landing / lc.batch_folder(1)).exists())

    def test_tampered_base_file_is_refused(self):
        path = self.landing / lc.batch_folder(0) / "trips" / lc.DATA_FILE_NAME
        path.write_bytes(path.read_bytes() + b"x")
        self.assert_fails("""
            seed: 1
            batches:
              - {batch_seq: 1, business_date: 2025-01-02, changes: [{table: trips, op: DELETE, key: TRI00001}]}
            """, "does not match its manifest sha256")

    def test_base_with_wrong_type_is_refused(self):
        rows = support.snapshot_rows("trips")
        for row in rows:
            row["actual_distance_miles"] = str(row["actual_distance_miles"])
        broken = self.tmp / "broken"
        with mock.patch.object(lc, "arrow_schema", side_effect=self._loose_schema):
            support.write_snapshot(broken, overrides={"trips": rows})
        with self.assertRaises(gen.FixtureError) as caught:
            gen.generate(self.config("""
                seed: 1
                batches:
                  - {batch_seq: 1, business_date: 2025-01-02, changes: [{table: trips, op: DELETE, key: TRI00001}]}
                """), broken)
        self.assertIn("differs from the contract", str(caught.exception))

    @staticmethod
    def _loose_schema(table, mode, _real=lc.arrow_schema):
        import pyarrow as pa
        schema = _real(table, mode)
        if table != "trips":
            return schema
        index = schema.get_field_index("actual_distance_miles")
        return schema.set(index, pa.field("actual_distance_miles", pa.string()))

    def test_manifest_is_written_after_data_and_missing_when_a_write_fails(self):
        original = open
        calls = []

        def failing_open(file, mode="r", *args, **kwargs):
            if "x" in mode and str(file).endswith(f"loads/{lc.DATA_FILE_NAME}"):
                raise OSError("disk full")
            if "x" in mode:
                calls.append(str(file))
            return original(file, mode, *args, **kwargs)

        with mock.patch("builtins.open", failing_open):
            with self.assertRaises(OSError):
                self.run_generator(BASE_CONFIG)
        batch1 = self.landing / lc.batch_folder(1)
        self.assertFalse((batch1 / lc.MANIFEST_NAME).exists(), "a half-written batch must have no manifest")
        self.assertTrue(all(not c.endswith(lc.MANIFEST_NAME) for c in calls))

    def test_dry_run_writes_nothing(self):
        manifests = gen.generate(self.config(BASE_CONFIG), self.landing, dry_run=True)
        self.assertEqual(sorted(manifests), [1, 2])
        self.assertEqual(sorted(p.name for p in self.landing.iterdir()), [lc.batch_folder(0)])


if __name__ == "__main__":
    unittest.main()
