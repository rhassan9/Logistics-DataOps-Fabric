"""The silver routine: transform parity on known rows, row hashes, the snapshot guard,
batch validation, and recovery from a write that fails part-way.

Runs on a local Spark session with the settings of Fabric Runtime 2.0 (UTC,
ANSI mode). Skipped when PySpark is not installed; the CI Spark job installs it
from ingestion/requirements-spark.txt.
"""

from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

try:
    import pyspark  # noqa: F401
except ImportError as exc:  # pragma: no cover - the static job has no PySpark
    raise unittest.SkipTest(f"PySpark is not installed: {exc}")

from pyspark.sql import functions as F

from ingestion import landing_contract as lc
from ingestion import silver_routine as sr
from ingestion.tests import spark_support as ss
from ingestion.tests import support

UTC = timezone.utc
SCHEMAS = ss.silver_schemas()
RUN_ID = "test-run"
INGEST_DATE = "2026-11-27"


def manifest_for(rows):
    """A snapshot manifest whose row counts match rows ({table: list of dicts})."""
    tables = {}
    for i, table in enumerate(lc.TABLES):
        tables[table] = {"files": [{"path": f"{table}/{lc.DATA_FILE_NAME}", "bytes": 100 + i, "sha256": f"{i:064x}"}],
                         "row_count": len(rows[table]), "min_sequence": 0, "max_sequence": 0,
                         "operation_counts": None}
    return {"contract_version": lc.CONTRACT_VERSION, "batch_id": "logistics-000000", "batch_seq": 0,
            "mode": lc.MODE_SNAPSHOT, "business_date": None, "as_of_date": support.AS_OF,
            "previous_batch_id": None, "supersedes_batch_id": None, "producer": {"name": "test"},
            "content_hash": lc.content_hash(tables), "tables": tables}


def base_rows():
    return {table: support.snapshot_rows(table) for table in lc.TABLES}


def build(rows, run_id=RUN_ID):
    """(manifest, frames, silver rows) for a snapshot batch of rows."""
    manifest = manifest_for(rows)
    frames = {table: ss.frame(table, rows[table]) for table in lc.TABLES}
    out = sr.snapshot_silver_rows(frames, manifest, SCHEMAS, run_id, INGEST_DATE)
    return manifest, frames, out


def collect(df, key):
    return {r[key]: r.asDict() for r in df.collect()}


class Store:
    """Silver tables held in memory, with the writer the routine is given."""

    def __init__(self, fail_on_write_number=None):
        self.tables = {t: ss.empty(SCHEMAS[t]) for t in lc.TABLES}
        self.writes = []
        self.fail_on = fail_on_write_number

    def write(self, table, rows):
        if self.fail_on is not None and len(self.writes) + 1 == self.fail_on:
            raise RuntimeError(f"forced failure writing {table}")
        stamped = (rows.withColumn("_silver_created_at", F.current_timestamp())
                       .withColumn("_silver_updated_at", F.current_timestamp()))
        self.tables[table] = self.tables[table].unionByName(stamped.select(*SCHEMAS[table].names)).cache()
        self.writes.append(table)
        return stamped.count()


def plans_for(batch_rows, store):
    return {t: sr.snapshot_plan(t, batch_rows[t], store.tables[t], SCHEMAS[t]) for t in lc.TABLES}


class OutputSchema(unittest.TestCase):
    def test_every_table_conforms_to_its_silver_definition(self):
        _, _, out = build(base_rows())
        for table in lc.TABLES:
            with self.subTest(table=table):
                expected = [f for f in SCHEMAS[table].fields if f.name not in sr.WRITE_STAMPED]
                self.assertEqual([(f.name, f.dataType) for f in out[table].schema.fields],
                                 [(f.name, f.dataType) for f in expected])
                self.assertEqual(out[table].count(), support.ROWS_PER_TABLE)

    def test_silver_definition_holds_the_lineage_columns(self):
        for table, schema in SCHEMAS.items():
            with self.subTest(table=table):
                self.assertEqual(schema.names[-2:], ["_source_batch_id", "_source_sequence"])
                self.assertTrue(set(sr.LINEAGE_COLUMNS) <= set(schema.names))

    def test_lineage_and_state_columns_of_a_snapshot(self):
        _, _, out = build(base_rows())
        row = out["loads"].first()
        self.assertIs(row["_is_deleted_in_source"], False)
        self.assertEqual(row["_source_changed_at"], datetime(2024, 12, 31))
        self.assertEqual(row["_source_batch_id"], "logistics-000000")
        self.assertEqual(row["_source_sequence"], 0)
        self.assertEqual(row["_silver_run_id"], RUN_ID)
        self.assertEqual(row["_source_ingest_date"], INGEST_DATE)

    def test_change_batches_are_refused(self):
        rows = base_rows()
        manifest = manifest_for(rows)
        manifest["mode"] = lc.MODE_CHANGE
        frames = {t: ss.frame(t, rows[t]) for t in lc.TABLES}
        with self.assertRaisesRegex(ValueError, "is not applied"):
            sr.snapshot_silver_rows(frames, manifest, SCHEMAS, RUN_ID, INGEST_DATE)

    def test_a_session_outside_utc_is_refused(self):
        spark = ss.spark()
        spark.conf.set("spark.sql.session.timeZone", "Asia/Karachi")
        try:
            with self.assertRaisesRegex(RuntimeError, "UTC"):
                sr.require_utc(spark)
        finally:
            spark.conf.set("spark.sql.session.timeZone", "UTC")
        sr.require_utc(spark)


class TransformParity(unittest.TestCase):
    """Known rows through each table's transform, against the values the running
    silver build (notebook and Dataflow) produces."""

    def run_table(self, table, rows, lookups_rows=None):
        rows_all = base_rows()
        rows_all[table] = rows
        if lookups_rows is not None:
            rows_all["trucks"] = lookups_rows
        _, _, out = build(rows_all)
        return collect(out[table], lc.TABLES[table]["key"])

    def row(self, table, **values):
        base = support.snapshot_rows(table, 1)[0]
        base.update(values)
        return base

    def test_drivers(self):
        out = self.run_table("drivers", [
            self.row("drivers", driver_id="D1", hire_date=date(2020, 1, 1), date_of_birth=date(1990, 6, 30),
                     termination_date=None),
            self.row("drivers", driver_id="D2", hire_date=date(2020, 1, 1), date_of_birth=date(1990, 1, 1),
                     termination_date=date(2023, 3, 1)),
            self.row("drivers", driver_id="D3", hire_date=date(2020, 1, 1), date_of_birth=date(1990, 1, 1),
                     termination_date=date(2019, 12, 31)),
        ])
        self.assertEqual(out["D1"]["tenure_days"], (date(2024, 12, 31) - date(2020, 1, 1)).days)
        self.assertEqual(out["D1"]["age_at_hire"], 30)  # a year difference, as the Dataflow computes it
        self.assertEqual(out["D2"]["tenure_days"], (date(2023, 3, 1) - date(2020, 1, 1)).days)
        self.assertEqual(out["D3"]["tenure_days"], -1)
        self.assertEqual({k: v["_dq_status"] for k, v in out.items()},
                         {"D1": "valid", "D2": "valid", "D3": "flagged"})

    def test_trucks_trailers_facilities_routes(self):
        trucks = self.run_table("trucks", [self.row("trucks", truck_id="T1", model_year=2018)])
        self.assertEqual(trucks["T1"]["truck_age_years_as_of"], 6)
        trailers = self.run_table("trailers", [self.row("trailers", trailer_id="R1", model_year=2015)])
        self.assertEqual(trailers["R1"]["trailer_age_years_as_of"], 9)
        facilities = self.run_table("facilities", [self.row("facilities", facility_id="F1")])
        self.assertIs(facilities["F1"]["is_centroid_coordinate"], True)
        routes = self.run_table("routes", [
            self.row("routes", route_id="RT1", origin_city="Dallas", destination_city="Houston"),
            self.row("routes", route_id="RT2", origin_city="Dallas", destination_city=None),
        ])
        self.assertEqual(routes["RT1"]["route_name"], "Dallas → Houston")
        self.assertIsNone(routes["RT2"]["route_name"])

    def test_trips_idle_flag(self):
        out = self.run_table("trips", [
            self.row("trips", trip_id="P1", idle_time_hours=Decimal("5.00"), actual_duration_hours=Decimal("4.00")),
            self.row("trips", trip_id="P2", idle_time_hours=Decimal("4.00"), actual_duration_hours=Decimal("4.00")),
            self.row("trips", trip_id="P3", idle_time_hours=None, actual_duration_hours=Decimal("4.00")),
        ])
        self.assertEqual((out["P1"]["is_idle_implausible"], out["P1"]["_dq_status"]), (True, "flagged"))
        self.assertEqual((out["P2"]["is_idle_implausible"], out["P2"]["_dq_status"]), (False, "valid"))
        self.assertEqual((out["P3"]["is_idle_implausible"], out["P3"]["_dq_status"]), (None, "valid"))

    def test_delivery_events(self):
        t0 = datetime(2024, 3, 1, 10, 0, tzinfo=UTC)

        def event(eid, load, kind, actual_offset, detention=0, scheduled=t0):
            return self.row("delivery_events", event_id=eid, load_id=load, event_type=kind,
                            scheduled_datetime=scheduled, actual_datetime=scheduled + actual_offset,
                            detention_minutes=detention)

        out = self.run_table("delivery_events", [
            event("E1", "L1", "Pickup", timedelta(minutes=120), detention=150),
            event("E2", "L1", "Delivery", timedelta(minutes=119, seconds=59, milliseconds=400),
                  scheduled=t0 + timedelta(hours=5)),
            event("E3", "L2", "Pickup", timedelta(minutes=-120), detention=100),
            event("E4", "L2", "Delivery", timedelta(minutes=-200)),
            event("E5", None, "Delivery", timedelta(0), detention=None),
        ])
        self.assertEqual(out["E1"]["arrival_status"], "Late")
        # 119.99 minutes rounds to 120.0 for display but is still on time.
        self.assertEqual(out["E2"]["arrival_variance_minutes"], Decimal("120.0"))
        self.assertEqual(out["E2"]["arrival_status"], "On Time")
        self.assertEqual(out["E3"]["arrival_status"], "Early")
        self.assertEqual(out["E1"]["billable_detention_minutes"], 30)
        self.assertEqual(out["E3"]["billable_detention_minutes"], 0)
        # greatest() ignores nulls, so the running code writes 0 for a missing detention.
        self.assertEqual(out["E5"]["billable_detention_minutes"], 0)
        # L2's delivery precedes its pickup: both events of that load are flagged.
        self.assertEqual({k: (v["is_timestamp_reversed"], v["_dq_status"]) for k, v in out.items()}, {
            "E1": (False, "valid"), "E2": (False, "valid"),
            "E3": (True, "flagged"), "E4": (True, "flagged"),
            "E5": (None, "valid"),
        })

    def test_maintenance_and_safety(self):
        maintenance = self.run_table("maintenance_records", [
            self.row("maintenance_records", maintenance_id="M1", service_description="Urgent brake repair")])
        self.assertEqual(maintenance["M1"]["service_urgency"], "Urgent")
        self.assertNotIn("service_description", maintenance["M1"])
        safety = self.run_table("safety_incidents", [
            self.row("safety_incidents", incident_id="S1", incident_type="Accident",
                     description="Major collision involving debris on road"),
            self.row("safety_incidents", incident_id="S2", incident_type="DOT Violation"),
            self.row("safety_incidents", incident_id="S3", incident_type="Equipment Damage"),
            self.row("safety_incidents", incident_id="S4", incident_type="Customer Complaint"),
            self.row("safety_incidents", incident_id="S5", incident_type="Moving Violation"),
            self.row("safety_incidents", incident_id="S6", incident_type="Weather Delay"),
        ])
        self.assertEqual((safety["S1"]["incident_severity"], safety["S1"]["incident_cause"]),
                         ("Major", "debris on road"))
        self.assertEqual({k: v["incident_category"] for k, v in safety.items()}, {
            "S1": "Safety", "S2": "Regulatory", "S3": "Asset", "S4": "Service", "S5": "Safety", "S6": "Other"})
        self.assertNotIn("location_state", safety["S1"])

    def test_fuel_capacity_uses_the_batch_trucks(self):
        out = self.run_table(
            "fuel_purchases",
            [self.row("fuel_purchases", fuel_purchase_id="U1", truck_id="T150", gallons=Decimal("160.00")),
             self.row("fuel_purchases", fuel_purchase_id="U2", truck_id="T150", gallons=Decimal("150.00")),
             self.row("fuel_purchases", fuel_purchase_id="U3", truck_id="UNKNOWN", gallons=Decimal("999.00"))],
            lookups_rows=[self.row("trucks", truck_id="T150", tank_capacity_gallons=150)],
        )
        self.assertEqual({k: (v["is_capacity_exceeded"], v["_dq_status"]) for k, v in out.items()},
                         {"U1": (True, "flagged"), "U2": (False, "valid"), "U3": (False, "valid")})
        self.assertNotIn("location_state", out["U1"])


class RowHashes(unittest.TestCase):
    def hash_of(self, table, df):
        return {r["business_key"]: r["row_hash"]
                for r in sr.row_hashes(df, table, sr.hash_columns(SCHEMAS[table])).collect()}

    def test_lineage_columns_do_not_change_the_hash(self):
        _, _, out = build(base_rows())
        restamped = (out["drivers"].withColumn("_silver_run_id", F.lit("other"))
                     .withColumn("_source_changed_at", F.current_timestamp())
                     .withColumn("_source_batch_id", F.lit(None).cast("string"))
                     .withColumn("_source_sequence", F.lit(None).cast("bigint"))
                     .withColumn("_source_ingest_date", F.lit("2024-01-01")))
        self.assertEqual(self.hash_of("drivers", out["drivers"]), self.hash_of("drivers", restamped))

    def test_business_and_state_columns_change_the_hash(self):
        drivers = build(base_rows())[2]["drivers"]
        one = F.col("driver_id") == "DRV00001"

        def with_value(column, value):
            return self.hash_of("drivers", drivers.withColumn(column, F.when(one, value).otherwise(F.col(column))))

        before = self.hash_of("drivers", drivers)
        renamed = with_value("first_name", F.lit("Ahmad"))
        self.assertNotEqual(before["DRV00001"], renamed["DRV00001"])
        self.assertEqual(before["DRV00002"], renamed["DRV00002"])
        self.assertNotEqual(with_value("license_state", F.lit(""))["DRV00001"],
                            with_value("license_state", F.lit(None).cast("string"))["DRV00001"])
        self.assertNotEqual(before["DRV00001"], with_value("_is_deleted_in_source", F.lit(True))["DRV00001"])
        self.assertNotEqual(before["DRV00001"], with_value("_dq_status", F.lit("flagged"))["DRV00001"])

    def test_a_microsecond_changes_the_hash(self):
        _, _, out = build(base_rows())
        events = out["delivery_events"]
        shifted = events.withColumn("actual_datetime", F.col("actual_datetime") + F.expr("INTERVAL 1 MICROSECOND"))
        self.assertNotEqual(self.hash_of("delivery_events", events), self.hash_of("delivery_events", shifted))


class SnapshotGuard(unittest.TestCase):
    def setUp(self):
        _, _, self.batch = build(base_rows())

    def loaded_store(self, rows_by_table=None):
        """A store whose silver equals the batch, written by an earlier run."""
        store = Store()
        _, _, earlier = build(base_rows(), run_id="earlier-run")
        for table in lc.TABLES:
            store.write(table, (rows_by_table or {}).get(table, earlier[table]))
        store.writes.clear()
        return store

    def test_empty_silver_loads_every_table(self):
        store = Store()
        plans = plans_for(self.batch, store)
        self.assertEqual({p["decision"] for p in plans.values()}, {sr.PLAN_LOAD})
        result = sr.apply_snapshot(plans, self.batch, store.write)
        self.assertEqual(result["outcome"], "applied")
        self.assertEqual(store.writes, list(sr.APPLY_ORDER))
        self.assertEqual(plans_for(self.batch, store), {t: {"table": t, "decision": sr.PLAN_UNCHANGED,
                                                            "equal": support.ROWS_PER_TABLE, "changed": 0,
                                                            "only_batch": 0, "only_silver": 0,
                                                            "repeated_keys": 0, "examples": {}}
                                                        for t in lc.TABLES})

    def test_equal_silver_writes_nothing(self):
        store = self.loaded_store()
        plans = plans_for(self.batch, store)
        self.assertEqual({p["decision"] for p in plans.values()}, {sr.PLAN_UNCHANGED})
        result = sr.apply_snapshot(plans, self.batch, store.write)
        self.assertEqual((result["outcome"], store.writes), ("unchanged", []))

    def test_same_keys_different_value_rejects_the_whole_batch(self):
        _, _, earlier = build(base_rows(), run_id="earlier-run")
        renamed = earlier["drivers"].withColumn(
            "first_name", F.when(F.col("driver_id") == "DRV00003", F.lit("Ahmad")).otherwise(F.col("first_name")))
        store = self.loaded_store({"drivers": renamed})
        plans = plans_for(self.batch, store)
        self.assertEqual(plans["drivers"]["decision"], sr.PLAN_REJECT)
        self.assertEqual((plans["drivers"]["changed"], plans["drivers"]["examples"]),
                         (1, {"changed": ["DRV00003"]}))
        result = sr.apply_snapshot(plans, self.batch, store.write)
        self.assertEqual((result["outcome"], result["rejected"], store.writes), ("rejected", ["drivers"], []))

    def test_key_differences_reject(self):
        _, _, earlier = build(base_rows(), run_id="earlier-run")
        extra = support.snapshot_rows("trucks", support.ROWS_PER_TABLE + 1)
        rows = base_rows()
        rows["trucks"] = extra
        _, _, wider = build(rows, run_id="earlier-run")
        cases = {
            "only_silver": wider["trucks"],
            "only_batch": earlier["trucks"].filter(F.col("truck_id") != "TRK00006"),
            "repeated_keys": earlier["trucks"].unionByName(earlier["trucks"].filter(F.col("truck_id") == "TRK00001")),
        }
        for label, silver in cases.items():
            with self.subTest(case=label):
                store = self.loaded_store({"trucks": silver})
                plan = plans_for(self.batch, store)["trucks"]
                self.assertEqual(plan["decision"], sr.PLAN_REJECT)
                self.assertEqual(plan[label], 1)

    def test_a_failed_write_is_retried_by_running_again(self):
        store = Store(fail_on_write_number=7)
        plans = plans_for(self.batch, store)
        with self.assertRaisesRegex(RuntimeError, "forced failure writing loads"):
            sr.apply_snapshot(plans, self.batch, store.write)
        self.assertEqual(store.writes, list(sr.APPLY_ORDER[:6]))

        store.fail_on = None
        store.writes.clear()
        _, _, retry_rows = build(base_rows(), run_id="retry-run")
        plans = plans_for(retry_rows, store)
        self.assertEqual([plans[t]["decision"] for t in sr.APPLY_ORDER],
                         [sr.PLAN_UNCHANGED] * 6 + [sr.PLAN_LOAD] * 6)
        result = sr.apply_snapshot(plans, retry_rows, store.write)
        self.assertEqual((result["outcome"], store.writes), ("applied", list(sr.APPLY_ORDER[6:])))
        final = plans_for(self.batch, store)
        self.assertEqual({p["decision"] for p in final.values()}, {sr.PLAN_UNCHANGED})

    def test_every_table_needs_a_plan(self):
        with self.assertRaisesRegex(ValueError, "no plan"):
            sr.apply_snapshot({}, self.batch, Store().write)


class BatchValidation(unittest.TestCase):
    def test_frame_problems(self):
        rows = base_rows()
        manifest = manifest_for(rows)
        good = ss.frame("trucks", rows["trucks"])
        self.assertEqual(sr.frame_problems("trucks", good, manifest, lc.MODE_SNAPSHOT), [])

        cases = {
            "do not equal the contract": good.withColumn("status", F.lit(1)),
            "rows read, manifest": good.limit(5),
            "null truck_id": good.withColumn("truck_id", F.when(F.col("truck_id") == "TRK00001", F.lit(None))
                                             .otherwise(F.col("truck_id"))),
            "repeat a truck_id": good.withColumn("truck_id", F.when(F.col("truck_id") == "TRK00002",
                                                                    F.lit("TRK00001")).otherwise(F.col("truck_id"))),
        }
        for fragment, df in cases.items():
            with self.subTest(expect=fragment):
                problems = sr.frame_problems("trucks", df, manifest, lc.MODE_SNAPSHOT)
                self.assertTrue(any(fragment in p for p in problems), problems)

    def test_layout_and_file_problems(self):
        manifest = manifest_for(base_rows())
        entries = list(lc.TABLES) + [lc.MANIFEST_NAME]
        files = {t: {lc.DATA_FILE_NAME: spec["files"][0]["bytes"]} for t, spec in manifest["tables"].items()}
        self.assertEqual(sr.layout_problems(manifest, entries, files), [])

        missing = [e for e in entries if e != "routes"]
        self.assertTrue(any("missing ['routes']" in p for p in sr.layout_problems(manifest, missing, files)))
        extra = dict(files, trucks={lc.DATA_FILE_NAME: files["trucks"][lc.DATA_FILE_NAME], "part-00001.parquet": 9})
        self.assertTrue(any("trucks: files present" in p for p in sr.layout_problems(manifest, entries, extra)))
        resized = dict(files, trucks={lc.DATA_FILE_NAME: 1})
        self.assertTrue(any("1 bytes listed" in p for p in sr.layout_problems(manifest, entries, resized)))

        entry = manifest["tables"]["trucks"]["files"][0]
        self.assertEqual(sr.file_problems(manifest, "trucks", {lc.DATA_FILE_NAME: (entry["bytes"], entry["sha256"])}),
                         [])
        wrong = sr.file_problems(manifest, "trucks", {lc.DATA_FILE_NAME: (entry["bytes"], "f" * 64)})
        self.assertTrue(any("sha256" in p for p in wrong))
        self.assertIsNone(sr.data_read_problem(b"PAR1xxxxPAR1", 12))
        self.assertIn("listed size 12", sr.data_read_problem(b"PAR1xx", 12))
        self.assertIn("listed size 12", sr.data_read_problem(b"", 12))
        self.assertIn("expected b'PAR1'", sr.data_read_problem(b"PAR1xxxxxxxx", 12))
        unread = sr.file_problems(manifest, "trucks", {lc.DATA_FILE_NAME: "no complete read after 4 attempts"})
        self.assertTrue(any("no complete read" in p for p in unread))
        self.assertEqual(sr.file_problems(manifest, "trucks", {}), [f"trucks/{lc.DATA_FILE_NAME}: not read"])


if __name__ == "__main__":
    unittest.main()
