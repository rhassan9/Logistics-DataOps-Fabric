"""The build report accepts only verified lakehouse references and rejects everything else.

The messages below are copied from CI run 1 on branch ci/static-checks (Nov 7, 2026):
the clean build and the gold-reference self-test. The real build output is also
tested on every CI run by the self-test job, which injects defects.
"""

import sys
import unittest
from pathlib import Path

CI_DIR = Path(__file__).resolve().parents[1]
REPO = CI_DIR.parent
sys.path.insert(0, str(CI_DIR))
import sql_build_report as br  # noqa: E402

BASE = "/home/runner/work/r/r/fabric/wh_logistics_gold.Warehouse"
PROJECT = " [/home/runner/work/r/r/fabric/wh_logistics_gold.Warehouse/wh_logistics_gold.sqlproj]"


def line(file, code, message, severity="error"):
    return f"{BASE}/{file}(10,5): Build {severity} {code}: {message}{PROJECT}"


ROUTE = "dim/Views/vw_src_route.sql"
LOCATION = "dim/Views/vw_src_location.sql"
TRUCK = "dim/Views/vw_src_truck.sql"
TRIP = "fact/StoredProcedures/usp_load_fact_trip.sql"
RECONCILE = "log/StoredProcedures/usp_reconcile_baselines.sql"
GOLD_ONLY = "log/Views/vw_cdf_checkpoint.sql"

# Real messages from the run.
ROUTE_COLUMN = ("View: [dim].[vw_src_route] contains an unresolved reference to an object. Either the object "
                "does not exist or the reference is ambiguous because it could refer to any of the following "
                "objects: [lh_logistics_silver].[dbo].[silver_routes].[base_rate_per_mile], "
                "[lh_logistics_silver].[dbo].[silver_routes].[s]::[base_rate_per_mile] or "
                "[ref].[us_state].[s]::[base_rate_per_mile].")
LOCATION_COLUMN = ("View: [dim].[vw_src_location] contains an unresolved reference to an object. Either the "
                   "object does not exist or the reference is ambiguous because it could refer to any of the "
                   "following objects: [dim].[vw_city_state_map].[x]::[location_city], "
                   "[lh_logistics_silver].[dbo].[silver_fuel_purchases].[fp]::[location_city] or "
                   "[lh_logistics_silver].[dbo].[silver_fuel_purchases].[location_city].")
TRUCK_TABLE = ("View: [dim].[vw_src_truck] has an unresolved reference to object "
               "[lh_logistics_silver].[dbo].[silver_trucks].")
PROC_SILVER = ("Procedure: [fact].[usp_load_fact_trip] has an unresolved reference to object "
               "[lh_logistics_silver].[dbo].[silver_trips].")
PROC_OPS = ("Procedure: [log].[usp_reconcile_baselines] contains an unresolved reference to an object. Either "
            "the object does not exist or the reference is ambiguous because it could refer to any of the "
            "following objects: [lh_logistics_ops].[dbo].[validation_driver_monthly_metrics].[b]::[driver_id] "
            "or [lh_logistics_ops].[dbo].[validation_driver_monthly_metrics].[driver_id].")
GOLD_TABLE_MISSING = ("Procedure: [fact].[usp_load_fact_trip] has an unresolved reference to object "
                      "[dim].[dim_truck_missing].")
GOLD_COLUMN_MISSING = ("Procedure: [fact].[usp_load_fact_trip] contains an unresolved reference to an object. "
                       "Either the object does not exist or the reference is ambiguous because it could refer "
                       "to any of the following objects: [dim].[dim_date].[tk]::[truck_id], "
                       "[dim].[dim_driver].[tk]::[truck_id], [dim].[dim_truck_missing].[tk]::[truck_id] or "
                       "[dim].[dim_truck_missing].[truck_id].")


def run(*lines, exit_code=1, expect=None):
    return br.report("\n".join(lines) + "\nBuild FAILED.\n", exit_code, REPO, expect)


CLEAN = [line(ROUTE, "SQL71561", ROUTE_COLUMN), line(LOCATION, "SQL71561", LOCATION_COLUMN),
         line(TRUCK, "SQL71561", TRUCK_TABLE), line(TRIP, "SQL71562", PROC_SILVER, "warning"),
         line(RECONCILE, "SQL71562", PROC_OPS, "warning")]


class Accepts(unittest.TestCase):
    def test_clean_build_messages(self):
        passed, text = run(*CLEAN)
        self.assertTrue(passed, text)
        self.assertIn("3 verified lakehouse references, 0 rejected", text)
        self.assertIn("2 verified lakehouse references, 0 rejected, 0 other", text)

    def test_duplicate_lines_counted_once(self):
        passed, text = run(line(TRUCK, "SQL71561", TRUCK_TABLE), line(TRUCK, "SQL71561", TRUCK_TABLE))
        self.assertIn("Errors: 1 ", text)

    def test_successful_build(self):
        passed, _ = run(exit_code=0)
        self.assertTrue(passed)

    def test_unrelated_warning_listed_not_failed(self):
        passed, text = run(line(TRUCK, "SQL71561", TRUCK_TABLE),
                           line(GOLD_ONLY, "SQL70588", "Some other warning.", "warning"))
        self.assertTrue(passed, text)
        self.assertIn("Other warnings", text)


class Rejects(unittest.TestCase):
    def assert_rejected(self, *lines, reason):
        passed, text = run(*CLEAN, *lines)
        self.assertFalse(passed, text)
        self.assertIn(reason, text)

    def test_syntax_error(self):
        self.assert_rejected(line(GOLD_ONLY, "SQL46010", "Incorrect syntax near w."),
                             reason="SQL46010 is not an unresolved-reference error")

    def test_missing_gold_table_in_procedure_is_only_a_warning_but_fails(self):
        self.assert_rejected(line(TRIP, "SQL71502", GOLD_TABLE_MISSING, "warning"),
                             reason="not a lakehouse table")

    def test_missing_gold_column_candidates(self):
        self.assert_rejected(line(TRIP, "SQL71502", GOLD_COLUMN_MISSING, "warning"),
                             reason="not a lakehouse table")

    def test_unknown_silver_table(self):
        self.assert_rejected(line(TRUCK, "SQL71561", TRUCK_TABLE.replace("silver_trucks", "silver_truckz")),
                             reason="unknown-table")

    def test_unknown_silver_column(self):
        self.assert_rejected(line(ROUTE, "SQL71561", ROUTE_COLUMN.replace("base_rate_per_mile", "base_rate")),
                             reason="unknown-column")

    def test_unknown_ops_column(self):
        self.assert_rejected(line(RECONCILE, "SQL71562", PROC_OPS.replace("driver_id", "driver_key"), "warning"),
                             reason="unknown-column")

    def test_wrong_case_is_not_the_same_table(self):
        self.assert_rejected(line(TRUCK, "SQL71561", TRUCK_TABLE.replace("silver_trucks", "Silver_Trucks")),
                             reason="unknown-table")

    def test_reference_from_a_script_that_does_not_read_that_lakehouse(self):
        self.assert_rejected(line(GOLD_ONLY, "SQL71561", TRUCK_TABLE),
                             reason="does not read lh_logistics_silver")

    def test_candidate_list_with_only_alias_forms(self):
        message = ROUTE_COLUMN.replace("[lh_logistics_silver].[dbo].[silver_routes].[base_rate_per_mile], ", "")
        self.assert_rejected(line(ROUTE, "SQL71561", message), reason="names no referenced object")

    def test_failed_build_without_readable_errors(self):
        passed, text = run("", exit_code=1)
        self.assertFalse(passed)
        self.assertIn("without any readable error", text)
        passed, _ = run("error MSB4019: something unexpected")
        self.assertFalse(passed)


class SelfTest(unittest.TestCase):
    def test_injected_defect_caught(self):
        passed, text = run(*CLEAN, line(TRIP, "SQL71502", GOLD_TABLE_MISSING, "warning"), expect=TRIP)
        self.assertTrue(passed, text)

    def test_injected_defect_missed(self):
        passed, text = run(*CLEAN, expect=TRIP)
        self.assertFalse(passed)
        self.assertIn("NOT caught", text)


if __name__ == "__main__":
    unittest.main()
