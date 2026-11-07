"""The build report accepts only verified silver references and rejects everything else.

The log lines follow the format of the probe run (msbuild "file(row,col): error CODE:
message [project]"). The real build output is also tested on every CI run by the
self-test job, which injects defects and expects them to be rejected.
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


VIEW = "dim/Views/vw_src_truck.sql"
GOLD_ONLY = "log/Views/vw_cdf_checkpoint.sql"
TABLE_REF = ("View: [dim].[vw_src_truck] has an unresolved reference to object "
             "[lh_logistics_silver].[dbo].[silver_trucks].")
COLUMN_CANDIDATES = ("Computed Column: [dim].[vw_src_truck].[acquisition_mileage] contains an unresolved "
                     "reference to an object. Either the object does not exist or the reference is ambiguous "
                     "because it could refer to any of the following objects: "
                     "[lh_logistics_silver].[dbo].[silver_trucks].[acquisition_mileage] or "
                     "[dim].[vw_src_truck].[s]::[acquisition_mileage].")


def run(*lines, exit_code=1, expect=None):
    return br.report("\n".join(lines) + "\nBuild FAILED.\n", exit_code, REPO, expect)


class Accepts(unittest.TestCase):
    def test_verified_table_and_column_references(self):
        passed, text = run(line(VIEW, "SQL71561", TABLE_REF), line(VIEW, "SQL71561", COLUMN_CANDIDATES))
        self.assertTrue(passed, text)
        self.assertIn("2 verified silver references, 0 other", text)

    def test_duplicate_lines_counted_once(self):
        passed, text = run(line(VIEW, "SQL71561", TABLE_REF), line(VIEW, "SQL71561", TABLE_REF))
        self.assertIn("Errors: 1 ", text)

    def test_successful_build(self):
        passed, _ = run(exit_code=0)
        self.assertTrue(passed)

    def test_warnings_listed_but_do_not_fail(self):
        warning = line(VIEW, "SQL71562", TABLE_REF.replace("View", "Procedure"), severity="warning")
        passed, text = run(line(VIEW, "SQL71561", TABLE_REF), warning)
        self.assertTrue(passed)
        self.assertIn("All warnings", text)


class Rejects(unittest.TestCase):
    def assert_rejected(self, *lines, reason):
        passed, text = run(*lines)
        self.assertFalse(passed, text)
        self.assertIn(reason, text)

    def test_syntax_error(self):
        self.assert_rejected(line(GOLD_ONLY, "SQL46010", "Incorrect syntax near w."),
                             reason="SQL46010 is not an unresolved-reference error")

    def test_unresolved_gold_object(self):
        message = ("Procedure: [fact].[usp_load_fact_trip] has an unresolved reference to object "
                   "[dim].[dim_truck_missing].")
        self.assert_rejected(line("fact/StoredProcedures/usp_load_fact_trip.sql", "SQL71501", message),
                             reason="outside lh_logistics_silver.dbo")

    def test_unknown_silver_table(self):
        self.assert_rejected(line(VIEW, "SQL71561", TABLE_REF.replace("silver_trucks", "silver_truckz")),
                             reason="unknown-table")

    def test_unknown_silver_column(self):
        message = COLUMN_CANDIDATES.replace("acquisition_mileage", "acquisition_mileage_missing")
        self.assert_rejected(line(VIEW, "SQL71561", message), reason="unknown-column")

    def test_wrong_case_is_not_the_same_table(self):
        self.assert_rejected(line(VIEW, "SQL71561", TABLE_REF.replace("silver_trucks", "Silver_Trucks")),
                             reason="unknown-table")

    def test_silver_error_in_script_that_does_not_read_silver(self):
        self.assert_rejected(line(GOLD_ONLY, "SQL71561", TABLE_REF),
                             reason="does not read silver")

    def test_failed_build_without_readable_errors(self):
        passed, text = run("error MSB4019: something unexpected")
        self.assertFalse(passed)
        passed, text = run("", exit_code=1)
        self.assertFalse(passed)
        self.assertIn("without any readable error", text)


class SelfTest(unittest.TestCase):
    def test_injected_defect_caught(self):
        passed, text = run(line(VIEW, "SQL71561", TABLE_REF),
                           line(GOLD_ONLY, "SQL46010", "Incorrect syntax near w."),
                           expect=GOLD_ONLY)
        self.assertTrue(passed, text)

    def test_injected_defect_missed(self):
        passed, text = run(line(VIEW, "SQL71561", TABLE_REF), expect=GOLD_ONLY)
        self.assertFalse(passed)
        self.assertIn("NOT caught", text)


if __name__ == "__main__":
    unittest.main()
