"""Each check passes on the committed repository and fails on one deliberate defect.

The defects are applied to a temporary copy of fabric/ and README.md, never to the
committed files and never to a Fabric workspace.
"""

import json
import os
import re
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

CI_DIR = Path(__file__).resolve().parents[1]
REPO = CI_DIR.parent
sys.path.insert(0, str(CI_DIR))
import repo_model as rm  # noqa: E402
import validate_repo as vr  # noqa: E402

PIPELINE = rm.GOLD_PIPELINE
SCHEMAS = Path(os.environ["CI_SCHEMAS_DIR"]) if os.environ.get("CI_SCHEMAS_DIR") else None


class RepoCopy(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        shutil.copytree(REPO / "fabric", self.root / "fabric")
        shutil.copy(REPO / "README.md", self.root / "README.md")

    def tearDown(self):
        self.tmp.cleanup()

    def edit_text(self, relative, old, new):
        path = self.root / relative
        text = path.read_text(encoding="utf-8-sig")
        self.assertEqual(text.count(old), 1, f"test anchor {old!r} must occur once in {relative}")
        path.write_text(text.replace(old, new, 1), encoding="utf-8")

    def edit_pipeline(self, change):
        path = self.root / PIPELINE
        document = rm.load_json(path)
        activities = {a["name"]: a for a in document["properties"]["activities"]}
        change(activities)
        path.write_text(json.dumps(document), encoding="utf-8")

    def assert_passes(self, result):
        self.assertEqual(result.failures, [], result.name)

    def assert_fails(self, result, fragment):
        joined = "\n".join(result.failures)
        self.assertTrue(result.failures, f"{result.name}: expected a failure")
        self.assertIn(fragment, joined)


class Baseline(RepoCopy):
    def test_committed_repository_passes_every_check(self):
        for check in vr.CHECKS[:-1]:
            self.assert_passes(check(self.root))
        self.assert_passes(vr.check_json_and_schemas(self.root, None))


class PipelineWiring(RepoCopy):
    def test_fact_calls_wrong_procedure(self):
        self.edit_pipeline(lambda a: a["Load_fact_fuel_purchase"]["typeProperties"].update(
            storedProcedureName="[fact].[usp_load_fact_delivery_event]"))
        self.assert_fails(vr.check_pipeline_wiring(self.root),
                          "Load_fact_fuel_purchase: calls [fact].[usp_load_fact_delivery_event]")

    def test_completeness_check_misses_a_fact(self):
        def drop(a):
            deps = a["Assert_Run_Complete"]["dependsOn"]
            a["Assert_Run_Complete"]["dependsOn"] = [d for d in deps if d["activity"] != "Load_fact_trip"]
        self.edit_pipeline(drop)
        self.assert_fails(vr.check_pipeline_wiring(self.root), "does not depend on Load_fact_trip")

    def test_completeness_check_runs_on_completion(self):
        def loosen(a):
            for d in a["Assert_Run_Complete"]["dependsOn"]:
                d["dependencyConditions"] = ["Completed"]
        self.edit_pipeline(loosen)
        self.assert_fails(vr.check_pipeline_wiring(self.root), "expected ['Succeeded']")

    def test_inactive_activity(self):
        self.edit_pipeline(lambda a: a["Load_fact_trip"].update(state="Inactive"))
        self.assert_fails(vr.check_pipeline_wiring(self.root), "Load_fact_trip: activity state is Inactive")

    def test_required_run_id_set_to_null(self):
        self.edit_pipeline(lambda a: a["Load_fact_trip"]["typeProperties"]["storedProcedureParameters"]
                           ["run_id"].update(value=None))
        self.assert_fails(vr.check_pipeline_wiring(self.root), "Load_fact_trip: parameter run_id is null")

    def test_mass_delete_left_on(self):
        self.edit_pipeline(lambda a: a["Load_fact_trip"]["typeProperties"]["storedProcedureParameters"]
                           ["allow_mass_delete"].update(value="true"))
        self.assert_fails(vr.check_pipeline_wiring(self.root), "allow_mass_delete is 'true'")

    def test_dependency_on_missing_activity(self):
        self.edit_pipeline(lambda a: a["Load_fact_trip"]["dependsOn"][0].update(activity="Detect_Changez"))
        self.assert_fails(vr.check_pipeline_wiring(self.root), "depends on missing activity Detect_Changez")

    def test_procedure_without_script(self):
        self.edit_pipeline(lambda a: a["Purge_Worklists"]["typeProperties"].update(
            storedProcedureName="[stg].[usp_does_not_exist]"))
        self.assert_fails(vr.check_pipeline_wiring(self.root), "has no script")


class ItemsAndReferences(RepoCopy):
    def test_reference_to_missing_item(self):
        self.edit_pipeline(lambda a: a["Detect_Changes"]["typeProperties"].update(
            notebookId="11111111-2222-3333-4444-555555555555"))
        self.assert_fails(vr.check_items_and_references(self.root), "is not an item in this repository")

    def test_duplicate_logical_id(self):
        source = rm.load_json(self.root / "fabric/pl_gold_load.DataPipeline/.platform")
        target = self.root / "fabric/pl_logistics_daily.DataPipeline/.platform"
        document = rm.load_json(target)
        document["config"]["logicalId"] = source["config"]["logicalId"]
        target.write_text(json.dumps(document), encoding="utf-8")
        self.assert_fails(vr.check_items_and_references(self.root), "logicalId also used by")

    def test_report_points_to_missing_model(self):
        self.edit_text("fabric/rpt_lane_performance.Report/definition.pbir",
                       "../sm_logistics_gold.SemanticModel", "../sm_missing.SemanticModel")
        self.assert_fails(vr.check_items_and_references(self.root), "does not exist")


class JsonAndSchemas(RepoCopy):
    def test_invalid_json(self):
        path = self.root / PIPELINE
        path.write_text(path.read_text(encoding="utf-8")[:-5], encoding="utf-8")
        self.assert_fails(vr.check_json_and_schemas(self.root, None), "not valid JSON")

    @unittest.skipUnless(SCHEMAS and SCHEMAS.is_dir(), "set CI_SCHEMAS_DIR to a json-schemas checkout")
    def test_schema_violation(self):
        self.assert_passes(vr.check_json_and_schemas(self.root, SCHEMAS))
        self.edit_text("fabric/rpt_lane_performance.Report/definition.pbir",
                       '"datasetReference"', '"datasetReferenze"')
        self.assert_fails(vr.check_json_and_schemas(self.root, SCHEMAS), "schema validation failed")


class Notebooks(RepoCopy):
    def test_python_syntax_error(self):
        path = next((self.root / "fabric").glob("nb_silver_endpoint_sync.Notebook/notebook-content.py"))
        text = path.read_text(encoding="utf-8")
        marker = "# CELL ********************\n"
        path.write_text(text.replace(marker, marker + "def broken(:\n", 1), encoding="utf-8")
        self.assert_fails(vr.check_notebooks(self.root), "nb_silver_endpoint_sync")


class ReportFields(RepoCopy):
    def test_visual_uses_missing_field(self):
        path = next(p for p in (self.root / rm.REPORT_DIR).rglob("visual.json")
                    if '"Property"' in p.read_text(encoding="utf-8-sig"))
        text = path.read_text(encoding="utf-8-sig")
        name = re.search(r'"Property":\s*"([^"]+)"', text).group(1)
        path.write_text(text.replace(f'"{name}"', f'"{name} Missing"', 1), encoding="utf-8")
        self.assert_fails(vr.check_report_fields(self.root), "is not in the model")


class ReadmeCounts(RepoCopy):
    def test_wrong_relationship_count(self):
        self.edit_text("README.md", "34 relationships", "55 relationships")
        self.assert_fails(vr.check_readme_counts(self.root), "README.md states 55 relationships")


class WarehouseNames(RepoCopy):
    def test_unknown_silver_table(self):
        self.edit_text(f"{rm.WAREHOUSE_DIR}/dim/Views/vw_src_truck.sql",
                       "lh_logistics_silver.dbo.silver_trucks", "lh_logistics_silver.dbo.silver_truckz")
        self.assert_fails(vr.check_warehouse_names(self.root), "silver_truckz is not a silver table")

    def test_wrong_schema_spelling(self):
        self.edit_text(f"{rm.WAREHOUSE_DIR}/dim/Views/vw_src_truck.sql",
                       "lh_logistics_silver.dbo.silver_trucks", "lh_logistics_silver.DBO.silver_trucks")
        self.assert_fails(vr.check_warehouse_names(self.root), "must be spelled")

    def test_three_part_self_reference(self):
        self.edit_text(f"{rm.WAREHOUSE_DIR}/fact/StoredProcedures/usp_load_fact_trip.sql",
                       "LEFT JOIN dim.dim_truck AS tk", "LEFT JOIN wh_logistics_gold.dim.dim_truck AS tk")
        self.assert_fails(vr.check_warehouse_names(self.root), "three-part self-reference")


class Secrets(RepoCopy):
    def test_webhook_url(self):
        # Built at runtime so no committed file contains a webhook URL.
        fake = "https://" + vr.WEBHOOK_HOST + "/services/" + "/".join(["TFAKE0000", "BFAKE0000", "x" * 24])
        path = self.root / "leak.txt"
        path.write_text(f"url = {fake}\n", encoding="utf-8")
        self.assert_fails(vr.check_secrets(self.root, [path]), "leak.txt:1: Slack webhook URL")

    def test_clean_file(self):
        path = self.root / "clean.txt"
        path.write_text("nothing to see\n", encoding="utf-8")
        self.assert_passes(vr.check_secrets(self.root, [path]))


if __name__ == "__main__":
    unittest.main()
