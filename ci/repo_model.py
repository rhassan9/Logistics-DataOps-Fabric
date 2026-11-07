"""Read the Fabric item definitions in this repository.

Shared by validate_repo.py and sql_build_report.py. Everything here reads files;
nothing writes to the repository or calls Fabric.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

FABRIC_DIR = "fabric"
WAREHOUSE_DIR = "fabric/wh_logistics_gold.Warehouse"
SILVER_DDL_NOTEBOOK = "fabric/nb_silver_create_tables.Notebook/notebook-content.py"
SEMANTIC_MODEL_DIR = "fabric/sm_logistics_gold.SemanticModel/definition"
REPORT_DIR = "fabric/rpt_lane_performance.Report/definition"
GOLD_PIPELINE = "fabric/pl_gold_load.DataPipeline/pipeline-content.json"

SILVER_DATABASE = "lh_logistics_silver"
SILVER_SCHEMA = "dbo"
SILVER_PREFIX = "silver_"
GOLD_DATABASE = "wh_logistics_gold"

# The ID Fabric writes when an activity points at an item in the same workspace.
SAME_WORKSPACE_ID = "00000000-0000-0000-0000-000000000000"


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def silver_inventory(root: Path) -> dict[str, set[str]]:
    """Silver tables and their columns, read from the silver DDL notebook.

    The notebook is the schema contract that creates every silver table, so the
    inventory follows it instead of being maintained by hand. Keys are full table
    names such as silver_trucks; values are column names, audit columns included.
    """
    text = (root / SILVER_DDL_NOTEBOOK).read_text(encoding="utf-8")

    audit = re.search(r'AUDIT_COLUMNS\s*=\s*"""(.*?)"""', text, re.S)
    if not audit:
        raise ValueError(f"AUDIT_COLUMNS not found in {SILVER_DDL_NOTEBOOK}")
    audit_columns = _column_names(audit.group(1))

    tables: dict[str, set[str]] = {}
    for block in ("REFERENCE_TABLES", "TRANSACTIONAL_TABLES"):
        body = re.search(rf"{block}\s*=\s*\{{(.*?)\n\}}", text, re.S)
        if not body:
            raise ValueError(f"{block} not found in {SILVER_DDL_NOTEBOOK}")
        for name, columns in re.findall(r'"(\w+)"\s*:\s*f?"""(.*?)"""', body.group(1), re.S):
            cols = _column_names(columns)
            if "{AUDIT_COLUMNS}" in columns:
                cols |= audit_columns
            tables[SILVER_PREFIX + name] = cols
    if not tables:
        raise ValueError(f"no silver tables found in {SILVER_DDL_NOTEBOOK}")
    return tables


def _column_names(ddl: str) -> set[str]:
    names = set()
    for line in ddl.splitlines():
        match = re.match(r"\s*([A-Za-z_]\w*)\s+[A-Z]", line)
        if match:
            names.add(match.group(1))
    return names


def warehouse_sql_files(root: Path) -> list[Path]:
    """Warehouse object scripts, excluding shared queries (not part of the build)."""
    base = root / WAREHOUSE_DIR
    return sorted(p for p in base.rglob("*.sql") if ".sharedqueries" not in p.parts)


def item_folders(root: Path) -> list[Path]:
    return sorted(p for p in (root / FABRIC_DIR).iterdir() if p.is_dir() and "." in p.name)


def logical_ids(root: Path) -> dict[str, Path]:
    ids = {}
    for folder in item_folders(root):
        platform = folder / ".platform"
        if platform.exists():
            ids[load_json(platform)["config"]["logicalId"]] = folder
    return ids


def walk_activities(activities: list[dict]):
    """Yield every activity, including those nested in control-flow containers."""
    for activity in activities:
        yield activity
        props = activity.get("typeProperties", {})
        for key in ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities"):
            if isinstance(props.get(key), list):
                yield from walk_activities(props[key])
        for case in props.get("cases", []) or []:
            yield from walk_activities(case.get("activities", []))


def pipelines(root: Path) -> dict[str, dict]:
    found = {}
    for folder in item_folders(root):
        if folder.name.endswith(".DataPipeline"):
            content = folder / "pipeline-content.json"
            found[folder.name] = load_json(content)
    return found


# Semantic model -------------------------------------------------------------

def _unquote(name: str) -> str:
    name = name.strip()
    if name.startswith("'") and name.endswith("'"):
        return name[1:-1].replace("''", "'")
    return name


def model_tables(root: Path) -> dict[str, dict]:
    """Table name -> columns, measures, and whether it is a Direct Lake table."""
    tables = {}
    for path in sorted((root / SEMANTIC_MODEL_DIR / "tables").glob("*.tmdl")):
        text = path.read_text(encoding="utf-8-sig")
        header = re.search(r"^table (.+?)\s*$", text, re.M)
        if not header:
            raise ValueError(f"no table header in {path}")
        columns = {_unquote(m) for m in re.findall(r"^\s+column ('(?:[^']|'')+'|[^\s=]+)", text, re.M)}
        measures = {_unquote(m) for m in re.findall(r"^\s+measure ('(?:[^']|'')+'|[^\s=]+)\s*=", text, re.M)}
        tables[_unquote(header.group(1))] = {
            "columns": columns,
            "measures": measures,
            "direct_lake": bool(re.search(r"^\s+mode:\s*directLake\s*$", text, re.M)),
            "calculation_group": bool(re.search(r"^\s+calculationGroup\b", text, re.M)),
            "path": path,
        }
    return tables


def model_relationships(root: Path) -> tuple[int, int]:
    text = (root / SEMANTIC_MODEL_DIR / "relationships.tmdl").read_text(encoding="utf-8-sig")
    total = len(re.findall(r"^relationship ", text, re.M))
    inactive = len(re.findall(r"^\s+isActive:\s*false\s*$", text, re.M))
    return total, inactive
