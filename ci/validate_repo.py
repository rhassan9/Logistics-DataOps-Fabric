"""Static checks on the committed Fabric item definitions.

Each check reads files only. It does not run notebooks, query a warehouse or call
Fabric, so a pass means the definitions are internally consistent, not that the
deployed pipeline produces correct data.

Usage:
    python ci/validate_repo.py [--root .] [--schemas PATH_TO_json-schemas_CHECKOUT]
Exit code 1 when any check fails.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import repo_model as rm  # noqa: E402

SCHEMA_URL_PREFIX = "https://developer.microsoft.com/json-schemas/"
# Assembled from parts so that this file never contains the host as one literal,
# which keeps the repository's own pre-commit search clean.
WEBHOOK_HOST = "hooks." + "slack.com"
WEBHOOK_PATTERN = re.compile(r"https?://" + re.escape(WEBHOOK_HOST) + r"/services/[A-Za-z0-9/_-]+")

FACT_ACTIVITY = re.compile(r"^Load_(fact_\w+)$")
RUN_ID_EXPRESSION = "@variables('v_run_id')"
COMPLETENESS_ACTIVITY = "Assert_Run_Complete"


@dataclass
class Result:
    name: str
    failures: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def fail(self, message: str):
        self.failures.append(message)

    def note(self, message: str):
        self.notes.append(message)


def rel(root: Path, path: Path) -> str:
    return path.relative_to(root).as_posix()


# 1. JSON parses and Microsoft schemas -----------------------------------------

def json_files(root: Path) -> list[Path]:
    suffixes = {".json", ".pbir", ".pbism", ".platform", ".schedules"}
    return sorted(p for p in (root / rm.FABRIC_DIR).rglob("*")
                  if p.is_file() and (p.suffix in suffixes or p.name in suffixes))


def check_json_and_schemas(root: Path, schemas: Path | None) -> Result:
    result = Result("JSON parses and validates against published schemas")
    validated = not_validated = 0
    reasons: dict[str, int] = {}
    try:
        from jsonschema import validators
        from referencing import Registry, Resource
        from referencing.jsonschema import DRAFT7
    except ImportError:
        validators = None

    def retrieve(uri: str):
        if not uri.startswith(SCHEMA_URL_PREFIX):
            raise LookupError(uri)
        return Resource.from_contents(
            rm.load_json(schemas / uri[len(SCHEMA_URL_PREFIX):]), default_specification=DRAFT7)

    for path in json_files(root):
        try:
            document = rm.load_json(path)
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            result.fail(f"{rel(root, path)}: not valid JSON ({error})")
            continue
        url = document.get("$schema") if isinstance(document, dict) else None
        reason = None
        if not url:
            reason = "no $schema declared"
        elif schemas is None or validators is None:
            reason = "schema checkout not provided"
        elif not url.startswith(SCHEMA_URL_PREFIX):
            reason = "schema is not a Microsoft published schema"
        elif not (schemas / url[len(SCHEMA_URL_PREFIX):]).exists():
            reason = f"schema not published: {url[len(SCHEMA_URL_PREFIX):]}"
        if reason:
            not_validated += 1
            reasons[reason] = reasons.get(reason, 0) + 1
            continue
        schema = rm.load_json(schemas / url[len(SCHEMA_URL_PREFIX):])
        validator = validators.validator_for(schema)(schema, registry=Registry(retrieve=retrieve))
        errors = sorted(validator.iter_errors(document), key=lambda e: list(e.path))
        if errors:
            first = errors[0]
            where = "/".join(str(p) for p in first.path) or "(root)"
            result.fail(f"{rel(root, path)}: schema validation failed at {where}: {first.message[:200]}")
        else:
            validated += 1

    result.note(f"validated: {validated}")
    result.note(f"not schema-validated: {not_validated}")
    for reason, count in sorted(reasons.items()):
        result.note(f"  {count} x {reason}")
    result.note(f"failed: {len(result.failures)}")
    return result


# 2. Item identity and references ------------------------------------------------

def check_items_and_references(root: Path) -> Result:
    result = Result("Item identities and internal references")
    seen: dict[str, str] = {}
    for folder in rm.item_folders(root):
        platform = folder / ".platform"
        if not platform.exists():
            result.fail(f"{rel(root, folder)}: missing .platform")
            continue
        meta = rm.load_json(platform)
        logical_id = meta.get("config", {}).get("logicalId")
        item_type = meta.get("metadata", {}).get("type")
        display = meta.get("metadata", {}).get("displayName")
        if not logical_id:
            result.fail(f"{rel(root, platform)}: no logicalId")
        elif logical_id in seen:
            result.fail(f"{rel(root, platform)}: logicalId also used by {seen[logical_id]}")
        else:
            seen[logical_id] = rel(root, folder)
        if f"{display}.{item_type}" != folder.name:
            result.fail(f"{rel(root, folder)}: folder name does not match displayName.type "
                        f"({display}.{item_type})")

    ids = rm.logical_ids(root)
    checked = 0
    for name, pipeline in rm.pipelines(root).items():
        for activity in rm.walk_activities(pipeline["properties"].get("activities", [])):
            props = activity.get("typeProperties", {})
            workspace = props.get("workspaceId")
            for key in ("notebookId", "pipelineId"):
                if key not in props:
                    continue
                if workspace not in (None, rm.SAME_WORKSPACE_ID):
                    result.note(f"{name}/{activity['name']}: {key} points to another workspace, not checked")
                    continue
                checked += 1
                if props[key] not in ids:
                    result.fail(f"{name}/{activity['name']}: {key} {props[key]} is not an item in this repository")

    for pbir in (root / rm.FABRIC_DIR).glob("*.Report/definition.pbir"):
        target = rm.load_json(pbir).get("datasetReference", {}).get("byPath", {}).get("path")
        if target:
            checked += 1
            if not (pbir.parent / target).resolve().is_dir():
                result.fail(f"{rel(root, pbir)}: semantic model path {target} does not exist")
    result.note(f"{len(seen)} items, {checked} internal references resolved")
    return result


# 3. Pipeline wiring ---------------------------------------------------------------

def procedure_parameters(root: Path, qualified_name: str) -> tuple[Path, set[str] | None]:
    match = re.fullmatch(r"\[?(\w+)\]?\.\[?(\w+)\]?", qualified_name)
    if not match:
        return Path(qualified_name), None
    schema, name = match.groups()
    path = root / rm.WAREHOUSE_DIR / schema / "StoredProcedures" / f"{name}.sql"
    if not path.exists():
        return path, None
    text = path.read_text(encoding="utf-8-sig")
    header = re.split(r"\bAS\s+BEGIN\b", text, maxsplit=1, flags=re.I)[0]
    return path, {p.lower() for p in re.findall(r"@(\w+)", header)}


def check_pipeline_wiring(root: Path) -> Result:
    result = Result("Pipeline wiring, activity state and required parameters")
    all_pipelines = rm.pipelines(root)
    for name, pipeline in all_pipelines.items():
        activities = list(rm.walk_activities(pipeline["properties"].get("activities", [])))
        names = {a["name"] for a in activities}
        for activity in activities:
            label = f"{name}/{activity['name']}"
            state = activity.get("state", "Active")
            if state != "Active":
                result.fail(f"{label}: activity state is {state}; an inactive activity is skipped "
                            f"and reports {activity.get('onInactiveMarkAs', 'Succeeded')}")
            for dependency in activity.get("dependsOn", []):
                if dependency.get("activity") not in names:
                    result.fail(f"{label}: depends on missing activity {dependency.get('activity')}")
            props = activity.get("typeProperties", {})
            if activity.get("type") == "SqlServerStoredProcedure":
                check_procedure_activity(root, label, activity, props, result)
            for key, parameter in (props.get("parameters") or {}).items():
                if isinstance(parameter, dict) and parameter.get("value") is None:
                    result.fail(f"{label}: parameter {key} is null")

    gold = all_pipelines.get(Path(rm.GOLD_PIPELINE).parent.name)
    if gold is None:
        result.fail(f"{rm.GOLD_PIPELINE} not found")
        return result
    activities = {a["name"]: a for a in rm.walk_activities(gold["properties"]["activities"])}
    facts = sorted(n for n in activities if FACT_ACTIVITY.match(n))
    if len(facts) != 6:
        result.fail(f"pl_gold_load: expected 6 fact activities, found {len(facts)}: {facts}")
    for fact in facts:
        procedure = activities[fact].get("typeProperties", {}).get("storedProcedureName", "")
        expected = f"usp_load_{FACT_ACTIVITY.match(fact).group(1)}"
        if not re.fullmatch(rf"\[?fact\]?\.\[?{expected}\]?", procedure):
            result.fail(f"pl_gold_load/{fact}: calls {procedure}, expected [fact].[{expected}]")
    assert_activity = activities.get(COMPLETENESS_ACTIVITY)
    if assert_activity is None:
        result.fail(f"pl_gold_load: {COMPLETENESS_ACTIVITY} not found")
    else:
        upstream = {d["activity"]: d.get("dependencyConditions", [])
                    for d in assert_activity.get("dependsOn", [])}
        for fact in facts:
            if fact not in upstream:
                result.fail(f"pl_gold_load/{COMPLETENESS_ACTIVITY}: does not depend on {fact}")
            elif upstream[fact] != ["Succeeded"]:
                result.fail(f"pl_gold_load/{COMPLETENESS_ACTIVITY}: dependency on {fact} is "
                            f"{upstream[fact]}, expected ['Succeeded']")
    result.note(f"{len(facts)} fact activities mapped; {COMPLETENESS_ACTIVITY} depends on all of them")
    return result


def check_procedure_activity(root, label, activity, props, result):
    procedure = props.get("storedProcedureName", "")
    path, declared = procedure_parameters(root, procedure)
    if declared is None:
        result.fail(f"{label}: procedure {procedure} has no script at {rel(root, path) if path.is_absolute() else path}")
        return
    passed = props.get("storedProcedureParameters") or {}
    for key, parameter in passed.items():
        if key.lower() not in declared:
            result.fail(f"{label}: passes @{key}, which {procedure} does not declare")
        value = parameter.get("value") if isinstance(parameter, dict) else parameter
        if value is None:
            result.fail(f"{label}: parameter {key} is null")
    if "run_id" in declared:
        value = (passed.get("run_id") or {}).get("value")
        expression = value.get("value") if isinstance(value, dict) else value
        if expression != RUN_ID_EXPRESSION:
            result.fail(f"{label}: run_id is {expression!r}, expected {RUN_ID_EXPRESSION}")
    if "allow_mass_delete" in passed:
        value = passed["allow_mass_delete"].get("value")
        if str(value).lower() != "false":
            result.fail(f"{label}: allow_mass_delete is {value!r}; production runs must pass false")


# 4. Notebook syntax -------------------------------------------------------------------

CELL_MARKER = re.compile(r"^# (CELL|PARAMETERS CELL|MARKDOWN|METADATA) \*+\s*$", re.M)
MAGIC_LINE = re.compile(r"^(\s*)[%!]")


def check_notebooks(root: Path) -> Result:
    result = Result(f"Notebook Python syntax (Python {sys.version_info.major}.{sys.version_info.minor})")
    for path in sorted((root / rm.FABRIC_DIR).glob("*.Notebook/notebook-content.py")):
        text = path.read_text(encoding="utf-8")
        if not text.startswith("# Fabric notebook source"):
            result.fail(f"{rel(root, path)}: missing the Fabric notebook source header")
        kinds = CELL_MARKER.findall(text)
        bodies = CELL_MARKER.split(text)[2::2]
        compiled = skipped_cells = magic_lines = 0
        for number, (kind, body) in enumerate(zip(kinds, bodies), start=1):
            if kind not in ("CELL", "PARAMETERS CELL"):
                continue
            lines = [line for line in body.splitlines() if not line.startswith("# META")]
            if "\n".join(lines).lstrip().startswith("%%"):
                skipped_cells += 1
                continue
            code = []
            for line in lines:
                magic = MAGIC_LINE.match(line)
                if magic:
                    magic_lines += 1
                    code.append(f"{magic.group(1)}pass")
                else:
                    code.append(line)
            try:
                compile("\n".join(code), f"{rel(root, path)} cell {number}", "exec")
                compiled += 1
            except SyntaxError as error:
                result.fail(f"{rel(root, path)}: cell {number} line {error.lineno}: {error.msg}")
        result.note(f"{path.parent.name}: {compiled} cells compiled, "
                    f"{skipped_cells} cell magics and {magic_lines} line magics skipped")
    return result


# 5. Report fields and model inventory --------------------------------------------------

def field_references(node):
    if isinstance(node, dict):
        for kind in ("Column", "Measure"):
            item = node.get(kind)
            if isinstance(item, dict) and "Property" in item:
                entity = item.get("Expression", {}).get("SourceRef", {}).get("Entity")
                if entity:
                    yield kind, entity, item["Property"]
        for value in node.values():
            yield from field_references(value)
    elif isinstance(node, list):
        for value in node:
            yield from field_references(value)


def check_report_fields(root: Path) -> Result:
    result = Result("Report fields exist in the semantic model")
    tables = rm.model_tables(root)
    count = 0
    for path in sorted((root / rm.REPORT_DIR).rglob("*.json")):
        for kind, entity, prop in field_references(rm.load_json(path)):
            count += 1
            table = tables.get(entity)
            if table is None:
                result.fail(f"{rel(root, path)}: table '{entity}' is not in the model")
            elif prop not in table["columns"] | table["measures"]:
                result.fail(f"{rel(root, path)}: {kind.lower()} '{entity}'[{prop}] is not in the model")
    result.note(f"{count} field references checked")
    return result


def check_readme_counts(root: Path) -> Result:
    result = Result("README model counts match the semantic model")
    tables = rm.model_tables(root)
    direct_lake = sum(t["direct_lake"] for t in tables.values())
    measures = sum(len(t["measures"]) for t in tables.values())
    relationships, inactive = rm.model_relationships(root)
    readme = (root / "README.md").read_text(encoding="utf-8")
    stated = re.search(r"(\d+) tables, (\d+) relationships and (\d+) measures", readme)
    if not stated:
        result.fail("README.md: sentence 'N tables, N relationships and N measures' not found; "
                    "update the README or this check")
    else:
        actual = (direct_lake, relationships, measures)
        claimed = tuple(int(x) for x in stated.groups())
        for label, said, real in zip(("Direct Lake tables", "relationships", "measures"), claimed, actual):
            if said != real:
                result.fail(f"README.md states {said} {label}; the model has {real}")
    result.note(f"model: {direct_lake} Direct Lake tables, {len(tables)} table files, "
                f"{relationships} relationships ({inactive} inactive), {measures} measures")
    return result


# 6. Warehouse SQL names --------------------------------------------------------------

THREE_PART = re.compile(r"\[?\b(lh_logistics_silver|lh_logistics_ops|wh_logistics_gold)\]?\s*\.\s*"
                        r"\[?(\w+)\]?\s*\.\s*\[?(\w+)\]?", re.I)


def check_warehouse_names(root: Path) -> Result:
    result = Result("Warehouse SQL references lakehouse tables that exist")
    inventory = rm.external_inventory(root)
    seen = {db: set() for db in inventory}
    for path in rm.warehouse_sql_files(root):
        text = path.read_text(encoding="utf-8-sig")
        for match in THREE_PART.finditer(text):
            database, schema, table = match.groups()
            line = text.count("\n", 0, match.start()) + 1
            where = f"{rel(root, path)}:{line}"
            if database.lower() == rm.GOLD_DATABASE:
                result.fail(f"{where}: three-part self-reference {match.group(0)}; use schema.object")
                continue
            if database not in inventory or schema != rm.SILVER_SCHEMA:
                result.fail(f"{where}: {match.group(0)} must be spelled <lakehouse>.{rm.SILVER_SCHEMA}.<table>, "
                            f"with the lakehouse in lower case")
                continue
            tables = inventory[database]
            if table not in tables:
                close = [t for t in tables if t.lower() == table.lower()]
                hint = f" (did you mean {close[0]}?)" if close else ""
                result.fail(f"{where}: {table} is not a table in {database}{hint}")
            seen[database].add(table)
    for database, tables in inventory.items():
        result.note(f"{database}: {len(tables)} tables defined, {len(seen[database])} referenced by the warehouse")
    return result


# 7. Secrets ------------------------------------------------------------------------------

def tracked_files(root: Path) -> list[Path]:
    output = subprocess.run(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
                            cwd=root, capture_output=True, check=True).stdout
    return [root / name for name in output.decode().split("\0") if name]


def check_secrets(root: Path, files: list[Path] | None = None) -> Result:
    result = Result("No Slack webhook URLs in the repository")
    scanned = 0
    for path in files if files is not None else tracked_files(root):
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        scanned += 1
        for match in WEBHOOK_PATTERN.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            result.fail(f"{rel(root, path)}:{line}: Slack webhook URL")
    result.note(f"{scanned} text files scanned")
    return result


CHECKS = [check_items_and_references, check_pipeline_wiring, check_notebooks,
          check_report_fields, check_readme_counts, check_warehouse_names, check_secrets]


def run(root: Path, schemas: Path | None) -> list[Result]:
    results = [check_json_and_schemas(root, schemas)]
    for check in CHECKS:
        try:
            results.append(check(root))
        except Exception as error:  # a check that cannot run is a failure, never a pass
            failed = Result(check.__name__)
            failed.fail(f"check could not run: {type(error).__name__}: {error}")
            results.append(failed)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--schemas", type=Path, help="checkout of github.com/microsoft/json-schemas")
    args = parser.parse_args()
    root = args.root.resolve()
    results = run(root, args.schemas.resolve() if args.schemas else None)
    failed = 0
    for result in results:
        status = "FAIL" if result.failures else "PASS"
        failed += bool(result.failures)
        print(f"\n[{status}] {result.name}")
        for message in result.failures:
            print(f"  error: {message}")
        for message in result.notes:
            print(f"  {message}")
    print(f"\n{len(results) - failed} of {len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
