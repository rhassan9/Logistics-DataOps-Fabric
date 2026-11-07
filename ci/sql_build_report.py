"""Classify the output of `dotnet build` for the warehouse SQL project.

Gold reads two lakehouses by three-part name: lh_logistics_silver (the source of
every dimension and fact) and lh_logistics_ops (the publisher's baselines used by
reconciliation). Lakehouse SQL analytics endpoints are not SQL projects, so the
build cannot resolve those names. It reports them as errors inside views
(SQL71501, SQL71561) and as warnings inside procedures (SQL71562), and stops before
producing a .dacpac. An unresolved reference to a gold object inside a procedure
is also only a warning (SQL71502), so warnings must be checked too.

An unresolved-reference diagnostic, error or warning, is accepted only when:

  * it is raised in a script that names the lakehouse it refers to
  * every object it names is <lakehouse>.dbo.<table>[.<column>], and that table and
    column exist in the notebook that defines the lakehouse schema (exact spelling)
  * for "could refer to any of the following objects" lists, only the direct
    candidates count: entries written [schema].[object].[alias]::[column] are DacFx
    trying the unresolved alias against every object in scope, and name nothing

Anything else fails the check, including a failed build whose errors cannot be read.
Other warnings are listed in full and do not fail the check.

Usage:
    python ci/sql_build_report.py BUILD_LOG BUILD_EXIT_CODE [--root .]
        [--summary FILE] [--expect-failure-in RELATIVE_SQL_PATH]
"""

from __future__ import annotations

import argparse
import collections
import re
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import repo_model as rm  # noqa: E402

LINE = re.compile(r"^(?P<file>.+?\.sql)\((?P<row>\d+),(?P<col>\d+)\):\s+(?:Build\s+)?"
                  r"(?P<sev>error|warning)\s+(?P<code>[A-Z]+\d+):\s+(?P<msg>.*?)(?:\s+\[[^\]]*\])?\s*$")
OTHER_ERROR = re.compile(r"\berror\s+(?P<code>[A-Z]+\d+)\s*:\s*(?P<msg>.*)$")
NAME = re.compile(r"\[[^\]]+\](?:(?:\.|::)\[[^\]]+\])*")
UNRESOLVED_ERRORS = {"SQL71501", "SQL71561"}
UNRESOLVED_WARNINGS = {"SQL71502", "SQL71562"}
CANDIDATES = "could refer to any of the following objects"


@dataclass(frozen=True)
class Diagnostic:
    file: str
    row: int
    severity: str
    code: str
    message: str

    @property
    def unresolved(self) -> bool:
        return self.code in (UNRESOLVED_ERRORS if self.severity == "error" else UNRESOLVED_WARNINGS)


def parse(log: str) -> tuple[list[Diagnostic], list[str]]:
    diagnostics, unparsed = {}, []
    for raw in log.splitlines():
        text = raw.strip()
        match = LINE.match(text)
        if match:
            d = Diagnostic(match["file"].replace("\\", "/"), int(match["row"]), match["sev"],
                           match["code"], match["msg"])
            diagnostics[(d.file, d.row, d.severity, d.code, d.message)] = d
        elif OTHER_ERROR.search(text) and " 0 Error(s)" not in text:
            unparsed.append(text)
    return list(diagnostics.values()), sorted(set(unparsed))


def split_name(name: str) -> list[str]:
    return re.findall(r"\[([^\]]+)\]", name)


def name_status(name: str, inventory) -> tuple[str, str | None]:
    """('valid' | 'unknown-table' | 'unknown-column' | 'not-external', database)."""
    parts = split_name(name)
    if len(parts) < 3 or parts[0] not in inventory or parts[1] != rm.SILVER_SCHEMA:
        return "not-external", None
    tables = inventory[parts[0]]
    if parts[2] not in tables:
        return "unknown-table", parts[0]
    if len(parts) == 3:
        return "valid", parts[0]
    if len(parts) == 4:
        return ("valid" if parts[3] in tables[parts[2]] else "unknown-column"), parts[0]
    return "not-external", parts[0]


def classify(d: Diagnostic, inventory, files_by_database) -> tuple[bool, str]:
    if "reference" not in d.message:
        return False, "message names no referenced object"
    names = NAME.findall(d.message.split("reference", 1)[1])
    candidates = CANDIDATES in d.message
    if candidates:
        names = [n for n in names if "::" not in n]
    if not names:
        return False, "message names no referenced object"
    results = [name_status(n, inventory) for n in names]
    statuses = {status for status, _ in results}
    if "not-external" in statuses:
        return False, "names an object that is not a lakehouse table (a gold object or another database)"
    for _, database in results:
        if not any(d.file.endswith(f) for f in files_by_database[database]):
            return False, f"raised in a script that does not read {database}"
    if candidates and "valid" not in statuses:
        return False, f"no candidate exists in the lakehouse schema ({', '.join(sorted(statuses))})"
    if not candidates and statuses != {"valid"}:
        return False, f"name not in the lakehouse schema ({', '.join(sorted(statuses))})"
    return True, "verified lakehouse reference"


def report(log: str, exit_code: int, root: Path, expect_failure_in: str | None = None):
    inventory = rm.external_inventory(root)
    sql = {p.relative_to(root).as_posix(): p.read_text(encoding="utf-8-sig")
           for p in rm.warehouse_sql_files(root)}
    files_by_database = {db: {f for f, text in sql.items() if db in text} for db in inventory}
    diagnostics, unparsed = parse(log)
    errors = [d for d in diagnostics if d.severity == "error"]
    warnings = [d for d in diagnostics if d.severity == "warning"]

    accepted, rejected = [], []
    for d in diagnostics:
        if d.unresolved:
            ok, reason = classify(d, inventory, files_by_database)
        elif d.severity == "error":
            ok, reason = False, f"{d.code} is not an unresolved-reference error"
        else:
            continue
        (accepted if ok else rejected).append((d, reason))
    other_warnings = [d for d in warnings if not d.unresolved]

    if exit_code == 0 and not rejected:
        verdict, passed = "Build succeeded and produced a .dacpac", True
    elif exit_code != 0 and not errors and not unparsed:
        verdict, passed = "Build failed without any readable error: inspect the raw log", False
    elif rejected or unparsed:
        verdict, passed = "Build has diagnostics that are not verified lakehouse references", False
    else:
        verdict, passed = ("No .dacpac: every unresolved reference names a lakehouse table or "
                           "column that exists in the notebook defining that lakehouse"), True

    if expect_failure_in is not None:
        caught = any(d.file.endswith(expect_failure_in) for d, _ in rejected)
        verdict = (f"Self-test {'passed' if caught else 'FAILED'}: injected defect in "
                   f"{expect_failure_in} was {'caught' if caught else 'NOT caught'}. ({verdict})")
        passed = caught

    def count(items, severity):
        return sum(1 for d, _ in items if d.severity == severity)

    out = [f"### Warehouse SQL build: {'PASS' if passed else 'FAIL'}", "", f"- {verdict}",
           f"- dotnet build exit code: {exit_code}",
           f"- Errors: {len(errors)} ({count(accepted, 'error')} verified lakehouse references, "
           f"{count(rejected, 'error')} rejected)",
           f"- Warnings: {len(warnings)} {dict(collections.Counter(d.code for d in warnings))} "
           f"({count(accepted, 'warning')} verified lakehouse references, "
           f"{count(rejected, 'warning')} rejected, {len(other_warnings)} other)"]
    if unparsed:
        out += ["", "Errors outside SQL scripts:", ""] + [f"- `{u[:300]}`" for u in unparsed]
    if rejected:
        out += ["", "Diagnostics that fail the check:", ""]
        out += [f"- `{d.file.split('/')[-1]}:{d.row}` {d.severity} {d.code} ({reason}): {d.message[:600]}"
                for d, reason in rejected[:40]]
    if accepted:
        by_file = collections.Counter(d.file.split("/")[-1] for d, _ in accepted)
        out += ["", "<details><summary>Verified lakehouse references by script</summary>", ""]
        out += [f"- {name}: {n}" for name, n in sorted(by_file.items())]
        out += ["", "</details>"]
    if other_warnings:
        out += ["", "<details><summary>Other warnings</summary>", ""]
        out += [f"- `{d.file.split('/')[-1]}:{d.row}` {d.code}: {d.message}" for d in other_warnings]
        out += ["", "</details>"]
    return passed, "\n".join(out) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("log", type=Path)
    parser.add_argument("exit_code", type=int)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--summary", type=Path, help="append the Markdown report to this file")
    parser.add_argument("--expect-failure-in", help="self-test: the build must fail in this script")
    args = parser.parse_args()
    passed, text = report(args.log.read_text(encoding="utf-8", errors="replace"), args.exit_code,
                          args.root.resolve(), args.expect_failure_in)
    print(text)
    if args.summary:
        with args.summary.open("a", encoding="utf-8") as handle:
            handle.write(text)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
