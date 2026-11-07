"""Classify the output of `dotnet build` for the warehouse SQL project.

Gold reads silver through the silver lakehouse's SQL analytics endpoint. That
endpoint is not a SQL project, so the build cannot resolve those names and stops
before producing a .dacpac. This script separates those expected errors from real
ones, and treats an error as expected only when all of these hold:

  * the code is SQL71501 or SQL71561 (unresolved reference)
  * the error is raised in a script that names lh_logistics_silver
  * every object it names is lh_logistics_silver.dbo.<table>[.<column>], and that
    table and column exist in the silver DDL notebook (exact spelling)

Anything else fails the check, including a failed build whose errors cannot be
read. Warnings are listed in full and do not fail the check.

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
OTHER_ERROR = re.compile(r"\b(?:error)\s+(?P<code>[A-Z]+\d+)\s*:\s*(?P<msg>.*)$")
NAME = re.compile(r"\[[^\]]+\](?:(?:\.|::)\[[^\]]+\])*")
UNRESOLVED_CODES = {"SQL71501", "SQL71561"}


@dataclass(frozen=True)
class Diagnostic:
    file: str
    row: int
    severity: str
    code: str
    message: str


def parse(log: str) -> tuple[list[Diagnostic], list[str]]:
    diagnostics, unparsed = {}, []
    for raw in log.splitlines():
        text = raw.strip()
        match = LINE.match(text)
        if match:
            d = Diagnostic(match["file"].replace("\\", "/"), int(match["row"]), match["sev"],
                           match["code"], match["msg"])
            diagnostics[(d.file, d.row, d.code, d.message)] = d
        elif OTHER_ERROR.search(text) and " 0 Error(s)" not in text:
            unparsed.append(text)
    return list(diagnostics.values()), sorted(set(unparsed))


def split_name(name: str) -> list[str]:
    return re.findall(r"\[([^\]]+)\]", name)


def silver_name_status(name: str, inventory: dict[str, set[str]]) -> str:
    """'valid', 'unknown-table', 'unknown-column', or 'not-silver'."""
    alias_form = "::" in name
    parts = split_name(name)
    if len(parts) < 3 or parts[0] != rm.SILVER_DATABASE or parts[1] != rm.SILVER_SCHEMA:
        return "not-silver"
    table, rest = parts[2], parts[3:]
    if table not in inventory:
        return "unknown-table"
    if not rest:
        return "valid"
    if alias_form and len(rest) == 2:
        rest = rest[1:]
    if len(rest) != 1:
        return "not-silver"
    return "valid" if rest[0] in inventory[table] else "unknown-column"


def classify(d: Diagnostic, inventory, silver_files: set[str]) -> tuple[bool, str]:
    if d.code not in UNRESOLVED_CODES:
        return False, f"{d.code} is not an unresolved-reference error"
    if not any(d.file.endswith(f) for f in silver_files):
        return False, "raised in a script that does not read silver"
    if "reference" not in d.message:
        return False, "message names no referenced object"
    head, referenced = d.message.split("reference", 1)
    names = NAME.findall(referenced)
    if not names:
        return False, "message names no referenced object"
    candidates = "could refer to any of the following objects" in d.message
    subject = split_name(NAME.findall(head)[0])[:2] if NAME.findall(head) else None
    if candidates and subject:
        # DacFx can also list the referencing object's own alias-scoped form
        # ([schema].[object].[alias]::[column]); it names no other object.
        names = [n for n in names if split_name(n)[:2] != subject]
        if not names:
            return False, "candidate list names no silver object"
    statuses = [silver_name_status(n, inventory) for n in names]
    if "not-silver" in statuses:
        return False, "names an object outside lh_logistics_silver.dbo"
    if candidates:
        if "valid" not in statuses:
            return False, f"no candidate exists in the silver DDL ({', '.join(sorted(set(statuses)))})"
    elif any(s != "valid" for s in statuses):
        return False, f"silver name not in the silver DDL ({', '.join(sorted(set(statuses)))})"
    return True, "verified silver reference"


def report(log: str, exit_code: int, root: Path, expect_failure_in: str | None = None):
    inventory = rm.silver_inventory(root)
    silver_files = {p.relative_to(root).as_posix() for p in rm.warehouse_sql_files(root)
                    if rm.SILVER_DATABASE in p.read_text(encoding="utf-8-sig")}
    diagnostics, unparsed = parse(log)
    errors = [d for d in diagnostics if d.severity == "error"]
    warnings = [d for d in diagnostics if d.severity == "warning"]
    expected, rejected = [], []
    for d in errors:
        ok, reason = classify(d, inventory, silver_files)
        (expected if ok else rejected).append((d, reason))

    if exit_code == 0:
        verdict, passed = "Build succeeded and produced a .dacpac", True
    elif not errors and not unparsed:
        verdict, passed = "Build failed without any readable error: inspect the raw log", False
    elif rejected or unparsed:
        verdict, passed = "Build has errors that are not verified silver references", False
    else:
        verdict, passed = ("No .dacpac: every error is a reference to a silver table or column "
                           "that exists in the silver DDL"), True

    if expect_failure_in is not None:
        caught = any(d.file.endswith(expect_failure_in) for d, _ in rejected)
        verdict = (f"Self-test {'passed' if caught else 'FAILED'}: injected defect in "
                   f"{expect_failure_in} was {'caught' if caught else 'NOT caught'}. ({verdict})")
        passed = caught

    out = [f"### Warehouse SQL build: {'PASS' if passed else 'FAIL'}", "", f"- {verdict}",
           f"- dotnet build exit code: {exit_code}",
           f"- Errors: {len(errors)} ({len(expected)} verified silver references, {len(rejected)} other)",
           f"- Warnings: {len(warnings)} {dict(collections.Counter(d.code for d in warnings))}"]
    if unparsed:
        out += ["", "Errors outside SQL scripts:", ""] + [f"- `{u[:300]}`" for u in unparsed]
    if rejected:
        out += ["", "Errors that fail the check:", ""]
        out += [f"- `{d.file.split('/')[-1]}:{d.row}` {d.code} ({reason}): {d.message[:400]}"
                for d, reason in rejected[:40]]
    if expected:
        by_file = collections.Counter(d.file.split("/")[-1] for d, _ in expected)
        out += ["", "<details><summary>Verified silver references by script</summary>", ""]
        out += [f"- {name}: {count}" for name, count in sorted(by_file.items())]
        out += ["", "Messages:", ""]
        out += [f"- `{d.file.split('/')[-1]}:{d.row}` {d.code}: {d.message}" for d, _ in expected]
        out += ["", "</details>"]
    if warnings:
        out += ["", "<details><summary>All warnings</summary>", ""]
        out += [f"- `{d.file.split('/')[-1]}:{d.row}` {d.code}: {d.message}" for d in warnings]
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
