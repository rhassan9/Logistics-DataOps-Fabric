"""Inject one known defect into the warehouse SQL on a CI runner, for the self-test.

Runs only in the self-test job, on the runner's throwaway checkout. It is never run
against a workspace and its edits are never committed. Prints the path of the script
it changed, which the self-test then expects the build report to reject.

Usage:
    python ci/inject_sql_failure.py {syntax,gold-reference,silver-table,silver-column,ops-column} [--root .]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import repo_model as rm  # noqa: E402

MUTATIONS = {
    # A gold-only view: no silver reference can hide the error.
    "syntax": ("log/Views/vw_cdf_checkpoint.sql", "SELECT w.step_name,", "SELEC w.step_name,"),
    # A gold reference inside a procedure that also reads silver.
    "gold-reference": ("fact/StoredProcedures/usp_load_fact_trip.sql",
                       "LEFT JOIN dim.dim_truck AS tk", "LEFT JOIN dim.dim_truck_missing AS tk"),
    # A silver table that does not exist.
    "silver-table": ("dim/Views/vw_src_truck.sql",
                     "FROM lh_logistics_silver.dbo.silver_trucks AS s",
                     "FROM lh_logistics_silver.dbo.silver_truckz AS s"),
    # A baseline column that does not exist, inside a procedure (reported as a warning).
    "ops-column": ("log/StoredProcedures/usp_reconcile_baselines.sql", "ON b.driver_id = g.driver_id", "ON b.driver_key = g.driver_id"),
    # A silver column that does not exist.
    "silver-column": ("dim/Views/vw_src_truck.sql",
                      "s.acquisition_mileage,", "s.acquisition_mileage_missing,"),
}


def inject(root: Path, mutation: str) -> str:
    relative, old, new = MUTATIONS[mutation]
    path = root / rm.WAREHOUSE_DIR / relative
    text = path.read_text(encoding="utf-8-sig")
    if text.count(old) != 1:
        raise SystemExit(f"anchor {old!r} found {text.count(old)} times in {path}; update MUTATIONS")
    path.write_text(text.replace(old, new), encoding="utf-8")
    return f"{rm.WAREHOUSE_DIR}/{relative}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("mutation", choices=sorted(MUTATIONS))
    parser.add_argument("--root", type=Path, default=Path("."))
    args = parser.parse_args()
    print(inject(args.root.resolve(), args.mutation))
    return 0


if __name__ == "__main__":
    sys.exit(main())
