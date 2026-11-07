# Tests

The tests that prove the gold layer, the pipeline and the semantic model. Design and results are in [docs/Gold.md](../docs/Gold.md#testing).

```
tests/
├── gold/
│   ├── checks/          read-only assertions: no rows returned means pass
│   └── scenarios/       controlled changes with expected outcomes
├── pipeline/            acceptance tests that break the real pipeline on purpose
└── semantic_model/      the model's figures against independent warehouse queries
```

There are four kinds of test, and they run differently:

| Kind | Changes data | Where it runs | Pass condition |
|---|---|---|---|
| Checks | no | any workspace, any time | no rows returned |
| Scenarios and acceptance tests | yes, in silver, gold or the pipeline | a development workspace only | the expected outcome each step states |
| Semantic model comparisons | no | DAX query view and the warehouse | the two sides return the same values |
| Continuous integration | no | GitHub Actions, on every push and pull request | every job passes |

## Checks

Each file in [`gold/checks`](gold/checks) is one read-only query against `wh_logistics_gold`. It looks for violations: no rows means the property holds, and every returned row names the failure and its size. Open a file in the warehouse SQL query editor and run it as a whole.

| Check | Proves |
|---|---|
| [01](gold/checks/01_facts_match_silver.sql) | five facts hold exactly silver's rows and values, compared both ways |
| [02](gold/checks/02_fact_trip_keys_resolve.sql) | every `fact_trip` key points at the right member, version or special member |
| [03](gold/checks/03_fact_delivery_event_keys_resolve.sql) | the same for `fact_delivery_event`, including the stop status junk key |
| [04](gold/checks/04_fact_fuel_purchase_keys_resolve.sql) | the same for `fact_fuel_purchase`, with route through trip and load |
| [05](gold/checks/05_fact_maintenance_keys_resolve.sql) | the same for `fact_maintenance` |
| [06](gold/checks/06_fact_safety_incident_keys_resolve.sql) | the same for `fact_safety_incident` |
| [07](gold/checks/07_fact_load_lifecycle_matches_oracle.sql) | `fact_load_lifecycle` equals an independent rebuild from silver, every column |
| [08](gold/checks/08_fact_load_lifecycle_agrees_with_fact_trip.sql) | the lifecycle and trip facts agree on resources, versions and dispatch date |
| [09](gold/checks/09_worklists_and_checkpoints.sql) | every step has exactly its checkpoints, and no successful run left worklist rows |
| [10](gold/checks/10_dimension_special_members.sql) | every dimension has its three special members, and every state resolves |
| [11](gold/checks/11_reconciliation_passes.sql) | the latest reconciliation against the publisher's baselines has no failing verified metric |

Checks 02 to 06 report `rows_with_unknown` without failing on it. A key that is legitimately Unknown already passes the rules, so a non-zero count with every other column at 0 describes the source data, not a defect.

The checks hold after any successful pipeline run. Check 11 fails by design after a real change in silver, because the publisher's baselines are static; see [Reconciliation](../docs/Gold.md#reconciliation).

## Scenarios

| File | Proves |
|---|---|
| [fact_change_scenarios.md](gold/scenarios/fact_change_scenarios.md) | value changes on parent tables, deletions, restores, Unknown-key healing, a late dimension version, orphaned keys, the lifecycle's pre-update image, and a forced full rebuild |
| [run_id_and_purge_guards.md](gold/scenarios/run_id_and_purge_guards.md) | a repeated detection, the purge's recovery rule and age floor, reconciliation retries and the dimension wrapper |
| [reconciliation_fails_on_mismatch.sql](gold/scenarios/reconciliation_fails_on_mismatch.sql) | reconciliation raises 50802 when gold drifts from a verified baseline; it rolls back everything, so it is safe anywhere |

The scenarios write to silver and gold directly, so they run in a development workspace. `fact_change_scenarios.md` ends with a forced full reconciliation that returns gold to silver's exact state, after which every check passes.

## Pipeline acceptance tests

[`pipeline/acceptance_tests.md`](pipeline/acceptance_tests.md) breaks `pl_logistics_daily` on purpose: an interrupted detection, a failing fact with both recovery paths, a deletion and restore, an upstream failure and overlapping runs. [`pipeline/touch_silver.py`](pipeline/touch_silver.py) creates the silver changes they need, because the source is static. It writes to silver directly and is never deployed with the workspace.

## Semantic model

| File | Run in | Use |
|---|---|---|
| [01_warehouse_checks.sql](semantic_model/01_warehouse_checks.sql) | warehouse | assertion: every relationship's key is unique, and the source price equals cost over gallons |
| [02_warehouse_figures.sql](semantic_model/02_warehouse_figures.sql) | warehouse | the expected figures, computed in T-SQL |
| [03_model_figures.dax](semantic_model/03_model_figures.dax) | DAX query view on `sm_logistics_gold` | the model's figures |

Queries V1 to V5 run on both sides and must return the same values: grand totals, every lane, the other seven requirements' measures, the Stop Date Basis month shift, and active trucks and drivers under year and lane filters. Both sides return rows, so these are compared side by side rather than asserted.

## Continuous integration

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs on every push and pull request. It needs no Fabric credentials and never connects to a workspace, so it proves the committed definitions are consistent, not that the pipeline produces correct data. That is proven by the checks, scenarios and acceptance tests above, run in Fabric.

| Job | Checks |
|---|---|
| Static checks | item JSON parses and, where Microsoft publishes a schema, validates against it; logicalIds are unique and every notebook, pipeline and model reference resolves; `pl_gold_load` calls the right procedure for each of the six facts, passes the run_id, never enables mass delete, and its completeness check waits for all six to succeed; notebooks parse as Python 3.13; every report field exists in the semantic model; the model counts in the README match the model; every lakehouse table the warehouse SQL names is created by the silver or ops notebooks; no Slack webhook URL is committed |
| Warehouse SQL build | `dotnet build` of the warehouse database project. A build cannot resolve lakehouse references, so each unresolved-reference error or warning is accepted only when it names a table or column the silver or ops notebooks create. Any other error fails the job |
| Build check catches ... | five runs that each inject one known defect on the runner (a syntax error, a missing gold table, a missing silver table, a missing silver column, a missing baseline column) and pass only if the build check rejects it |

Before validating the repository, the static checks run their own tests: each check must fail on a deliberate defect in a temporary copy.

Not covered by CI: data and anything that needs a workspace; the semantic model's TMDL, which is not JSON; and silver column names inside stored procedures, which the build reports only at table level. Report visuals whose schema version Microsoft has not published are parsed but reported as not schema-validated.
