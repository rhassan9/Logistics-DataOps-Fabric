# Acceptance test A1: batch 0 over the existing silver tables

Batch 0 (`landing/logistics/batch_seq=000000`, INITIAL_SNAPSHOT, as of 2024-12-31) is applied
to the silver tables built from bronze. Because batch 0 is those same rows, applying it must
change nothing:

- the silver routine plans every table as unchanged and writes nothing: no table gets a new
  Delta commit
- every silver row hash equals a baseline captured before the run
- a gold run under the batch's run_id is a no-change run, the 11 gold checks return no rows,
  and the ten publisher comparisons match exactly
- the batch ends `completed` in the registry, and the next decision is `nothing_to_do`

On the way it proves the batch registry: the claim, the event rules, retries that write
nothing, and that two transactions on the registry lock cannot both commit.

Run every step in order in the development workspace. Each step lists what to expect;
stop at the first step that differs.

## 0. Before the run

| Check | Expected |
|---|---|
| `nb_silver_create_tables`, CELL 1 | `drop_existing = False` |
| `log.batch_manifest`, `log.batch_event` | both empty, or the step 2 queries show no batch |
| `landing/logistics/batch_seq=000000` | 12 table folders and `_manifest.json` |

## 1. Silver lineage columns

Run all cells of `nb_silver_create_tables`.

- CELL 6 prints `tables changed: 12`, each `added _source_batch_id, _source_sequence`. A
  second run prints `tables changed: 0`.
- CELL 7 prints `All tables ready.`

The existing rows hold null in both columns. This is a schema change only: no row is
rewritten.

## 2. Registry seed and checks that write nothing

In the warehouse:

```sql
EXEC log.usp_seed_batch_registry;
SELECT * FROM log.batch_claim_lock;                        -- one row: batch_registry
EXEC log.usp_claim_next_batch 'a1-probe', 'operator';      -- outcome nothing_to_do
EXEC log.usp_claim_next_batch NULL, 'operator';            -- error 50902
EXEC log.usp_record_batch_event 0, 'silver_applied', 'x', 'operator', NULL, NULL, 0;  -- error 50907
EXEC log.usp_record_batch_event 0, 'claimed', 'x', 'operator', NULL, NULL, NULL;      -- error 50906
SELECT COUNT(*) FROM log.batch_event;                      -- 0
```

**Two transactions on the lock row.** This needs two connections that each keep a transaction
open between statements, for example two query windows in VS Code with the mssql extension:

| Connection A | Connection B |
|---|---|
| `BEGIN TRAN; UPDATE log.batch_claim_lock SET held_by = 'a1-lock-test', touched_at = SYSUTCDATETIME() WHERE lock_name = 'batch_registry';` | |
| | `EXEC log.usp_claim_next_batch 'a1-probe-2', 'operator';` returns `nothing_to_do` and commits |
| `COMMIT;` fails with error 24556 or 24706 (update conflict) | |
| `SELECT held_by FROM log.batch_claim_lock;` returns `a1-probe-2` | |

The transaction that commits second fails and writes nothing, so two claims cannot both
succeed. If connection B waits instead of returning, commit A: B then fails with the
conflict error and `held_by` is `a1-lock-test`. Either way exactly one commits.

## 3. Baseline

Run `nb_silver_hash_baseline` with `action = "capture"`, `label = "a1-before-batch0"`.
It prints 541,930 rows captured across the 12 tables, the counts of section 7.5 of the
project record. A second capture with the same label is refused.

## 4. Discovery

Run `nb_batch_discover` with `run_id = "a1-discover-001"`.

- `new: batch_seq 0  logistics-000000  INITIAL_SNAPSHOT  landed ... valid`
- `next: claim batch_seq=0`
- `landed_at` equals the last-modified time of `_manifest.json` in the storage account
  (Azure portal, container `landing`).

A second run prints `nothing new to register`.

## 5. Claim, and the event rules

```sql
EXEC log.usp_claim_next_batch 'a1-batch0-001', 'operator';   -- outcome claimed, batch_seq 0
EXEC log.usp_claim_next_batch 'a1-batch0-001', 'operator';   -- outcome claimed, decision already_claimed
EXEC log.usp_claim_next_batch 'a1-other', 'operator';        -- outcome in_flight
EXEC log.usp_record_batch_event 0, 'completed', 'a1-batch0-001', 'operator', NULL, NULL, NULL;  -- error 50909
EXEC log.usp_record_batch_event 0, 'silver_applied', 'a1-other', 'operator', NULL, NULL, 0;     -- error 50908
SELECT event_type, run_id FROM log.batch_event;              -- one row: claimed, a1-batch0-001
```

## 6. Apply

Run `nb_silver_apply_batch` with `run_id = "a1-batch0-001"`, `batch_seq = 0`.

- validation: 0 problems
- every table plans `unchanged`, with `only batch 0, only silver 0, changed 0, repeated keys 0`
- exit value: `"outcome": "unchanged"`, `"rows_written": 0`, and `versions_before` equal to
  `versions_after` for all 12 tables

Then:

```sql
EXEC log.usp_record_batch_event 0, 'silver_applied', 'a1-batch0-001', 'operator',
     'A1: batch 0 unchanged over existing silver', NULL, 0;
EXEC log.usp_record_batch_event 0, 'silver_applied', 'a1-batch0-001', 'operator',
     'A1: batch 0 unchanged over existing silver', NULL, 0;  -- a retry: writes nothing
EXEC log.usp_record_batch_event 0, 'completed', 'a1-batch0-001', 'operator', NULL, NULL, NULL;
     -- error 50812: no gold step has succeeded under this run_id yet
SELECT event_type FROM log.batch_event ORDER BY event_at;    -- claimed, silver_applied
```

## 7. Silver against the baseline

Run `nb_silver_hash_baseline` with `action = "compare"`, `label = "a1-before-batch0"`.
Every table shows `changed 0, only base 0, only silver 0, repeated 0`, and the run prints
`silver equals the baseline`.

## 8. Gold

Run `pl_gold_load` with the parameter `run_id = a1-batch0-001`.

- the run succeeds
- `nb_gold_change_detection` stages no keys for any fact (`log.cdf_batch` rows for the run;
  no `stg` key rows under the run_id). Its change feed window holds step 1's schema change,
  so this also shows the feed reads across the added columns.
- every fact step in `log.etl_run` reports 0 rows inserted, updated and deleted
- `log.vw_reconciliation_latest`: all ten comparisons pass
- the 11 queries in `tests/gold/checks` return no rows

## 9. Completion

```sql
EXEC log.usp_record_batch_event 0, 'completed', 'a1-batch0-001', 'operator',
     'A1: gold no-change run, reconciliation passed', NULL, NULL;
SELECT batch_seq, state, owner_run_id FROM log.vw_batch_state;   -- 0, completed, a1-batch0-001
SELECT decision FROM log.vw_batch_next;                           -- nothing_to_do
```

## Evidence to keep

The date, the commit the workspace runs, the apply notebook's exit value, the baseline
comparison output, the `pl_gold_load` run link, and the reconciliation result.

## What replaces the batch 0 gate

`nb_landing_gate_batch0` checked batch 0 once. Each of its lasting checks now runs on every
batch, in `nb_silver_apply_batch` before any write, or is covered by a test:

| Gate check | Now |
|---|---|
| manifest | `manifest_problems` in discovery and again before applying, plus the registered content hash |
| batch_layout | `layout_problems` |
| files (size, sha256) | `file_digest` with the complete-read rule, then `file_problems` |
| readable | the apply notebook reads every file; an unreadable file rejects the batch |
| schema_vs_contract | `frame_problems` |
| count_vs_manifest | `frame_problems` |
| key_not_null, key_unique | `frame_problems` |
| count_vs_silver, keys_vs_silver | the snapshot plan: every key and row hash of silver against the batch, plus this test's baseline comparison |
| schema_vs_bronze | `ingestion/tests` (contract against `sql/01_create_schema.sql`); bronze is retired |
| count_vs_source | not carried over: the source row counts are fixed numbers of the Neon load, which is retired |
