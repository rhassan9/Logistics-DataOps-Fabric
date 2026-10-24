# Run id, purge and retry guards

These scenarios prove that the orchestration safeguards behave as `docs/Gold.md` describes: a detection can be repeated until a procedure consumes it, the purge never removes keys a recovery needs, a reconciliation retry keeps whole attempts, and the dimension wrapper is rerunnable.

They use the real data path and leave gold unchanged, because a full scan rebuilds every `fact_trip` row to the same values. Every run_id starts with `test-g`, so the log rows are easy to find. Run them in a development workspace.

Running the notebook by hand means: set Cell 1 to the values shown, then **Run all**. Set Cell 1 back to `run_id = ""`, all six `steps` and `full_scan_steps = ""` afterwards.

---

## G1. A detection may repeat until a fact procedure uses the run_id

**G1-a. First detection.** Cell 1: `run_id = "test-g1"`, `steps = "fact_trip"`, `full_scan_steps = "fact_trip"`. Expect `fact_trip: keys=85,410; full_scan=True`.

**G1-b. The same run_id again, before any procedure.** Run all again. Expect the line `repeating a detection no fact procedure has used; 2 earlier window(s) are superseded`, and `keys=85,410` again.

```sql
SELECT COUNT(*) AS worklist_rows, COUNT(DISTINCT k.trip_id) AS distinct_trips,
       COUNT(DISTINCT k.detected_at) AS detections
FROM stg.changed_trip_keys AS k
WHERE k.run_id = 'test-g1';
-- 170820 | 85410 | 2
```

**G1-c. The procedure reads each key once.**

```sql
DECLARE @run_id VARCHAR(64) = 'test-g1';
EXEC fact.usp_load_fact_trip @run_id = @run_id, @allow_mass_delete = 0;

SELECT r.event_type, r.rows_read, r.rows_inserted, r.rows_updated, r.rows_deleted
FROM log.etl_run AS r
WHERE r.run_id = 'test-g1' AND r.step_name = 'fact_trip';
-- succeeded | 85410 | 0 | 85410 | 0

SELECT COUNT(*) AS left_in_worklist FROM stg.changed_trip_keys WHERE run_id = 'test-g1';
-- 0: the commit removed both detections' rows
```

Checks 01, 02 and 09 pass, and the `fact_trip` checkpoint equals test-g1's `version_to` on both silver tables.

**G1-d. The same run_id after the procedure.** Run all once more with the G1-a values. Cell 2 stops with `run_id test-g1 has already been used by a fact procedure`.

---

## G2. The purge keeps keys a recovery needs

The purge only touches rows at least 24 hours old, so a same-day test cannot watch it delete. This preview applies the same rule without the age cutoff:

```sql
-- PREVIEW: what the purge would delete from stg.changed_trip_keys, ignoring age
SELECT k.run_id, COUNT(*) AS would_delete
FROM stg.changed_trip_keys AS k
WHERE EXISTS (SELECT 1 FROM log.cdf_batch AS b
              WHERE b.step_name = 'fact_trip'
                AND b.detected_at > k.detected_at
                AND EXISTS (SELECT 1 FROM log.etl_run AS r
                            WHERE r.run_id = b.run_id AND r.step_name = b.step_name
                              AND r.event_type = 'succeeded' AND r.event_at >= b.detected_at)
                AND (b.is_full_scan = 1
                     OR NOT EXISTS (SELECT 1 FROM log.cdf_batch AS o
                                    WHERE o.run_id = k.run_id
                                      AND o.step_name = 'fact_trip' AND o.is_full_scan = 1)))
GROUP BY k.run_id;
```

**G2-a. A failed full scan.** Notebook: `run_id = "test-g2-a"`, `steps = "fact_trip"`, `full_scan_steps = "fact_trip"`. Do not run the procedure; that stands in for a failure. Preview: no row for test-g2-a, because nothing later has succeeded.

**G2-b. A later incremental success does not supersede a full scan.** Notebook: `run_id = "test-g2-b"`, `steps = "fact_trip"`, `full_scan_steps = ""`, then:

```sql
DECLARE @run_id VARCHAR(64) = 'test-g2-b';
EXEC fact.usp_load_fact_trip @run_id = @run_id, @allow_mass_delete = 0;
```

Preview: still no row for test-g2-a. An incremental window does not cover what a full reconciliation was asked to check.

**G2-c. A later full-scan success does supersede it.** Notebook: `run_id = "test-g2-c"`, `steps = "fact_trip"`, `full_scan_steps = "fact_trip"`, then run the procedure with `test-g2-c`. Preview: `test-g2-a | 85410`.

**G2-d. The age floor holds, and the purge logs itself.**

```sql
EXEC stg.usp_purge_orphan_keys @run_id = 'test-g2-purge', @older_than_hours = 24;

SELECT COUNT(*) AS still_there FROM stg.changed_trip_keys WHERE run_id = 'test-g2-a';
-- 85410: superseded, but younger than 24 hours

SELECT r.event_type, r.rows_deleted
FROM log.etl_run AS r WHERE r.run_id = 'test-g2-purge';
-- succeeded | 0

EXEC stg.usp_purge_orphan_keys @run_id = 'test-g2-purge', @older_than_hours = 1;
-- error 50701
```

**Cleanup.** test-g2-a's keys are provably redundant, so remove them rather than waiting a day:

```sql
DELETE FROM stg.changed_trip_keys WHERE run_id = 'test-g2-a';
```

---

## G3. A reconciliation retry keeps whole attempts

```sql
EXEC log.usp_reconcile_baselines @run_id = 'test-g3';
EXEC log.usp_reconcile_baselines @run_id = 'test-g3';

SELECT COUNT(DISTINCT r.checked_at) AS attempts, COUNT(*) AS result_rows
FROM log.reconciliation_result AS r WHERE r.run_id = 'test-g3';
-- attempts 2; result_rows twice latest_rows below

SELECT COUNT(*) AS latest_rows, COUNT(DISTINCT l.checked_at) AS attempts_shown,
       SUM(CASE WHEN l.status = 'fail' THEN 1 ELSE 0 END) AS failed
FROM log.vw_reconciliation_latest AS l WHERE l.run_id = 'test-g3';
-- attempts_shown 1, failed 0

SELECT r.event_type, COUNT(*) AS n
FROM log.etl_run AS r
WHERE r.run_id = 'test-g3' AND r.step_name = 'reconcile_baselines'
GROUP BY r.event_type;
-- started 2, succeeded 2
```

---

## G4. The dimension wrapper is rerunnable and logs under the run_id

**G4-a. Before.** Run the profile query (members, special, versions, current_rows, deleted, max_key per dimension) in `fabric/wh_logistics_gold.Warehouse/.sharedqueries/load_all_dim_data.sql` and keep the result.

**G4-b. Run the wrapper.**

```sql
DECLARE @run_id VARCHAR(64) = 'test-g4';
EXEC dim.usp_load_dimensions @run_id = @run_id, @allow_mass_delete = 0;

SELECT r.event_type, r.message
FROM log.etl_run AS r
WHERE r.run_id = 'test-g4' ORDER BY r.event_at;
-- started, then succeeded
```

**G4-c. After.** Rerun the G4-a query. Every figure, `max_key` included, is identical: silver has not changed, so a correct load changes nothing. The junk dimension counts from `.sharedqueries/load_all_junk_dimensions.sql` are unchanged too.

**G4-d. Parameter guard.**

```sql
EXEC dim.usp_load_dimensions @run_id = 'test-g4-null', @allow_mass_delete = NULL;
-- error 50391

SELECT COUNT(*) AS events FROM log.etl_run WHERE run_id = 'test-g4-null';
-- 0: rejected before anything started
```
