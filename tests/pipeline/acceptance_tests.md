# Pipeline acceptance tests

These prove that `pl_logistics_daily` and `pl_gold_load` behave correctly when something goes wrong, not only when every step turns green. Each test breaks the real pipeline on purpose and checks the state it leaves behind. Results are summarised in [docs/Gold.md](../../docs/Gold.md#pipeline-acceptance-tests).

Run them in order, each from a clean, green state, in a development workspace.

| # | Test | Proves |
|---|---|---|
| 2 | No-change run | an idle run touches nothing and passes |
| 3 | Detection interrupted, rerun with the same run_id | a retried detection is safe |
| 4a | A fact procedure fails; rerun it with the same run_id | a failure after detection loses nothing; the failed step catches up alone |
| 4b | A fact procedure fails; recover with a fresh run | a new run re-reads the window the failed run never consumed |
| 5 | Deletion and restoration through the pipeline | soft deletes flow to gold, and reconciliation catches the drift |
| 6 | Retry with increasing delay | observed during 4a |
| 7 | An upstream failure blocks everything downstream | dependency conditions hold |
| 8 | Overlapping runs | a second run queues behind the first |
| 1 | Scheduled run | unattended runs authenticate and succeed |

## Setup

The source is static, so the tests need changes. Create a notebook `nb_test_touch_silver` with default lakehouse `lh_logistics_silver` from [`touch_silver.py`](touch_silver.py), and mark Cell 1 as the parameter cell. It is test-only: it writes to silver directly and is not deployed with the workspace.

- `touch` rewrites `n` trips with their own values. Delta records them in the change feed as updated, so detection picks them up, but no value changes and reconciliation still passes.
- `delete` flags `n` trips deleted and prints `keys_csv` and `restore_from_version`.
- `restore` copies the flag back from that version.

Keep these two queries open in a warehouse tab:

```sql
-- Q1: the run's log
DECLARE @run_id VARCHAR(64) = '<gold run_id>';
SELECT r.step_name, r.event_type, r.rows_read, r.rows_updated, r.rows_deleted, r.event_at, r.message
FROM log.etl_run AS r WHERE r.run_id = @run_id ORDER BY r.event_at;

-- Q2: the checkpoints
SELECT c.step_name, c.source_table, c.last_version
FROM log.vw_cdf_checkpoint AS c ORDER BY c.step_name, c.source_table;
```

The gold run_id is the **Gold run_id** line of the Slack message, or `pl-` followed by the parent run ID of `pl_logistics_daily`.

---

## Test 2: no-change run

Run `pl_logistics_daily` with silver unchanged. Every step runs once with 0 keys, Q2 is unchanged, and reconciliation passes.

## Test 3: detection interrupted, rerun with the same run_id

1. Run Q2 and keep the result.
2. Touch: `mode = "touch"`, `n = 100`, `offset = 0`.
3. Run `pl_gold_load` with `run_id = acc-t3`. When `Detect_Changes` has run for 60 to 90 seconds, cancel the run.
4. Inspect what the interruption left:
   ```sql
   SELECT 'cdf_batch' AS what, COUNT(*) AS n FROM log.cdf_batch WHERE run_id = 'acc-t3'
   UNION ALL SELECT 'trip worklist', COUNT(*) FROM stg.changed_trip_keys WHERE run_id = 'acc-t3'
   UNION ALL SELECT 'fact events', COUNT(*) FROM log.etl_run
             WHERE run_id = 'acc-t3' AND step_name LIKE 'fact[_]%';
   -- fact events 0; the other two depend on when the run was cancelled
   ```
5. Run `pl_gold_load` again with `run_id = acc-t3`.

**Pass:** all green; Q1 shows `fact_trip` reading 100 keys even if step 4 showed a partial worklist; Q2 shows `silver_trips` at the new version; the `acc-t3` worklist is empty.

## Test 4a: a fact procedure fails; rerun it with the same run_id

1. In `pl_gold_load`, open `Load_fact_trip`, **Settings**, and tick **Treat as null** on `allow_mass_delete`. The procedure now stops at its parameter check with error 50611 before touching anything. Save.
2. Run Q2 and keep the result. Touch with `n = 100`, `offset = 100`.
3. Run `pl_logistics_daily`.
   - `Load_fact_trip` fails after its retries; the other five facts succeed.
   - `Assert_Run_Complete`, `Reconcile_Baselines` and `Purge_Worklists` are skipped; the run fails.
   - The Slack alert names error 50611.
4. Check the state:
   ```sql
   DECLARE @run_id VARCHAR(64) = '<gold run_id>';
   SELECT COUNT(*) AS trip_keys_waiting FROM stg.changed_trip_keys WHERE run_id = @run_id;
   -- 100: the failed step's worklist is intact
   ```
   Q2: `fact_trip` unchanged; `fact_fuel_purchase` and `fact_load_lifecycle` advanced on `silver_trips`, because they read it too and succeeded.
5. Repair the activity: untick **Treat as null**, set `false`, save.
6. Recover by rerunning only the failed procedure with the same run_id:
   ```sql
   DECLARE @run_id VARCHAR(64) = '<gold run_id>';
   EXEC fact.usp_load_fact_trip @run_id = @run_id, @allow_mass_delete = 0;
   EXEC log.usp_assert_gold_run_complete @run_id = @run_id;
   EXEC log.usp_reconcile_baselines @run_id = @run_id;
   SELECT COUNT(*) AS trip_keys_waiting FROM stg.changed_trip_keys WHERE run_id = @run_id;   -- 0
   ```

**Pass:** Q1 shows `fact_trip` succeeded with 100 read and 100 updated; Q2 shows its checkpoint at the new version; checks 01, 02 and 09 in [`../gold/checks`](../gold/checks) return no rows.

**Test 6** is observed here: in the Monitoring hub, the start times of `Load_fact_trip`'s attempts show growing gaps.

## Test 4b: a fact procedure fails; recover with a fresh run

1. Break `Load_fact_trip` as in 4a. Touch with `n = 100`, `offset = 200`.
2. Run `pl_logistics_daily`. It fails as in 4a; call its gold run_id **X**.
3. Repair the activity and run `pl_logistics_daily` again; call its gold run_id **Y**.

**Pass:**
- Q1 for Y: `fact_trip` reads 100. Silver did not change between X and Y, so these keys can only come from the window X never consumed.
- Q2: `fact_trip` on `silver_trips` at the newest version.
- The purge preview in [`../gold/scenarios/run_id_and_purge_guards.md`](../gold/scenarios/run_id_and_purge_guards.md) (G2) shows X with `would_delete = 100`.
- `EXEC log.usp_assert_gold_run_complete @run_id = '<Y>';` passes.

Cleanup: `DELETE FROM stg.changed_trip_keys WHERE run_id = '<X>';`

## Test 5: deletion and restoration through the pipeline

1. Delete: `mode = "delete"`, `n = 3`, `offset = 1000`. Keep the printed `keys_csv` and `restore_from_version`.
2. Run `pl_logistics_daily`. Every fact step succeeds and `fact_trip` reads 3 keys. `Reconcile_Baselines` fails with 50802, correctly, because gold now has three fewer live trips than the static baseline. Slack alerts.
   ```sql
   SELECT t.trip_id, t.is_deleted_in_source
   FROM fact.fact_trip AS t WHERE t.trip_id IN (<the 3 keys, quoted>);
   -- is_deleted_in_source = 1 on all three
   ```
3. Restore: `mode = "restore"` with the saved `keys_csv` and `restore_from_version`.
4. Run `pl_logistics_daily` again.

**Pass:** all green, reconciliation passes, the three trips are live again, and checks 01 to 11 return no rows.

## Test 7: an upstream failure blocks everything downstream

1. In `pl_gold_load`, tick **Treat as null** on `Load_Dimensions`' `allow_mass_delete`. Save.
2. Run `pl_logistics_daily`.

**Pass:** `Load_Dimensions` fails with 50391; every later activity is skipped; the run fails and Slack alerts; Q2 is unchanged and Q1 has no events, because the wrapper rejects the call before it logs. Repair (`false`), save, and run once more: green.

## Test 8: overlapping runs

Run `pl_logistics_daily`, and click **Run** again while it is running.

**Pass:** the second run shows as queued in the Monitoring hub until the first finishes, and its first log event comes after the first run's `purge_worklists` event.

## Test 1: scheduled run

`pl_logistics_daily` runs daily at 00:00 Pakistan Standard Time (the schedule is versioned in `fabric/pl_logistics_daily.DataPipeline/.schedules`), with Fabric failure notifications to the owner.

**Pass:** the Monitoring hub shows the run with trigger type **Scheduled**, and Q1 shows a normal run. A scheduled run executes as the user who owns the schedule, so this also proves unattended runs authenticate.
