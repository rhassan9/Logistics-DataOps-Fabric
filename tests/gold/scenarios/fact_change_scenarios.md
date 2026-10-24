# Fact change scenarios

These scenarios prove that change detection and the fact procedures handle each kind of change: a value change on a parent table, a deletion at source, a restore, gold-side damage, a dimension version created after the facts were loaded, and a forced full rebuild. The source is static, so each scenario makes its change directly in silver or gold.

**Run them only in a development workspace.** They write to `lh_logistics_silver` and to gold, bypassing bronze. Run setup code from a scratch notebook attached to `lh_logistics_silver` that is not part of any pipeline, and delete it afterwards: re-executing a setup cell repeats the change.

## How a scenario run works

1. Run `nb_gold_change_detection` with the parameters the scenario gives: `run_id`, `steps` and `full_scan_steps`. The notebook prints one line per step, for example `fact_trip: keys=85,410; full_scan=True`.
2. Run the procedures for the same steps with the same `run_id`, through one variable so no line carries a stale id:

   ```sql
   DECLARE @run_id VARCHAR(64) = '<run_id>';
   EXEC fact.usp_load_fact_trip            @run_id = @run_id, @allow_mass_delete = 0;
   EXEC fact.usp_load_fact_delivery_event  @run_id = @run_id, @allow_mass_delete = 0;
   EXEC fact.usp_load_fact_fuel_purchase   @run_id = @run_id, @allow_mass_delete = 0;
   EXEC fact.usp_load_fact_maintenance     @run_id = @run_id, @allow_mass_delete = 0;
   EXEC fact.usp_load_fact_safety_incident @run_id = @run_id, @allow_mass_delete = 0;
   EXEC fact.usp_load_fact_load_lifecycle  @run_id = @run_id, @allow_mass_delete = 0;
   ```

   Keep only the lines for the scenario's `steps`.
3. Read the run's log:

   ```sql
   SELECT r.step_name, r.event_type, r.rows_read, r.rows_inserted, r.rows_updated,
          r.rows_deleted, r.rows_unknown_member
   FROM log.etl_run AS r
   WHERE r.run_id = '<run_id>' AND r.event_type <> 'started'
   ORDER BY r.step_name;
   ```

   `rows_read` is the keys in the worklist, `rows_updated` the rows rebuilt that already existed, `rows_deleted` the rows newly soft-deleted.
4. Run the checks for the facts in scope from [`../checks`](../checks).

Each section keeps the `steps` it lists, so the expected key counts hold. Facts outside a section's steps catch up at the closing run.

When a run is done, set the notebook back to `run_id = ""`, all six `steps` and `full_scan_steps = ""`.

---

## A. fact_trip

`steps = "fact_trip"`. Checks 01 (fact_trip row) and 02 after every run.

### A1. Nothing changed

`run_id = "test-a1"`. Expect `keys=0; full_scan=False`, both batches with `version_from = version_to`, log read 0, checkpoint unchanged.

If check 02 reports `rows_with_unknown` above 0, those rows are re-tried on every run by design, and `keys` equals that number instead of 0.

### A2. A load changes route

Setup:

```python
L = "lh_logistics_silver.dbo.silver_loads"
row = spark.sql(f"""SELECT l.load_id, l.route_id FROM {L} AS l
                    WHERE l.route_id IS NOT NULL AND NOT l._is_deleted_in_source
                    ORDER BY l.load_id LIMIT 1""").first()
new_route = spark.sql(f"""SELECT MIN(r.route_id) AS r FROM lh_logistics_silver.dbo.silver_routes AS r
                          WHERE r.route_id <> '{row.route_id}'""").first().r
n = spark.sql(f"""SELECT COUNT(*) AS n FROM lh_logistics_silver.dbo.silver_trips
                  WHERE load_id = '{row.load_id}'""").first().n
spark.sql(f"""UPDATE {L} SET route_id = '{new_route}', _silver_updated_at = current_timestamp()
              WHERE load_id = '{row.load_id}'""")
print(f"load {row.load_id}: route {row.route_id} -> {new_route}; trips on this load: {n}")
```

`run_id = "test-a2"`. Expect `keys=n`, log read `n`, updated `n`. Checks pass, and the load's trips carry the new route:

```sql
SELECT f.trip_id, r.route_id FROM fact.fact_trip AS f
JOIN dim.dim_route AS r ON r.route_key = f.route_key
WHERE f.load_id = '<load_id>';
```

### A3. A trip is deleted at source

```python
T = "lh_logistics_silver.dbo.silver_trips"
t = spark.sql(f"""SELECT MAX(trip_id) AS id FROM {T}
                  WHERE load_id <> '<load_id from A2>'""").first().id
spark.sql(f"""UPDATE {T} SET _is_deleted_in_source = true, _silver_updated_at = current_timestamp()
              WHERE trip_id = '{t}'""")
print(t)
```

`run_id = "test-a3"`. Expect `keys=1`, log read 1, updated 1, deleted 1. The row stays in gold with `is_deleted_in_source = 1`, and check 01 passes with one fewer live row.

### A4. Restore through the real silver pipeline

Run `nb_silver_transactional` with `load_mode = "full"` and `tables_filter = "loads,trips"`. Bronze still holds the original values, so silver recomputes them. Each table's latest MERGE updates exactly one row:

```python
for t in ["loads", "trips"]:
    h = spark.sql(f"DESCRIBE HISTORY lh_logistics_silver.dbo.silver_{t} LIMIT 1").first()
    print(t, h.operation, h.operationMetrics.get("numTargetRowsUpdated"))
```

`run_id = "test-a4"`. Expect `keys=n+1`, log updated `n+1`, deleted 0. Checks pass, with A2's trips back on their original route.

### A5. An Unknown key heals with silver unchanged

```sql
UPDATE fact.fact_trip SET driver_key = -1
WHERE trip_id = (SELECT MIN(f.trip_id) FROM fact.fact_trip AS f
                 WHERE f.driver_key > 0 AND f.is_deleted_in_source = 0);
```

Check 02 now returns `bad_driver = 1`, which shows the check catches the damage. `run_id = "test-a5"`. Expect `keys=1` with both change feeds empty, log updated 1, and check 02 passing again.

### A6. A wrong version, then an orphaned key

Split one driver into two versions at 2024-01-01, as if a version had been created after the facts were loaded:

```sql
DECLARE @d VARCHAR(20) = (SELECT MIN(x.driver_id) FROM dim.dim_driver AS x
                          WHERE x.driver_key > 0 AND x.is_current = 1);

SELECT @d AS driver_id, COUNT(*) AS trips_to_move
FROM fact.fact_trip AS f JOIN dim.dim_driver AS d ON d.driver_key = f.driver_key
WHERE d.driver_id = @d AND f.dispatch_date_key >= 20240101;

UPDATE dim.dim_driver SET valid_to_date_key = 20240101, is_current = 0
WHERE driver_id = @d AND is_current = 1;

INSERT INTO dim.dim_driver
    (driver_id, driver_name, driver_home_terminal, employment_status, driver_hire_date,
     driver_termination_date, years_experience, driver_version_label,
     valid_from_date_key, valid_to_date_key, is_current, is_deleted, version_reason)
SELECT driver_id, driver_name, driver_home_terminal, employment_status, driver_hire_date,
       driver_termination_date, years_experience, driver_version_label,
       20240101, 99991231, 1, is_deleted, 'Test version'
FROM dim.dim_driver WHERE driver_id = @d AND valid_to_date_key = 20240101;
```

Call `trips_to_move` `m`; if it is 0, undo the two statements and pick another driver. Check 02 now returns `bad_driver = m`. `run_id = "test-a6a"`. Expect `keys=m`, updated `m`, and check 02 passing: the trips hold the test version's key.

Then remove the test version, leaving those trips on a key that no longer exists:

```sql
DECLARE @d VARCHAR(20) = (SELECT x.driver_id FROM dim.dim_driver AS x WHERE x.version_reason = 'Test version');

DELETE FROM dim.dim_driver WHERE driver_id = @d AND version_reason = 'Test version';

UPDATE dim.dim_driver SET valid_to_date_key = 99991231, is_current = 1
WHERE driver_id = @d AND valid_to_date_key = 20240101;
```

`run_id = "test-a6b"`. Expect `keys=m`, found by the orphaned-key rule, updated `m`, and check 02 passing. The deleted test row consumed one IDENTITY value, so `dim_driver`'s highest key ends one above its member count, which is harmless.

---

## B. Delivery event, fuel purchase, maintenance and safety incident

`steps = "fact_trip,fact_delivery_event,fact_fuel_purchase,fact_maintenance,fact_safety_incident"`. Checks 01 and 02 to 06 after every run.

Route reaches delivery events through their own load and fuel purchases through trip then load. Before relying on the first, confirm every event's load is its trip's load:

```sql
SELECT COUNT(*) AS events, SUM(CASE WHEN e.load_id <> t.load_id THEN 1 ELSE 0 END) AS load_mismatch
FROM lh_logistics_silver.dbo.silver_delivery_events AS e
JOIN lh_logistics_silver.dbo.silver_trips AS t ON t.trip_id = e.trip_id;
-- load_mismatch = 0
```

### B1. Nothing changed

`run_id = "test-b1"`. Every step `keys=0` (or its `rows_with_unknown`), `full_scan=False`, log read 0, checkpoints unchanged.

### B2. One load changes route

Run the A2 setup cell, then record what each fact picks up:

```sql
SELECT
    (SELECT COUNT(*) FROM lh_logistics_silver.dbo.silver_delivery_events AS e WHERE e.load_id = '<load_id>') AS events_on_load,
    (SELECT COUNT(*) FROM lh_logistics_silver.dbo.silver_fuel_purchases AS p
     JOIN lh_logistics_silver.dbo.silver_trips AS t ON t.trip_id = p.trip_id
     WHERE t.load_id = '<load_id>') AS purchases_on_load;
```

`run_id = "test-b2"`.

| Step | Expect |
|---|---|
| fact_trip | keys `n`, updated `n` |
| fact_delivery_event | keys `events_on_load`, updated the same |
| fact_fuel_purchase | keys `purchases_on_load`, updated the same |
| fact_maintenance, fact_safety_incident | keys 0 |

### B3. One row deleted in each table

```python
for table, key in [("delivery_events", "event_id"), ("fuel_purchases", "fuel_purchase_id"),
                   ("maintenance_records", "maintenance_id"), ("safety_incidents", "incident_id")]:
    t = f"lh_logistics_silver.dbo.silver_{table}"
    k = spark.sql(f"SELECT MAX({key}) AS k FROM {t}").first().k
    spark.sql(f"""UPDATE {t} SET _is_deleted_in_source = true, _silver_updated_at = current_timestamp()
                  WHERE {key} = '{k}'""")
    print(table, k)
```

`run_id = "test-b3"`. Each of the four steps: keys 1, updated 1, deleted 1; `fact_trip` keys 0. Each row stays in gold with `is_deleted_in_source = 1`.

### B4. Restore through the real silver pipeline

Run `nb_silver_transactional` with `load_mode = "full"` and `tables_filter = ""`. Silver's latest MERGE updates exactly one row on `silver_loads`, `silver_delivery_events`, `silver_fuel_purchases`, `silver_maintenance_records` and `silver_safety_incidents`, and none on `silver_trips`.

`run_id = "test-b4"`.

| Step | Expect |
|---|---|
| fact_trip | keys `n` (route restored) |
| fact_delivery_event | keys `events_on_load + 1` |
| fact_fuel_purchase | keys `purchases_on_load + 1` |
| fact_maintenance, fact_safety_incident | keys 1 |

If B3's deleted event or purchase belongs to B2's load, it is one key, not two, so that line is one lower. Deleted 0 everywhere, and every fact's live rows equal its rows.

---

## C. fact_load_lifecycle

All six steps. Checks 07 and 08 after every run, plus 01 and 02.

### C1. Three kinds of change

```python
S = "lh_logistics_silver.dbo"
a = spark.sql(f"SELECT MIN(load_id) AS k FROM {S}.silver_loads").first().k
b = spark.sql(f"SELECT MAX(load_id) AS k FROM {S}.silver_loads").first().k
c = spark.sql(f"SELECT MIN(load_id) AS k FROM {S}.silver_loads WHERE load_id > '{a}'").first().k
trip_b  = spark.sql(f"SELECT trip_id FROM {S}.silver_trips WHERE load_id = '{b}'").first().trip_id
event_c = spark.sql(f"""SELECT event_id FROM {S}.silver_delivery_events
                        WHERE load_id = '{c}' AND event_type = 'Pickup'""").first().event_id
fuel_ab = spark.sql(f"""SELECT COUNT(*) AS n FROM {S}.silver_fuel_purchases AS f
                        JOIN {S}.silver_trips AS t ON t.trip_id = f.trip_id
                        WHERE t.load_id IN ('{a}', '{b}')""").first().n
print("A", a, "| B", b, trip_b, "| C", c, event_c, "| fuel keys expected", fuel_ab)

# A: a genuine value change on the load.
spark.sql(f"""UPDATE {S}.silver_loads SET revenue = revenue + 0.01, _silver_updated_at = current_timestamp()
              WHERE load_id = '{a}'""")
# B: the load's only trip deleted at source.
spark.sql(f"""UPDATE {S}.silver_trips SET _is_deleted_in_source = true, _silver_updated_at = current_timestamp()
              WHERE trip_id = '{trip_b}'""")
# C: the load's Pickup event moved to load A. Only the pre-update image names C.
spark.sql(f"""UPDATE {S}.silver_delivery_events SET load_id = '{a}', _silver_updated_at = current_timestamp()
              WHERE event_id = '{event_c}'""")
```

`run_id = "test-c1"`.

| Step | Expect |
|---|---|
| fact_load_lifecycle | keys 3 (A, B, C), updated 3, inserted 0, deleted 0, unknown 0 |
| fact_trip | keys 2 (A's trip and B's trip), deleted 1 |
| fact_delivery_event | keys 3 (A's two events and the moved event) |
| fact_fuel_purchase | keys `fuel_ab` |
| fact_maintenance, fact_safety_incident | keys 0 |

Load C proves the pre-update image is read: without it, lifecycle keys would be 2 and check 07 would fail on C.

Row by row:
- **A:** revenue one cent higher; every milestone unchanged.
- **B:** dispatched, pickup and delivery date keys and all three fulfilling keys are Not Applicable (-2); all four lags null; `is_timestamp_reversed` 0; revenue unchanged and the row live.
- **C:** `pickup_date_key` -2; `dispatch_to_pickup_hours` and `pickup_to_delivery_hours` null; the delivery milestone and `booking_to_delivery_hours` unchanged.

### C2. Restore through the real silver pipeline

Run `nb_silver_transactional` with `load_mode = "full"` and `tables_filter = ""`. Silver's latest MERGE updates one row each on `silver_loads`, `silver_trips` and `silver_delivery_events`.

`run_id = "test-c2"`. The same key counts as C1; `fact_trip` restores B's trip with deleted 0; A's revenue returns to its original value. All checks pass.

---

## Closing run: forced full reconciliation

This leaves gold equal to silver row for row and key for key, whatever ran before it.

`run_id = "test-close"`, all six `steps`, and `full_scan_steps` naming all six facts. Every step: keys equal to its row count, updated the same, inserted 0, deleted 0, and the notebook line ending `fallback=...=forced`.

Then refresh the reconciliation so check 11 reads a current result, and run checks 01 to 11. All return no rows.

```sql
EXEC log.usp_reconcile_baselines @run_id = 'test-close';
```
