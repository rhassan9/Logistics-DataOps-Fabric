/* Check 09: checkpoints and worklists are consistent.

   Every fact step has a checkpoint for each silver table its change detection
   reads, and no other. A fact procedure removes its run's worklist rows inside
   the transaction that publishes the fact, and logs success only after the
   commit, so no worklist row may belong to a run whose step succeeded. Rows of
   a failed run stay until the purge clears them, which is expected.

   Run in: wh_logistics_gold. Read only.
   Pass:   no rows. */

WITH expected AS (
              SELECT CAST('fact_trip' AS VARCHAR(64)) AS step_name, CAST('silver_trips' AS VARCHAR(64)) AS source_table
    UNION ALL SELECT 'fact_trip',            'silver_loads'
    UNION ALL SELECT 'fact_delivery_event',  'silver_delivery_events'
    UNION ALL SELECT 'fact_delivery_event',  'silver_loads'
    UNION ALL SELECT 'fact_fuel_purchase',   'silver_fuel_purchases'
    UNION ALL SELECT 'fact_fuel_purchase',   'silver_trips'
    UNION ALL SELECT 'fact_fuel_purchase',   'silver_loads'
    UNION ALL SELECT 'fact_maintenance',     'silver_maintenance_records'
    UNION ALL SELECT 'fact_safety_incident', 'silver_safety_incidents'
    UNION ALL SELECT 'fact_load_lifecycle',  'silver_loads'
    UNION ALL SELECT 'fact_load_lifecycle',  'silver_trips'
    UNION ALL SELECT 'fact_load_lifecycle',  'silver_delivery_events'
),
worklist AS (
              SELECT CAST('fact_trip' AS VARCHAR(64)) AS step_name, k.run_id FROM stg.changed_trip_keys AS k
    UNION ALL SELECT 'fact_delivery_event',  k.run_id FROM stg.changed_delivery_event_keys  AS k
    UNION ALL SELECT 'fact_fuel_purchase',   k.run_id FROM stg.changed_fuel_purchase_keys   AS k
    UNION ALL SELECT 'fact_maintenance',     k.run_id FROM stg.changed_maintenance_keys     AS k
    UNION ALL SELECT 'fact_safety_incident', k.run_id FROM stg.changed_safety_incident_keys AS k
    UNION ALL SELECT 'fact_load_lifecycle',  k.run_id FROM stg.changed_load_keys            AS k
)
SELECT 'checkpoint missing' AS problem, e.step_name, e.source_table,
       CAST(NULL AS VARCHAR(64)) AS run_id, CAST(NULL AS BIGINT) AS n
FROM expected AS e
WHERE NOT EXISTS (SELECT 1 FROM log.vw_cdf_checkpoint AS c
                  WHERE c.step_name = e.step_name AND c.source_table = e.source_table)

UNION ALL
SELECT 'unexpected checkpoint', c.step_name, c.source_table, NULL, NULL
FROM log.vw_cdf_checkpoint AS c
WHERE NOT EXISTS (SELECT 1 FROM expected AS e
                  WHERE e.step_name = c.step_name AND e.source_table = c.source_table)

UNION ALL
SELECT 'worklist rows left by a successful run', w.step_name, NULL, w.run_id, COUNT(*)
FROM worklist AS w
WHERE EXISTS (SELECT 1 FROM log.etl_run AS r
              WHERE r.run_id = w.run_id AND r.step_name = w.step_name AND r.event_type = 'succeeded')
GROUP BY w.step_name, w.run_id;
