/* Check 08: fact_load_lifecycle and fact_trip agree.

   The fulfilling driver and truck sit on the version valid at the dispatch
   date, and every live trip's resources and dispatch date equal the lifecycle
   row of its load. The cross-fact check is the one that exposed the reused
   run_id defect described in docs/Gold.md.

   Run in: wh_logistics_gold. Read only.
   Pass:   no rows. */

SELECT 'fulfilling driver on the wrong version' AS problem, COUNT(*) AS n
FROM fact.fact_load_lifecycle AS f
JOIN dim.dim_driver AS d ON d.driver_key = f.fulfilling_driver_key
WHERE f.fulfilling_driver_key > 0
  AND NOT (f.dispatched_date_key >= d.valid_from_date_key AND f.dispatched_date_key < d.valid_to_date_key)
HAVING COUNT(*) > 0

UNION ALL
SELECT 'fulfilling truck on the wrong version', COUNT(*)
FROM fact.fact_load_lifecycle AS f
JOIN dim.dim_truck AS t ON t.truck_key = f.fulfilling_truck_key
WHERE f.fulfilling_truck_key > 0
  AND NOT (f.dispatched_date_key >= t.valid_from_date_key AND f.dispatched_date_key < t.valid_to_date_key)
HAVING COUNT(*) > 0

UNION ALL
SELECT 'live trips disagreeing with their lifecycle row', COUNT(*)
FROM fact.fact_load_lifecycle AS f
JOIN fact.fact_trip AS t ON t.load_id = f.load_id AND t.is_deleted_in_source = 0
WHERE f.fulfilling_driver_key  <> t.driver_key
   OR f.fulfilling_truck_key   <> t.truck_key
   OR f.fulfilling_trailer_key <> t.trailer_key
   OR f.dispatched_date_key    <> t.dispatch_date_key
HAVING COUNT(*) > 0;
