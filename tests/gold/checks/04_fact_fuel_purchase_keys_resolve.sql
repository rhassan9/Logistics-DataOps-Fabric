/* Check 04: every fact_fuel_purchase key points at the right dimension row.

   Truck and driver resolve on the version valid at the purchase date. Route
   resolves through trip then load. Location: a null city is Missing (0), a
   city with no member is Unknown (-1).

   The date rule is stricter than the procedure's: it expects the YYYYMMDD key,
   never -1, because every purchase date lies inside dim_date's range.

   rows_with_unknown is reported, not asserted.

   Run in: wh_logistics_gold. Read only.
   Pass:   no rows. */

WITH s AS (
    SELECT p.fuel_purchase_id, p.truck_id, p.driver_id, p.trip_id, p.location_city, p.purchase_date,
           t.trip_id AS trip_found, t.load_id, l.load_id AS load_found, l.route_id,
           CASE WHEN p.purchase_date IS NULL THEN 0
                ELSE YEAR(p.purchase_date) * 10000 + MONTH(p.purchase_date) * 100 + DAY(p.purchase_date) END AS event_key
    FROM lh_logistics_silver.dbo.silver_fuel_purchases AS p
    LEFT JOIN lh_logistics_silver.dbo.silver_trips AS t ON t.trip_id = p.trip_id
    LEFT JOIN lh_logistics_silver.dbo.silver_loads AS l ON l.load_id = t.load_id
),
r AS (
    SELECT
        SUM(CASE WHEN f.purchase_date_key = s.event_key THEN 0 ELSE 1 END) AS bad_date,
        SUM(CASE WHEN s.truck_id IS NULL THEN CASE WHEN f.truck_key = 0 THEN 0 ELSE 1 END
                 WHEN tk.truck_id = s.truck_id AND s.event_key >= tk.valid_from_date_key
                      AND s.event_key < tk.valid_to_date_key THEN 0
                 WHEN f.truck_key = -1 AND tkm.truck_id IS NULL THEN 0
                 ELSE 1 END) AS bad_truck,
        SUM(CASE WHEN s.driver_id IS NULL THEN CASE WHEN f.driver_key = 0 THEN 0 ELSE 1 END
                 WHEN dr.driver_id = s.driver_id AND s.event_key >= dr.valid_from_date_key
                      AND s.event_key < dr.valid_to_date_key THEN 0
                 WHEN f.driver_key = -1 AND drm.driver_id IS NULL THEN 0
                 ELSE 1 END) AS bad_driver,
        SUM(CASE WHEN s.trip_id IS NULL    THEN CASE WHEN f.route_key = 0  THEN 0 ELSE 1 END
                 WHEN s.trip_found IS NULL THEN CASE WHEN f.route_key = -1 THEN 0 ELSE 1 END
                 WHEN s.load_id IS NULL    THEN CASE WHEN f.route_key = 0  THEN 0 ELSE 1 END
                 WHEN s.load_found IS NULL THEN CASE WHEN f.route_key = -1 THEN 0 ELSE 1 END
                 WHEN s.route_id IS NULL   THEN CASE WHEN f.route_key = 0  THEN 0 ELSE 1 END
                 WHEN rt.route_id = s.route_id AND s.event_key >= rt.valid_from_date_key
                      AND s.event_key < rt.valid_to_date_key THEN 0
                 WHEN f.route_key = -1 AND rtm.route_id IS NULL THEN 0
                 ELSE 1 END) AS bad_route,
        SUM(CASE WHEN s.location_city IS NULL THEN CASE WHEN f.location_key = 0 THEN 0 ELSE 1 END
                 WHEN lc.location_city = s.location_city AND f.location_key > 0 THEN 0
                 WHEN f.location_key = -1 AND lcm.location_city IS NULL THEN 0
                 ELSE 1 END) AS bad_location,
        SUM(CASE WHEN -1 IN (f.purchase_date_key, f.truck_key, f.driver_key, f.route_key, f.location_key)
                 THEN 1 ELSE 0 END) AS rows_with_unknown
    FROM fact.fact_fuel_purchase AS f
    JOIN s ON s.fuel_purchase_id = f.fuel_purchase_id
    LEFT JOIN dim.dim_truck    AS tk ON tk.truck_key    = f.truck_key
    LEFT JOIN dim.dim_driver   AS dr ON dr.driver_key   = f.driver_key
    LEFT JOIN dim.dim_route    AS rt ON rt.route_key    = f.route_key
    LEFT JOIN dim.dim_location AS lc ON lc.location_key = f.location_key
    LEFT JOIN (SELECT DISTINCT d.truck_id  FROM dim.dim_truck  AS d WHERE d.truck_key  > 0) AS tkm ON tkm.truck_id  = s.truck_id
    LEFT JOIN (SELECT DISTINCT d.driver_id FROM dim.dim_driver AS d WHERE d.driver_key > 0) AS drm ON drm.driver_id = s.driver_id
    LEFT JOIN (SELECT DISTINCT d.route_id  FROM dim.dim_route  AS d WHERE d.route_key  > 0) AS rtm ON rtm.route_id  = s.route_id
    LEFT JOIN (SELECT d.location_city FROM dim.dim_location AS d WHERE d.location_key > 0) AS lcm ON lcm.location_city = s.location_city
)
SELECT 'fact_fuel_purchase' AS fact, r.*
FROM r
WHERE r.bad_date > 0 OR r.bad_truck > 0 OR r.bad_driver > 0 OR r.bad_route > 0 OR r.bad_location > 0;
