/* Check 02: every fact_trip key points at the right dimension row.

   Independent of fact.usp_load_fact_trip. Each key must point at the member
   silver names, on the version valid at the dispatch date, or at the right
   special member: Missing (0) for a null reference, Unknown (-1) only when no
   member exists. Route resolves through the trip's load.

   rows_with_unknown is reported, not asserted: a legitimate Unknown already
   passes the bad_* rules.

   Run in: wh_logistics_gold. Read only.
   Pass:   no rows. A returned row shows how many rows fail each key. */

WITH s AS (
    SELECT t.trip_id, t.load_id, t.driver_id, t.truck_id, t.trailer_id, t.dispatch_date,
           l.load_id AS load_found, l.route_id
    FROM lh_logistics_silver.dbo.silver_trips AS t
    LEFT JOIN lh_logistics_silver.dbo.silver_loads AS l ON l.load_id = t.load_id
),
r AS (
    SELECT
        SUM(CASE WHEN f.dispatch_date_key = CASE WHEN s.dispatch_date IS NULL THEN 0
                     ELSE YEAR(s.dispatch_date) * 10000 + MONTH(s.dispatch_date) * 100 + DAY(s.dispatch_date) END
                 THEN 0 ELSE 1 END) AS bad_date,
        SUM(CASE WHEN s.driver_id IS NULL THEN CASE WHEN f.driver_key = 0 THEN 0 ELSE 1 END
                 WHEN dr.driver_id = s.driver_id AND f.dispatch_date_key >= dr.valid_from_date_key
                      AND f.dispatch_date_key < dr.valid_to_date_key THEN 0
                 WHEN f.driver_key = -1 AND drm.driver_id IS NULL THEN 0
                 ELSE 1 END) AS bad_driver,
        SUM(CASE WHEN s.truck_id IS NULL THEN CASE WHEN f.truck_key = 0 THEN 0 ELSE 1 END
                 WHEN tk.truck_id = s.truck_id AND f.dispatch_date_key >= tk.valid_from_date_key
                      AND f.dispatch_date_key < tk.valid_to_date_key THEN 0
                 WHEN f.truck_key = -1 AND tkm.truck_id IS NULL THEN 0
                 ELSE 1 END) AS bad_truck,
        SUM(CASE WHEN s.trailer_id IS NULL THEN CASE WHEN f.trailer_key = 0 THEN 0 ELSE 1 END
                 WHEN tl.trailer_id = s.trailer_id AND f.dispatch_date_key >= tl.valid_from_date_key
                      AND f.dispatch_date_key < tl.valid_to_date_key THEN 0
                 WHEN f.trailer_key = -1 AND tlm.trailer_id IS NULL THEN 0
                 ELSE 1 END) AS bad_trailer,
        SUM(CASE WHEN s.load_id IS NULL    THEN CASE WHEN f.route_key = 0  THEN 0 ELSE 1 END
                 WHEN s.load_found IS NULL THEN CASE WHEN f.route_key = -1 THEN 0 ELSE 1 END
                 WHEN s.route_id IS NULL   THEN CASE WHEN f.route_key = 0  THEN 0 ELSE 1 END
                 WHEN rt.route_id = s.route_id AND f.dispatch_date_key >= rt.valid_from_date_key
                      AND f.dispatch_date_key < rt.valid_to_date_key THEN 0
                 WHEN f.route_key = -1 AND rtm.route_id IS NULL THEN 0
                 ELSE 1 END) AS bad_route,
        SUM(CASE WHEN -1 IN (f.dispatch_date_key, f.driver_key, f.truck_key, f.trailer_key, f.route_key)
                 THEN 1 ELSE 0 END) AS rows_with_unknown
    FROM fact.fact_trip AS f
    JOIN s ON s.trip_id = f.trip_id
    LEFT JOIN dim.dim_driver  AS dr ON dr.driver_key  = f.driver_key
    LEFT JOIN dim.dim_truck   AS tk ON tk.truck_key   = f.truck_key
    LEFT JOIN dim.dim_trailer AS tl ON tl.trailer_key = f.trailer_key
    LEFT JOIN dim.dim_route   AS rt ON rt.route_key   = f.route_key
    LEFT JOIN (SELECT DISTINCT d.driver_id  FROM dim.dim_driver  AS d WHERE d.driver_key  > 0) AS drm ON drm.driver_id  = s.driver_id
    LEFT JOIN (SELECT DISTINCT d.truck_id   FROM dim.dim_truck   AS d WHERE d.truck_key   > 0) AS tkm ON tkm.truck_id   = s.truck_id
    LEFT JOIN (SELECT DISTINCT d.trailer_id FROM dim.dim_trailer AS d WHERE d.trailer_key > 0) AS tlm ON tlm.trailer_id = s.trailer_id
    LEFT JOIN (SELECT DISTINCT d.route_id   FROM dim.dim_route   AS d WHERE d.route_key   > 0) AS rtm ON rtm.route_id   = s.route_id
)
SELECT 'fact_trip' AS fact, r.*
FROM r
WHERE r.bad_date > 0 OR r.bad_driver > 0 OR r.bad_truck > 0 OR r.bad_trailer > 0 OR r.bad_route > 0;
