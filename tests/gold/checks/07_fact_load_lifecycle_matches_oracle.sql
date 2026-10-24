/* Check 07: fact_load_lifecycle equals an independent rebuild from silver.

   The oracle rebuilds every row straight from silver without the procedure's
   fulfilling-trip ranking, and translates gold's surrogate keys back to
   business ids, so key resolution is tested as well as every measure, lag and
   flag. The rebuild is valid only while every load has at most one live trip;
   the first branch fails the check if that stops being true.

   Run in: wh_logistics_gold. Read only.
   Pass:   no rows. Each returned row names a problem and its size. */

WITH expected AS (
    SELECT
        l.load_id,
        CASE WHEN l.load_date IS NULL THEN 0
             ELSE YEAR(l.load_date) * 10000 + MONTH(l.load_date) * 100 + DAY(l.load_date) END AS booked_date_key,
        CASE WHEN t.trip_id IS NULL THEN -2 WHEN t.dispatch_date IS NULL THEN 0
             ELSE YEAR(t.dispatch_date) * 10000 + MONTH(t.dispatch_date) * 100 + DAY(t.dispatch_date) END AS dispatched_date_key,
        CASE WHEN p.actual_datetime IS NULL THEN -2
             ELSE YEAR(p.actual_datetime) * 10000 + MONTH(p.actual_datetime) * 100 + DAY(p.actual_datetime) END AS pickup_date_key,
        CASE WHEN d.actual_datetime IS NULL THEN -2
             ELSE YEAR(d.actual_datetime) * 10000 + MONTH(d.actual_datetime) * 100 + DAY(d.actual_datetime) END AS delivery_date_key,
        l.customer_id,
        l.route_id,
        COALESCE(l.booking_type, 'Not Recorded') AS booking_type,
        COALESCE(l.load_type, 'Not Recorded') AS load_type,
        t.driver_id,
        t.truck_id,
        t.trailer_id,
        l.revenue, l.fuel_surcharge, l.accessorial_charges, l.weight_lbs, l.pieces,
        DATEDIFF(day, l.load_date, t.dispatch_date) AS booking_to_dispatch_days,
        CAST(ROUND(DATEDIFF_BIG(microsecond, CAST(t.dispatch_date AS DATETIME2(6)), p.actual_datetime)
                   / 3600000000.0, 2) AS DECIMAL(10,2)) AS dispatch_to_pickup_hours,
        CASE WHEN d.actual_datetime < p.actual_datetime THEN NULL
             ELSE CAST(ROUND(DATEDIFF_BIG(microsecond, p.actual_datetime, d.actual_datetime)
                             / 3600000000.0, 2) AS DECIMAL(10,2)) END AS pickup_to_delivery_hours,
        CAST(ROUND(DATEDIFF_BIG(microsecond, CAST(l.load_date AS DATETIME2(6)), d.actual_datetime)
                   / 3600000000.0, 2) AS DECIMAL(10,2)) AS booking_to_delivery_hours,
        CAST(CASE WHEN d.actual_datetime < p.actual_datetime THEN 1 ELSE 0 END AS BIT) AS is_timestamp_reversed,
        CAST(CASE WHEN l._is_deleted_in_source = 1 THEN 1 ELSE 0 END AS BIT) AS is_deleted_in_source
    FROM lh_logistics_silver.dbo.silver_loads AS l
    LEFT JOIN lh_logistics_silver.dbo.silver_trips AS t
      ON t.load_id = l.load_id AND t._is_deleted_in_source = 0
    LEFT JOIN lh_logistics_silver.dbo.silver_delivery_events AS p
      ON p.trip_id = t.trip_id AND p.load_id = l.load_id
     AND p.event_type = 'Pickup' AND p._is_deleted_in_source = 0
    LEFT JOIN lh_logistics_silver.dbo.silver_delivery_events AS d
      ON d.trip_id = t.trip_id AND d.load_id = l.load_id
     AND d.event_type = 'Delivery' AND d._is_deleted_in_source = 0
),
actual AS (
    SELECT
        f.load_id, f.booked_date_key, f.dispatched_date_key, f.pickup_date_key, f.delivery_date_key,
        c.customer_id, r.route_id, lt.booking_type, lt.load_type,
        dr.driver_id, tk.truck_id, tl.trailer_id,
        f.revenue, f.fuel_surcharge, f.accessorial_charges, f.weight_lbs, f.pieces,
        f.booking_to_dispatch_days, f.dispatch_to_pickup_hours, f.pickup_to_delivery_hours,
        f.booking_to_delivery_hours, f.is_timestamp_reversed, f.is_deleted_in_source
    FROM fact.fact_load_lifecycle AS f
    LEFT JOIN dim.dim_customer  AS c  ON c.customer_key   = f.customer_key           AND f.customer_key > 0
    LEFT JOIN dim.dim_route     AS r  ON r.route_key      = f.route_key              AND f.route_key > 0
    LEFT JOIN dim.dim_load_type AS lt ON lt.load_type_key = f.load_type_key          AND f.load_type_key > 0
    LEFT JOIN dim.dim_driver    AS dr ON dr.driver_key    = f.fulfilling_driver_key  AND f.fulfilling_driver_key > 0
    LEFT JOIN dim.dim_truck     AS tk ON tk.truck_key     = f.fulfilling_truck_key   AND f.fulfilling_truck_key > 0
    LEFT JOIN dim.dim_trailer   AS tl ON tl.trailer_key   = f.fulfilling_trailer_key AND f.fulfilling_trailer_key > 0
)
SELECT 'loads with more than one live trip (oracle assumption broken)' AS problem, COUNT(*) AS n
FROM (SELECT t.load_id
      FROM lh_logistics_silver.dbo.silver_trips AS t
      WHERE t._is_deleted_in_source = 0
      GROUP BY t.load_id
      HAVING COUNT(*) > 1) AS m
HAVING COUNT(*) > 0

UNION ALL
SELECT 'gold rows minus silver rows', (SELECT COUNT(*) FROM fact.fact_load_lifecycle)
                                    - (SELECT COUNT(*) FROM lh_logistics_silver.dbo.silver_loads)
WHERE (SELECT COUNT(*) FROM fact.fact_load_lifecycle)
   <> (SELECT COUNT(*) FROM lh_logistics_silver.dbo.silver_loads)

UNION ALL
SELECT 'duplicate load_id rows in gold', (SELECT COUNT(*) FROM fact.fact_load_lifecycle)
                                       - (SELECT COUNT(DISTINCT f.load_id) FROM fact.fact_load_lifecycle AS f)
WHERE (SELECT COUNT(*) FROM fact.fact_load_lifecycle)
   <> (SELECT COUNT(DISTINCT f.load_id) FROM fact.fact_load_lifecycle AS f)

UNION ALL
SELECT 'expected rows not in gold', COUNT(*)
FROM (SELECT * FROM expected EXCEPT SELECT * FROM actual) AS x
HAVING COUNT(*) > 0

UNION ALL
SELECT 'gold rows not expected', COUNT(*)
FROM (SELECT * FROM actual EXCEPT SELECT * FROM expected) AS y
HAVING COUNT(*) > 0;
