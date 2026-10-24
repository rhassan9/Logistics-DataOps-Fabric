/* Semantic model figures, computed independently in the warehouse.

   Each query computes, in T-SQL against the gold tables, what a measure group in
   sm_logistics_gold returns. Run it next to the DAX query of the same name in
   03_model_figures.dax and compare every value: rates to four decimals, money to
   the cent. These are comparisons, not assertions: both sides return rows.

   Run in: wh_logistics_gold. Read only. */

-- V1. Grand totals. Compare with query V1 in 03_model_figures.dax.
SELECT
    (SELECT COUNT(*)
     FROM fact.fact_delivery_event AS e
     JOIN dim.dim_delivery_status AS ds ON ds.delivery_status_key = e.delivery_status_key
     WHERE e.is_deleted_in_source = 0 AND ds.event_type = 'Delivery')                 AS delivery_events,
    (SELECT CAST(AVG(CASE WHEN ds.arrival_status = 'On Time' THEN 1.0 ELSE 0.0 END) AS DECIMAL(9, 6))
     FROM fact.fact_delivery_event AS e
     JOIN dim.dim_delivery_status AS ds ON ds.delivery_status_key = e.delivery_status_key
     WHERE e.is_deleted_in_source = 0 AND ds.event_type = 'Delivery')                 AS on_time_rate,
    (SELECT CAST(AVG(CAST(e.arrival_variance_minutes AS DECIMAL(18, 6))) AS DECIMAL(18, 4))
     FROM fact.fact_delivery_event AS e
     JOIN dim.dim_delivery_status AS ds ON ds.delivery_status_key = e.delivery_status_key
     WHERE e.is_deleted_in_source = 0 AND ds.event_type = 'Delivery')                 AS avg_arrival_variance,
    (SELECT SUM(l.revenue + l.fuel_surcharge + l.accessorial_charges)
     FROM fact.fact_load_lifecycle AS l WHERE l.is_deleted_in_source = 0)             AS total_charges,
    (SELECT SUM(f.total_cost)
     FROM fact.fact_fuel_purchase AS f WHERE f.is_deleted_in_source = 0)              AS fuel_cost,
    (SELECT SUM(CAST(t.actual_distance_miles AS BIGINT))
     FROM fact.fact_trip AS t WHERE t.is_deleted_in_source = 0)                       AS actual_miles;


-- V2. By lane, ordered like query V2 in 03_model_figures.dax. Every lane must match.
WITH d AS (
    SELECT e.route_key,
           COUNT(*) AS delivery_events,
           AVG(CASE WHEN ds.arrival_status = 'On Time' THEN 1.0 ELSE 0.0 END) AS on_time_rate,
           AVG(CAST(e.arrival_variance_minutes AS DECIMAL(18, 6))) AS avg_arrival_variance
    FROM fact.fact_delivery_event AS e
    JOIN dim.dim_delivery_status AS ds ON ds.delivery_status_key = e.delivery_status_key
    WHERE e.is_deleted_in_source = 0 AND ds.event_type = 'Delivery'
    GROUP BY e.route_key
), l AS (
    SELECT l.route_key,
           SUM(l.revenue) AS revenue,
           SUM(l.revenue + l.fuel_surcharge + l.accessorial_charges) AS total_charges
    FROM fact.fact_load_lifecycle AS l
    WHERE l.is_deleted_in_source = 0
    GROUP BY l.route_key
), f AS (
    SELECT f.route_key, SUM(f.total_cost) AS fuel_cost
    FROM fact.fact_fuel_purchase AS f
    WHERE f.is_deleted_in_source = 0
    GROUP BY f.route_key
), t AS (
    SELECT t.route_key, SUM(CAST(t.actual_distance_miles AS BIGINT)) AS actual_miles
    FROM fact.fact_trip AS t
    WHERE t.is_deleted_in_source = 0
    GROUP BY t.route_key
)
SELECT r.route_name,
       d.delivery_events,
       CAST(d.on_time_rate AS DECIMAL(9, 6))          AS on_time_rate,
       CAST(d.avg_arrival_variance AS DECIMAL(18, 4)) AS avg_arrival_variance,
       l.total_charges,
       f.fuel_cost,
       l.total_charges - f.fuel_cost                  AS contribution_after_fuel,
       CAST(l.revenue / NULLIF(t.actual_miles, 0) AS DECIMAL(18, 4)) AS revenue_per_mile
FROM dim.dim_route AS r
LEFT JOIN d ON d.route_key = r.route_key
LEFT JOIN l ON l.route_key = r.route_key
LEFT JOIN f ON f.route_key = r.route_key
LEFT JOIN t ON t.route_key = r.route_key
WHERE r.route_key > 0
ORDER BY contribution_after_fuel DESC;


-- V3. The other seven requirements' base measures. Compare with query V3.
SELECT
    (SELECT COUNT(*) FROM fact.fact_trip AS t WHERE t.is_deleted_in_source = 0)                        AS trips,
    (SELECT SUM(t.fuel_gallons_used) FROM fact.fact_trip AS t WHERE t.is_deleted_in_source = 0)        AS trip_fuel_gallons,
    (SELECT SUM(f.gallons) FROM fact.fact_fuel_purchase AS f WHERE f.is_deleted_in_source = 0)         AS gallons_purchased,
    (SELECT CAST(SUM(f.total_cost) / NULLIF(SUM(f.gallons), 0) AS DECIMAL(18, 4))
     FROM fact.fact_fuel_purchase AS f WHERE f.is_deleted_in_source = 0)                               AS average_price_per_gallon,
    (SELECT CAST(MIN(f.total_cost / f.gallons) AS DECIMAL(18, 4))
     FROM fact.fact_fuel_purchase AS f WHERE f.is_deleted_in_source = 0 AND f.gallons > 0)             AS lowest_price_paid,
    (SELECT CAST(MAX(f.total_cost / f.gallons) AS DECIMAL(18, 4))
     FROM fact.fact_fuel_purchase AS f WHERE f.is_deleted_in_source = 0 AND f.gallons > 0)             AS highest_price_paid,
    (SELECT COUNT(*) FROM fact.fact_delivery_event AS e WHERE e.is_deleted_in_source = 0)              AS stop_events,
    (SELECT SUM(CAST(e.billable_detention_minutes AS BIGINT))
     FROM fact.fact_delivery_event AS e WHERE e.is_deleted_in_source = 0)                              AS billable_detention_minutes,
    (SELECT COUNT(*) FROM fact.fact_delivery_event AS e
     WHERE e.is_deleted_in_source = 0 AND e.billable_detention_minutes > 0)                            AS events_over_free_time,
    (SELECT COUNT(*) FROM fact.fact_maintenance AS m WHERE m.is_deleted_in_source = 0)                 AS maintenance_events,
    (SELECT SUM(m.labor_cost) + SUM(m.parts_cost)
     FROM fact.fact_maintenance AS m WHERE m.is_deleted_in_source = 0)                                 AS maintenance_cost,
    (SELECT SUM(m.downtime_hours) FROM fact.fact_maintenance AS m WHERE m.is_deleted_in_source = 0)    AS downtime_hours,
    (SELECT COUNT(*) FROM fact.fact_safety_incident AS s WHERE s.is_deleted_in_source = 0)             AS incidents,
    (SELECT SUM(s.vehicle_damage_cost) + SUM(s.cargo_damage_cost)
     FROM fact.fact_safety_incident AS s WHERE s.is_deleted_in_source = 0)                             AS claim_cost,
    (SELECT COUNT(DISTINCT tk.truck_id)
     FROM fact.fact_trip AS t JOIN dim.dim_truck AS tk ON tk.truck_key = t.truck_key
     WHERE t.is_deleted_in_source = 0 AND tk.truck_key > 0)                                            AS active_trucks,
    (SELECT COUNT(DISTINCT dr.driver_id)
     FROM fact.fact_trip AS t JOIN dim.dim_driver AS dr ON dr.driver_key = t.driver_key
     WHERE t.is_deleted_in_source = 0 AND dr.driver_key > 0)                                           AS active_drivers,
    (SELECT CAST(AVG(CAST(c.credit_terms_days AS DECIMAL(18, 4))) AS DECIMAL(18, 4))
     FROM dim.dim_customer AS c WHERE c.is_current = 1 AND c.customer_key > 0)                         AS average_credit_terms_days,
    (SELECT CAST(AVG(CASE WHEN e.detention_minutes > 0 THEN 1.0 ELSE 0.0 END) AS DECIMAL(9, 6))
     FROM fact.fact_delivery_event AS e
     JOIN dim.dim_delivery_status AS ds ON ds.delivery_status_key = e.delivery_status_key
     WHERE e.is_deleted_in_source = 0 AND ds.event_type = 'Delivery')                                  AS delivery_share_with_detention;


-- V4. Delivery events by scheduled month and by actual month. Compare with query V4:
-- the totals are equal, and the deliveries whose actual date falls in a different
-- month from their scheduled date move between months.
SELECT m.year_month_number,
       SUM(m.by_scheduled) AS deliveries_by_scheduled_date,
       SUM(m.by_actual)    AS deliveries_by_actual_date
FROM
(
    SELECT e.scheduled_date_key / 100 AS year_month_number, 1 AS by_scheduled, 0 AS by_actual
    FROM fact.fact_delivery_event AS e
    JOIN dim.dim_delivery_status AS ds ON ds.delivery_status_key = e.delivery_status_key
    WHERE e.is_deleted_in_source = 0 AND ds.event_type = 'Delivery'
    UNION ALL
    SELECT e.actual_date_key / 100, 0, 1
    FROM fact.fact_delivery_event AS e
    JOIN dim.dim_delivery_status AS ds ON ds.delivery_status_key = e.delivery_status_key
    WHERE e.is_deleted_in_source = 0 AND ds.event_type = 'Delivery'
) AS m
GROUP BY m.year_month_number
ORDER BY m.year_month_number;


-- V5. Active trucks and drivers under filters: by dispatch year, then by lane. Compare with queries V5a and V5b.
SELECT t.dispatch_date_key / 10000 AS calendar_year,
       COUNT(*) AS trips,
       COUNT(DISTINCT CASE WHEN tk.truck_key > 0 THEN tk.truck_id END)  AS active_trucks,
       COUNT(DISTINCT CASE WHEN dr.driver_key > 0 THEN dr.driver_id END) AS active_drivers
FROM fact.fact_trip AS t
JOIN dim.dim_truck  AS tk ON tk.truck_key  = t.truck_key
JOIN dim.dim_driver AS dr ON dr.driver_key = t.driver_key
WHERE t.is_deleted_in_source = 0
GROUP BY t.dispatch_date_key / 10000
ORDER BY calendar_year;

SELECT r.route_name,
       COUNT(DISTINCT CASE WHEN tk.truck_key > 0 THEN tk.truck_id END)  AS active_trucks,
       COUNT(DISTINCT CASE WHEN dr.driver_key > 0 THEN dr.driver_id END) AS active_drivers
FROM fact.fact_trip AS t
JOIN dim.dim_route  AS r  ON r.route_key   = t.route_key
JOIN dim.dim_truck  AS tk ON tk.truck_key  = t.truck_key
JOIN dim.dim_driver AS dr ON dr.driver_key = t.driver_key
WHERE t.is_deleted_in_source = 0
GROUP BY r.route_name
ORDER BY r.route_name;
