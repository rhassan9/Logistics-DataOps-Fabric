-- =====================================================================
-- load it with the same parameters as before.
-- dim_date's keys are deterministic, so facts are unaffected.
-- =====================================================================
EXEC dim.usp_load_dim_date
     @start_date            = '2022-01-01',
     @end_date              = '2025-12-31',
     @complete_through_date = '2024-12-31';


-- =====================================================================
-- verify. Expect 1461 / 1461 / 2022-01-01 / 2025-12-31, then
-- unmatched_keys = 0 on every row of the second result.
-- =====================================================================
SELECT COUNT(*) AS calendar_rows,
       COUNT(DISTINCT dc.full_date) AS distinct_dates,
       MIN(dc.full_date) AS first_date,
       MAX(dc.full_date) AS last_date,
       SUM(CAST(dc.is_complete_month AS INT)) AS days_in_complete_months
FROM dim.dim_calendar AS dc;

-- Every fact date, in every role, must find a calendar row. Date keys are
-- NOT NULL in every fact table, so unmatched is the only failure left.
SELECT x.fact, x.date_role, COUNT(*) AS rows_total,
       SUM(CASE WHEN dc.date_key IS NULL THEN 1 ELSE 0 END) AS unmatched_keys
FROM
(
    SELECT 'fact_delivery_event' AS fact, 'scheduled_date_key' AS date_role, e.scheduled_date_key AS k FROM fact.fact_delivery_event AS e
    UNION ALL SELECT 'fact_delivery_event', 'actual_date_key',     e.actual_date_key     FROM fact.fact_delivery_event AS e
    UNION ALL SELECT 'fact_load_lifecycle', 'booked_date_key',     l.booked_date_key     FROM fact.fact_load_lifecycle AS l
    UNION ALL SELECT 'fact_load_lifecycle', 'dispatched_date_key', l.dispatched_date_key FROM fact.fact_load_lifecycle AS l
    UNION ALL SELECT 'fact_load_lifecycle', 'pickup_date_key',     l.pickup_date_key     FROM fact.fact_load_lifecycle AS l
    UNION ALL SELECT 'fact_load_lifecycle', 'delivery_date_key',   l.delivery_date_key   FROM fact.fact_load_lifecycle AS l
    UNION ALL SELECT 'fact_trip',           'dispatch_date_key',   t.dispatch_date_key   FROM fact.fact_trip AS t
    UNION ALL SELECT 'fact_fuel_purchase',  'purchase_date_key',   f.purchase_date_key   FROM fact.fact_fuel_purchase AS f
    UNION ALL SELECT 'fact_maintenance',    'maintenance_date_key', m.maintenance_date_key FROM fact.fact_maintenance AS m
    UNION ALL SELECT 'fact_safety_incident','incident_date_key',   s.incident_date_key   FROM fact.fact_safety_incident AS s
) AS x
LEFT JOIN dim.dim_calendar AS dc ON dc.date_key = x.k
GROUP BY x.fact, x.date_role
ORDER BY x.fact, x.date_role;

SELECT COUNT(*)                                          AS total_rows,        -- 1464
       SUM(CASE WHEN d.date_key > 0 THEN 1 ELSE 0 END)   AS date_rows,         -- 1461
       MIN(d.full_date)                                  AS first_date,        -- 2022-01-01
       MAX(d.full_date)                                  AS last_date,         -- 2025-12-31
       COUNT(DISTINCT CASE WHEN d.is_complete_month = 1
                           THEN d.year_month_number END) AS complete_months    -- 36
FROM dim.dim_date AS d;

SELECT d.date_key, d.year_month_label, d.quarter_label, d.is_complete_month
FROM dim.dim_date AS d
WHERE d.date_key IN (-2, -1, 0, 20240229, 20241231, 20250101, 20250103)
ORDER BY d.date_key;