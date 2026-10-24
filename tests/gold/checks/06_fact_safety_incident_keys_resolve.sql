/* Check 06: every fact_safety_incident key points at the right dimension row.

   Driver and truck resolve on the version valid at the incident date. The
   incident class key must point at the junk row whose seven attributes equal
   the incident's.

   rows_with_unknown is reported, not asserted.

   Run in: wh_logistics_gold. Read only.
   Pass:   no rows. */

WITH s AS (
    SELECT i.incident_id, i.driver_id, i.truck_id, i.location_city,
           v.incident_type, v.incident_category, v.incident_severity, v.incident_cause,
           v.preventable_status, v.at_fault_status, v.injury_status,
           CASE WHEN i.incident_date IS NULL THEN 0
                ELSE YEAR(i.incident_date) * 10000 + MONTH(i.incident_date) * 100 + DAY(i.incident_date) END AS event_key
    FROM lh_logistics_silver.dbo.silver_safety_incidents AS i
    LEFT JOIN dim.vw_src_incident_class AS v ON v.incident_id = i.incident_id
),
r AS (
    SELECT
        SUM(CASE WHEN f.incident_date_key = s.event_key THEN 0 ELSE 1 END) AS bad_date,
        SUM(CASE WHEN s.driver_id IS NULL THEN CASE WHEN f.driver_key = 0 THEN 0 ELSE 1 END
                 WHEN dr.driver_id = s.driver_id AND s.event_key >= dr.valid_from_date_key
                      AND s.event_key < dr.valid_to_date_key THEN 0
                 WHEN f.driver_key = -1 AND drm.driver_id IS NULL THEN 0
                 ELSE 1 END) AS bad_driver,
        SUM(CASE WHEN s.truck_id IS NULL THEN CASE WHEN f.truck_key = 0 THEN 0 ELSE 1 END
                 WHEN tk.truck_id = s.truck_id AND s.event_key >= tk.valid_from_date_key
                      AND s.event_key < tk.valid_to_date_key THEN 0
                 WHEN f.truck_key = -1 AND tkm.truck_id IS NULL THEN 0
                 ELSE 1 END) AS bad_truck,
        SUM(CASE WHEN s.location_city IS NULL THEN CASE WHEN f.location_key = 0 THEN 0 ELSE 1 END
                 WHEN lc.location_city = s.location_city AND f.location_key > 0 THEN 0
                 WHEN f.location_key = -1 AND lcm.location_city IS NULL THEN 0
                 ELSE 1 END) AS bad_location,
        SUM(CASE WHEN ic.incident_type = s.incident_type AND ic.incident_category = s.incident_category
                  AND ic.incident_severity = s.incident_severity AND ic.incident_cause = s.incident_cause
                  AND ic.preventable_status = s.preventable_status AND ic.at_fault_status = s.at_fault_status
                  AND ic.injury_status = s.injury_status AND f.incident_class_key > 0 THEN 0 ELSE 1 END) AS bad_class,
        SUM(CASE WHEN -1 IN (f.incident_date_key, f.driver_key, f.truck_key, f.location_key, f.incident_class_key)
                 THEN 1 ELSE 0 END) AS rows_with_unknown
    FROM fact.fact_safety_incident AS f
    JOIN s ON s.incident_id = f.incident_id
    LEFT JOIN dim.dim_driver AS dr ON dr.driver_key = f.driver_key
    LEFT JOIN dim.dim_truck AS tk ON tk.truck_key = f.truck_key
    LEFT JOIN dim.dim_location AS lc ON lc.location_key = f.location_key
    LEFT JOIN dim.dim_incident_class AS ic ON ic.incident_class_key = f.incident_class_key
    LEFT JOIN (SELECT DISTINCT d.driver_id FROM dim.dim_driver AS d WHERE d.driver_key > 0) AS drm ON drm.driver_id = s.driver_id
    LEFT JOIN (SELECT DISTINCT d.truck_id  FROM dim.dim_truck  AS d WHERE d.truck_key  > 0) AS tkm ON tkm.truck_id  = s.truck_id
    LEFT JOIN (SELECT d.location_city FROM dim.dim_location AS d WHERE d.location_key > 0) AS lcm ON lcm.location_city = s.location_city
)
SELECT 'fact_safety_incident' AS fact, r.*
FROM r
WHERE r.bad_date > 0 OR r.bad_driver > 0 OR r.bad_truck > 0 OR r.bad_location > 0 OR r.bad_class > 0;
