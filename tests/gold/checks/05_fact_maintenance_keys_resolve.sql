/* Check 05: every fact_maintenance key points at the right dimension row.

   Truck resolves on the version valid at the maintenance date. Location comes
   from the facility city. The maintenance class key must point at the junk row
   whose attributes equal the record's.

   rows_with_unknown is reported, not asserted.

   Run in: wh_logistics_gold. Read only.
   Pass:   no rows. */

WITH s AS (
    SELECT m.maintenance_id, m.truck_id, m.facility_location, v.maintenance_type, v.service_urgency,
           CASE WHEN m.maintenance_date IS NULL THEN 0
                ELSE YEAR(m.maintenance_date) * 10000 + MONTH(m.maintenance_date) * 100 + DAY(m.maintenance_date) END AS event_key
    FROM lh_logistics_silver.dbo.silver_maintenance_records AS m
    LEFT JOIN dim.vw_src_maintenance_class AS v ON v.maintenance_id = m.maintenance_id
),
r AS (
    SELECT
        SUM(CASE WHEN f.maintenance_date_key = s.event_key THEN 0 ELSE 1 END) AS bad_date,
        SUM(CASE WHEN s.truck_id IS NULL THEN CASE WHEN f.truck_key = 0 THEN 0 ELSE 1 END
                 WHEN tk.truck_id = s.truck_id AND s.event_key >= tk.valid_from_date_key
                      AND s.event_key < tk.valid_to_date_key THEN 0
                 WHEN f.truck_key = -1 AND tkm.truck_id IS NULL THEN 0
                 ELSE 1 END) AS bad_truck,
        SUM(CASE WHEN s.facility_location IS NULL THEN CASE WHEN f.location_key = 0 THEN 0 ELSE 1 END
                 WHEN lc.location_city = s.facility_location AND f.location_key > 0 THEN 0
                 WHEN f.location_key = -1 AND lcm.location_city IS NULL THEN 0
                 ELSE 1 END) AS bad_location,
        SUM(CASE WHEN mc.maintenance_type = s.maintenance_type AND mc.service_urgency = s.service_urgency
                  AND f.maintenance_class_key > 0 THEN 0 ELSE 1 END) AS bad_class,
        SUM(CASE WHEN -1 IN (f.maintenance_date_key, f.truck_key, f.location_key, f.maintenance_class_key)
                 THEN 1 ELSE 0 END) AS rows_with_unknown
    FROM fact.fact_maintenance AS f
    JOIN s ON s.maintenance_id = f.maintenance_id
    LEFT JOIN dim.dim_truck AS tk ON tk.truck_key = f.truck_key
    LEFT JOIN dim.dim_location AS lc ON lc.location_key = f.location_key
    LEFT JOIN dim.dim_maintenance_class AS mc ON mc.maintenance_class_key = f.maintenance_class_key
    LEFT JOIN (SELECT DISTINCT d.truck_id FROM dim.dim_truck AS d WHERE d.truck_key > 0) AS tkm ON tkm.truck_id = s.truck_id
    LEFT JOIN (SELECT d.location_city FROM dim.dim_location AS d WHERE d.location_key > 0) AS lcm ON lcm.location_city = s.facility_location
)
SELECT 'fact_maintenance' AS fact, r.*
FROM r
WHERE r.bad_date > 0 OR r.bad_truck > 0 OR r.bad_location > 0 OR r.bad_class > 0;
