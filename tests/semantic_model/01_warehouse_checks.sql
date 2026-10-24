/* Semantic model prerequisites, checked in the warehouse.

   V0: every dimension key is unique, the one side of every relationship in
       sm_logistics_gold. A duplicate key would make a relationship invalid.
   V6: the source's price per gallon equals total_cost / gallons, which is why
       the model derives price from cost and gallons and carries no price column.

   Run in: wh_logistics_gold. Read only.
   Pass:   no rows. */

WITH v0 AS (
              SELECT CAST('Date' AS VARCHAR(32)) AS model_table, COUNT(*) - COUNT(DISTINCT d.date_key) AS duplicate_keys FROM dim.dim_calendar AS d
    UNION ALL SELECT 'Customer',          COUNT(*) - COUNT(DISTINCT x.customer_key)          FROM dim.dim_customer AS x
    UNION ALL SELECT 'Driver',            COUNT(*) - COUNT(DISTINCT x.driver_key)            FROM dim.dim_driver AS x
    UNION ALL SELECT 'Truck',             COUNT(*) - COUNT(DISTINCT x.truck_key)             FROM dim.dim_truck AS x
    UNION ALL SELECT 'Trailer',           COUNT(*) - COUNT(DISTINCT x.trailer_key)           FROM dim.dim_trailer AS x
    UNION ALL SELECT 'Route',             COUNT(*) - COUNT(DISTINCT x.route_key)             FROM dim.dim_route AS x
    UNION ALL SELECT 'Facility',          COUNT(*) - COUNT(DISTINCT x.facility_key)          FROM dim.dim_facility AS x
    UNION ALL SELECT 'Location',          COUNT(*) - COUNT(DISTINCT x.location_key)          FROM dim.dim_location AS x
    UNION ALL SELECT 'Load Type',         COUNT(*) - COUNT(DISTINCT x.load_type_key)         FROM dim.dim_load_type AS x
    UNION ALL SELECT 'Stop Status',       COUNT(*) - COUNT(DISTINCT x.delivery_status_key)   FROM dim.dim_delivery_status AS x
    UNION ALL SELECT 'Maintenance Class', COUNT(*) - COUNT(DISTINCT x.maintenance_class_key) FROM dim.dim_maintenance_class AS x
    UNION ALL SELECT 'Incident Class',    COUNT(*) - COUNT(DISTINCT x.incident_class_key)    FROM dim.dim_incident_class AS x
),
v6 AS (
    SELECT SUM(CASE WHEN ABS(p.total_cost / p.gallons - p.price_per_gallon) >= 0.01 THEN 1 ELSE 0 END) AS rows_differing,
           MAX(ABS(p.total_cost / p.gallons - p.price_per_gallon)) AS max_abs_difference
    FROM lh_logistics_silver.dbo.silver_fuel_purchases AS p
    WHERE p._is_deleted_in_source = 0 AND p.gallons > 0
)
SELECT 'V0 duplicate keys' AS problem, v0.model_table AS detail, CAST(v0.duplicate_keys AS DECIMAL(18, 4)) AS n
FROM v0
WHERE v0.duplicate_keys <> 0

UNION ALL
SELECT 'V6 source price differs from cost / gallons by a cent or more',
       'max difference ' + CAST(CAST(v6.max_abs_difference AS DECIMAL(18, 4)) AS VARCHAR(30)),
       CAST(v6.rows_differing AS DECIMAL(18, 4))
FROM v6
WHERE v6.rows_differing > 0;
