/* Check 10: every dimension carries its three special members, and every
   state resolves.

   Each dimension holds exactly one Missing (0), Unknown (-1) and Not
   Applicable (-2) row; facts depend on them to resolve null and broken
   references. Route and facility states must resolve through ref.us_state.

   Run in: wh_logistics_gold. Read only.
   Pass:   no rows. */

WITH special AS (
              SELECT CAST('dim_date' AS VARCHAR(32)) AS dim, d.date_key AS k FROM dim.dim_date AS d WHERE d.date_key <= 0
    UNION ALL SELECT 'dim_customer',          x.customer_key          FROM dim.dim_customer          AS x WHERE x.customer_key <= 0
    UNION ALL SELECT 'dim_driver',            x.driver_key            FROM dim.dim_driver            AS x WHERE x.driver_key <= 0
    UNION ALL SELECT 'dim_truck',             x.truck_key             FROM dim.dim_truck             AS x WHERE x.truck_key <= 0
    UNION ALL SELECT 'dim_trailer',           x.trailer_key           FROM dim.dim_trailer           AS x WHERE x.trailer_key <= 0
    UNION ALL SELECT 'dim_route',             x.route_key             FROM dim.dim_route             AS x WHERE x.route_key <= 0
    UNION ALL SELECT 'dim_facility',          x.facility_key          FROM dim.dim_facility          AS x WHERE x.facility_key <= 0
    UNION ALL SELECT 'dim_location',          x.location_key          FROM dim.dim_location          AS x WHERE x.location_key <= 0
    UNION ALL SELECT 'dim_load_type',         x.load_type_key         FROM dim.dim_load_type         AS x WHERE x.load_type_key <= 0
    UNION ALL SELECT 'dim_delivery_status',   x.delivery_status_key   FROM dim.dim_delivery_status   AS x WHERE x.delivery_status_key <= 0
    UNION ALL SELECT 'dim_maintenance_class', x.maintenance_class_key FROM dim.dim_maintenance_class AS x WHERE x.maintenance_class_key <= 0
    UNION ALL SELECT 'dim_incident_class',    x.incident_class_key    FROM dim.dim_incident_class    AS x WHERE x.incident_class_key <= 0
)
SELECT s.dim AS problem_dimension,
       'special rows are not exactly 0, -1 and -2 once each' AS problem,
       COUNT(*) AS special_rows
FROM special AS s
GROUP BY s.dim
HAVING NOT (COUNT(*) = 3 AND MIN(s.k) = -2 AND MAX(s.k) = 0 AND COUNT(DISTINCT s.k) = 3)

UNION ALL
SELECT 'dim_route', 'state did not resolve', COUNT(*)
FROM dim.dim_route AS r
WHERE r.route_key > 0 AND (r.origin_state_name = 'Unknown' OR r.destination_state_name = 'Unknown')
HAVING COUNT(*) > 0

UNION ALL
SELECT 'dim_facility', 'state did not resolve', COUNT(*)
FROM dim.dim_facility AS f
WHERE f.facility_key > 0 AND f.facility_state_name = 'Unknown'
HAVING COUNT(*) > 0;
