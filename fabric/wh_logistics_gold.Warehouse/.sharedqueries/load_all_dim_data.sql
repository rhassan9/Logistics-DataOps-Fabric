EXEC dim.usp_load_dim_customer;  EXEC dim.usp_load_dim_trailer;
EXEC dim.usp_load_dim_route;     EXEC dim.usp_load_dim_facility;
EXEC dim.usp_load_dim_driver @effective_date = '2026-09-30';
EXEC dim.usp_load_dim_truck  @effective_date = '2026-09-30';

SELECT 'customer' AS dim, COUNT(*) AS total_rows, SUM(CASE WHEN t.is_current = 1 AND t.customer_key > 0 THEN 1 ELSE 0 END) AS current_members FROM dim.dim_customer AS t
UNION ALL SELECT 'trailer',  COUNT(*), SUM(CASE WHEN t.is_current = 1 AND t.trailer_key  > 0 THEN 1 ELSE 0 END) FROM dim.dim_trailer  AS t
UNION ALL SELECT 'route',    COUNT(*), SUM(CASE WHEN t.is_current = 1 AND t.route_key    > 0 THEN 1 ELSE 0 END) FROM dim.dim_route    AS t
UNION ALL SELECT 'facility', COUNT(*), SUM(CASE WHEN t.is_current = 1 AND t.facility_key > 0 THEN 1 ELSE 0 END) FROM dim.dim_facility AS t
UNION ALL SELECT 'driver',   COUNT(*), SUM(CASE WHEN t.is_current = 1 AND t.driver_key   > 0 THEN 1 ELSE 0 END) FROM dim.dim_driver   AS t
UNION ALL SELECT 'truck',    COUNT(*), SUM(CASE WHEN t.is_current = 1 AND t.truck_key    > 0 THEN 1 ELSE 0 END) FROM dim.dim_truck    AS t;
-- Expect total = members + 3: customer 203/200, trailer 183/180, route 61/58, facility 53/50, driver 153/150, truck 123/120

SELECT COUNT(*) AS unresolved_states
FROM dim.dim_route AS r
WHERE r.route_key > 0 AND (r.origin_state_name = 'Unknown' OR r.destination_state_name = 'Unknown');
-- Expect 0; repeat on dim_facility.facility_state_name