EXEC dim.usp_load_dim_customer @allow_mass_delete = 0;
GO
EXEC dim.usp_load_dim_trailer  @allow_mass_delete = 0;
GO
EXEC dim.usp_load_dim_route    @allow_mass_delete = 0;
GO
EXEC dim.usp_load_dim_facility @allow_mass_delete = 0;
GO
EXEC dim.usp_load_dim_driver   @allow_mass_delete = 0;
GO
EXEC dim.usp_load_dim_truck    @allow_mass_delete = 0;
GO

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

SELECT 'dim_customer' AS dim, SUM(CASE WHEN customer_key > 0 THEN 1 ELSE 0 END) AS members,
       SUM(CASE WHEN customer_key <= 0 THEN 1 ELSE 0 END) AS special, COUNT(*) AS versions,
       SUM(CAST(is_current AS INT)) AS current_rows, SUM(CAST(is_deleted AS INT)) AS deleted,
       MAX(customer_key) AS max_key
FROM dim.dim_customer
UNION ALL
SELECT 'dim_trailer', SUM(CASE WHEN trailer_key > 0 THEN 1 ELSE 0 END), SUM(CASE WHEN trailer_key <= 0 THEN 1 ELSE 0 END),
       COUNT(*), SUM(CAST(is_current AS INT)), SUM(CAST(is_deleted AS INT)), MAX(trailer_key)
FROM dim.dim_trailer
UNION ALL
SELECT 'dim_route', SUM(CASE WHEN route_key > 0 THEN 1 ELSE 0 END), SUM(CASE WHEN route_key <= 0 THEN 1 ELSE 0 END),
       COUNT(*), SUM(CAST(is_current AS INT)), SUM(CAST(is_deleted AS INT)), MAX(route_key)
FROM dim.dim_route
UNION ALL
SELECT 'dim_facility', SUM(CASE WHEN facility_key > 0 THEN 1 ELSE 0 END), SUM(CASE WHEN facility_key <= 0 THEN 1 ELSE 0 END),
       COUNT(*), SUM(CAST(is_current AS INT)), SUM(CAST(is_deleted AS INT)), MAX(facility_key)
FROM dim.dim_facility
UNION ALL
SELECT 'dim_driver', SUM(CASE WHEN driver_key > 0 THEN 1 ELSE 0 END), SUM(CASE WHEN driver_key <= 0 THEN 1 ELSE 0 END),
       COUNT(*), SUM(CAST(is_current AS INT)), SUM(CAST(is_deleted AS INT)), MAX(driver_key)
FROM dim.dim_driver
UNION ALL
SELECT 'dim_truck', SUM(CASE WHEN truck_key > 0 THEN 1 ELSE 0 END), SUM(CASE WHEN truck_key <= 0 THEN 1 ELSE 0 END),
       COUNT(*), SUM(CAST(is_current AS INT)), SUM(CAST(is_deleted AS INT)), MAX(truck_key)
FROM dim.dim_truck;
 
-- Idempotency: run the six load EXECs again, then rerun the query above.
-- Every figure, max_key included, must be identical.
 