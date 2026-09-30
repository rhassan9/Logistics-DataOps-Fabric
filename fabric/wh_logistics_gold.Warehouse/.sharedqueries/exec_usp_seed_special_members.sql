EXEC dim.usp_seed_special_members;
EXEC dim.usp_seed_special_members;

SELECT 'customer' AS dim, COUNT(*) AS special_rows FROM dim.dim_customer AS t WHERE t.customer_key <= 0
UNION ALL SELECT 'driver',   COUNT(*) FROM dim.dim_driver   AS t WHERE t.driver_key   <= 0
UNION ALL SELECT 'truck',    COUNT(*) FROM dim.dim_truck    AS t WHERE t.truck_key    <= 0
UNION ALL SELECT 'trailer',  COUNT(*) FROM dim.dim_trailer  AS t WHERE t.trailer_key  <= 0
UNION ALL SELECT 'route',    COUNT(*) FROM dim.dim_route    AS t WHERE t.route_key    <= 0
UNION ALL SELECT 'facility', COUNT(*) FROM dim.dim_facility AS t WHERE t.facility_key <= 0;
-- Expect 3 for each