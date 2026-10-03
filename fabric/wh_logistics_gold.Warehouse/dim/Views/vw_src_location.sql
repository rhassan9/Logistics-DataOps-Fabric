CREATE   VIEW dim.vw_src_location
AS
SELECT x.location_city,
       m.location_state_code,
       COALESCE(us.state_name, 'Unknown') AS location_state_name,
       m.state_source
FROM
(
    SELECT fp.location_city AS location_city FROM lh_logistics_silver.dbo.silver_fuel_purchases AS fp
    UNION SELECT si.location_city FROM lh_logistics_silver.dbo.silver_safety_incidents AS si
    UNION SELECT mr.facility_location FROM lh_logistics_silver.dbo.silver_maintenance_records AS mr
) AS x
LEFT JOIN dim.vw_city_state_map AS m ON m.location_city = x.location_city
LEFT JOIN ref.us_state AS us ON us.state_code = m.location_state_code
WHERE x.location_city IS NOT NULL;

GO