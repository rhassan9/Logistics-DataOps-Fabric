CREATE   VIEW dim.vw_src_route
AS
SELECT s.route_id,
       COALESCE(s.route_name,       'Not Recorded') AS route_name,
       COALESCE(s.origin_city,      'Not Recorded') AS origin_city,
       s.origin_state                               AS origin_state_code,
       COALESCE(os.state_name,      'Unknown')      AS origin_state_name,
       COALESCE(s.destination_city, 'Not Recorded') AS destination_city,
       s.destination_state                          AS destination_state_code,
       COALESCE(ds.state_name,      'Unknown')      AS destination_state_name,
       s.typical_distance_miles,
       s.base_rate_per_mile,
       s.fuel_surcharge_rate,
       s.typical_transit_days,
       s._is_deleted_in_source                      AS is_deleted_in_source
FROM lh_logistics_silver.dbo.silver_routes AS s
LEFT JOIN ref.us_state AS os ON os.state_code = s.origin_state
LEFT JOIN ref.us_state AS ds ON ds.state_code = s.destination_state;

GO