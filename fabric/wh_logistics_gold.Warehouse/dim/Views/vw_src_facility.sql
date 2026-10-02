CREATE   VIEW dim.vw_src_facility
AS
SELECT s.facility_id,
       COALESCE(s.facility_name,   'Not Recorded') AS facility_name,
       COALESCE(s.facility_type,   'Not Recorded') AS facility_type,
       COALESCE(s.city,            'Not Recorded') AS facility_city,
       s.state                                     AS facility_state_code,
       COALESCE(us.state_name,     'Unknown')      AS facility_state_name,
       COALESCE(s.operating_hours, 'Not Recorded') AS operating_hours,
       s.dock_doors,
       s.latitude                                  AS facility_latitude,
       s.longitude                                 AS facility_longitude,
       s.is_centroid_coordinate
FROM lh_logistics_silver.dbo.silver_facilities AS s
LEFT JOIN ref.us_state AS us ON us.state_code = s.state;

GO