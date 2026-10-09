CREATE   VIEW dim.vw_src_truck
AS
SELECT s.truck_id,
       COALESCE(s.unit_number,   'Not Recorded') AS unit_number,
       COALESCE(s.make,          'Not Recorded') AS truck_make,
       COALESCE(s.status,        'Not Recorded') AS truck_status,
       COALESCE(s.home_terminal, 'Not Recorded') AS truck_home_terminal,
       s.model_year                              AS truck_model_year,
       s.acquisition_date                        AS truck_acquisition_date,
       s.acquisition_mileage,
       s.tank_capacity_gallons,
       s._is_deleted_in_source                   AS is_deleted_in_source,
       YEAR(s._source_changed_at) * 10000
           + MONTH(s._source_changed_at) * 100
           + DAY(s._source_changed_at)           AS source_changed_date_key
FROM lh_logistics_silver.dbo.silver_trucks AS s;

GO