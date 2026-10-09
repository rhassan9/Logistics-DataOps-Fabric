CREATE   VIEW dim.vw_src_driver
AS
SELECT s.driver_id,
       CASE WHEN s.first_name IS NULL AND s.last_name IS NULL THEN 'Not Recorded'
            ELSE LTRIM(RTRIM(CONCAT(s.first_name, ' ', s.last_name))) END AS driver_name,
       COALESCE(s.home_terminal,     'Not Recorded') AS driver_home_terminal,
       COALESCE(s.employment_status, 'Not Recorded') AS employment_status,
       s.hire_date                                   AS driver_hire_date,
       s.termination_date                            AS driver_termination_date,
       s.years_experience,
       s._is_deleted_in_source                       AS is_deleted_in_source,
       YEAR(s._source_changed_at) * 10000
           + MONTH(s._source_changed_at) * 100
           + DAY(s._source_changed_at)               AS source_changed_date_key
FROM lh_logistics_silver.dbo.silver_drivers AS s;

GO