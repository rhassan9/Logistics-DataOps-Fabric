CREATE   VIEW dim.vw_src_load_type
AS
SELECT l.load_id,
       COALESCE(l.booking_type, 'Not Recorded') AS booking_type,
       COALESCE(l.load_type,    'Not Recorded') AS load_type
FROM lh_logistics_silver.dbo.silver_loads AS l;

GO