CREATE   VIEW dim.vw_src_maintenance_class
AS
SELECT m.maintenance_id,
       COALESCE(m.maintenance_type, 'Not Recorded') AS maintenance_type,
       COALESCE(m.service_urgency,  'Not Recorded') AS service_urgency
FROM lh_logistics_silver.dbo.silver_maintenance_records AS m;

GO