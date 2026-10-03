EXEC dim.usp_seed_special_members_derived;
EXEC dim.usp_load_dim_load_type;  EXEC dim.usp_load_dim_delivery_status;
EXEC dim.usp_load_dim_maintenance_class;  EXEC dim.usp_load_dim_incident_class;
EXEC dim.usp_load_dim_location;

SELECT 'load_type' AS dim, COUNT(*) AS total_rows FROM dim.dim_load_type
UNION ALL SELECT 'delivery_status',   COUNT(*) FROM dim.dim_delivery_status
UNION ALL SELECT 'maintenance_class', COUNT(*) FROM dim.dim_maintenance_class
UNION ALL SELECT 'incident_class',    COUNT(*) FROM dim.dim_incident_class
UNION ALL SELECT 'location',          COUNT(*) FROM dim.dim_location;
