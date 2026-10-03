CREATE   PROCEDURE dim.usp_load_dim_maintenance_class
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dim.dim_maintenance_class (maintenance_type, service_urgency)
    SELECT DISTINCT v.maintenance_type, v.service_urgency
    FROM dim.vw_src_maintenance_class AS v
    WHERE NOT EXISTS (SELECT 1 FROM dim.dim_maintenance_class AS j
                      WHERE j.maintenance_type = v.maintenance_type AND j.service_urgency = v.service_urgency);
END;

GO