CREATE   PROCEDURE dim.usp_load_dim_incident_class
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dim.dim_incident_class
        (incident_type, incident_category, incident_severity, incident_cause,
         preventable_status, at_fault_status, injury_status)
    SELECT DISTINCT v.incident_type, v.incident_category, v.incident_severity, v.incident_cause,
                    v.preventable_status, v.at_fault_status, v.injury_status
    FROM dim.vw_src_incident_class AS v
    WHERE NOT EXISTS (SELECT 1 FROM dim.dim_incident_class AS j
                      WHERE j.incident_type      = v.incident_type
                        AND j.incident_category  = v.incident_category
                        AND j.incident_severity  = v.incident_severity
                        AND j.incident_cause     = v.incident_cause
                        AND j.preventable_status = v.preventable_status
                        AND j.at_fault_status    = v.at_fault_status
                        AND j.injury_status      = v.injury_status);
END;

GO