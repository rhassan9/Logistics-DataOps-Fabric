CREATE   VIEW dim.vw_src_incident_class
AS
SELECT i.incident_id,
       COALESCE(i.incident_type,     'Not Recorded') AS incident_type,
       COALESCE(i.incident_category, 'Not Recorded') AS incident_category,
       COALESCE(i.incident_severity, 'Not Recorded') AS incident_severity,
       COALESCE(i.incident_cause,    'Not Recorded') AS incident_cause,
       CASE i.preventable_flag WHEN 1 THEN 'Preventable' WHEN 0 THEN 'Not Preventable' ELSE 'Not Recorded' END AS preventable_status,
       CASE i.at_fault_flag    WHEN 1 THEN 'At Fault'    WHEN 0 THEN 'Not At Fault'    ELSE 'Not Recorded' END AS at_fault_status,
       CASE i.injury_flag      WHEN 1 THEN 'Injury'      WHEN 0 THEN 'No Injury'       ELSE 'Not Recorded' END AS injury_status
FROM lh_logistics_silver.dbo.silver_safety_incidents AS i;

GO