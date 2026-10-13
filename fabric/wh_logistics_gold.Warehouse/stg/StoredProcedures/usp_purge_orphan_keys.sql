CREATE   PROCEDURE stg.usp_purge_orphan_keys
    @older_than_hours INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @cutoff DATETIME2(6);
 
    -- A floor protects runs still in flight between detection and the gold load.
    IF @older_than_hours IS NULL OR @older_than_hours < 24
        THROW 50701, 'stg.usp_purge_orphan_keys: @older_than_hours must be 24 or more.', 1;
 
    SET @cutoff = DATEADD(HOUR, -@older_than_hours, SYSUTCDATETIME());
 
    -- A key is orphaned when no success of its step followed its detection.
    DELETE FROM stg.changed_trip_keys
    WHERE detected_at < @cutoff
      AND NOT EXISTS (SELECT 1 FROM log.etl_run AS r
                      WHERE r.run_id = stg.changed_trip_keys.run_id
                        AND r.step_name = 'fact_trip' AND r.event_type = 'succeeded'
                        AND r.event_at >= stg.changed_trip_keys.detected_at);
 
    DELETE FROM stg.changed_delivery_event_keys
    WHERE detected_at < @cutoff
      AND NOT EXISTS (SELECT 1 FROM log.etl_run AS r
                      WHERE r.run_id = stg.changed_delivery_event_keys.run_id
                        AND r.step_name = 'fact_delivery_event' AND r.event_type = 'succeeded'
                        AND r.event_at >= stg.changed_delivery_event_keys.detected_at);
 
    DELETE FROM stg.changed_fuel_purchase_keys
    WHERE detected_at < @cutoff
      AND NOT EXISTS (SELECT 1 FROM log.etl_run AS r
                      WHERE r.run_id = stg.changed_fuel_purchase_keys.run_id
                        AND r.step_name = 'fact_fuel_purchase' AND r.event_type = 'succeeded'
                        AND r.event_at >= stg.changed_fuel_purchase_keys.detected_at);
 
    DELETE FROM stg.changed_maintenance_keys
    WHERE detected_at < @cutoff
      AND NOT EXISTS (SELECT 1 FROM log.etl_run AS r
                      WHERE r.run_id = stg.changed_maintenance_keys.run_id
                        AND r.step_name = 'fact_maintenance' AND r.event_type = 'succeeded'
                        AND r.event_at >= stg.changed_maintenance_keys.detected_at);
 
    DELETE FROM stg.changed_safety_incident_keys
    WHERE detected_at < @cutoff
      AND NOT EXISTS (SELECT 1 FROM log.etl_run AS r
                      WHERE r.run_id = stg.changed_safety_incident_keys.run_id
                        AND r.step_name = 'fact_safety_incident' AND r.event_type = 'succeeded'
                        AND r.event_at >= stg.changed_safety_incident_keys.detected_at);
 
    DELETE FROM stg.changed_load_keys
    WHERE detected_at < @cutoff
      AND NOT EXISTS (SELECT 1 FROM log.etl_run AS r
                      WHERE r.run_id = stg.changed_load_keys.run_id
                        AND r.step_name = 'fact_load_lifecycle' AND r.event_type = 'succeeded'
                        AND r.event_at >= stg.changed_load_keys.detected_at);
END;

GO