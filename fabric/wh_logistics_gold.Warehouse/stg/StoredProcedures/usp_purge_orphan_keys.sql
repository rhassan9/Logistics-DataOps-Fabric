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
 
    DELETE FROM stg.changed_trip_keys
    WHERE detected_at < @cutoff
      AND run_id NOT IN (SELECT r.run_id FROM log.etl_run AS r
                         WHERE r.step_name = 'fact_trip' AND r.event_type = 'succeeded');
 
    DELETE FROM stg.changed_delivery_event_keys
    WHERE detected_at < @cutoff
      AND run_id NOT IN (SELECT r.run_id FROM log.etl_run AS r
                         WHERE r.step_name = 'fact_delivery_event' AND r.event_type = 'succeeded');
 
    DELETE FROM stg.changed_fuel_purchase_keys
    WHERE detected_at < @cutoff
      AND run_id NOT IN (SELECT r.run_id FROM log.etl_run AS r
                         WHERE r.step_name = 'fact_fuel_purchase' AND r.event_type = 'succeeded');
 
    DELETE FROM stg.changed_maintenance_keys
    WHERE detected_at < @cutoff
      AND run_id NOT IN (SELECT r.run_id FROM log.etl_run AS r
                         WHERE r.step_name = 'fact_maintenance' AND r.event_type = 'succeeded');
 
    DELETE FROM stg.changed_safety_incident_keys
    WHERE detected_at < @cutoff
      AND run_id NOT IN (SELECT r.run_id FROM log.etl_run AS r
                         WHERE r.step_name = 'fact_safety_incident' AND r.event_type = 'succeeded');
END;

GO