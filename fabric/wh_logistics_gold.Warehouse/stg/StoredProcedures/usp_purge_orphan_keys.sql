CREATE   PROCEDURE stg.usp_purge_orphan_keys
    @older_than_hours INT
AS
BEGIN
    SET NOCOUNT ON;

    -- A floor protects runs still in flight between detection and the gold load.
    IF @older_than_hours IS NULL OR @older_than_hours < 24
        THROW 50601, 'stg.usp_purge_orphan_keys: @older_than_hours must be 24 or more.', 1;

    DELETE FROM stg.changed_trip_keys
    WHERE detected_at < DATEADD(HOUR, -@older_than_hours, SYSUTCDATETIME())
      AND run_id NOT IN (SELECT r.run_id FROM log.etl_run AS r
                         WHERE r.step_name = 'fact_trip' AND r.event_type = 'succeeded');
END;

GO