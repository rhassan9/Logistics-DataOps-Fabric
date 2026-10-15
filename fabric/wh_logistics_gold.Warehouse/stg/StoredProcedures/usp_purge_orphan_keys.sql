CREATE   PROCEDURE stg.usp_purge_orphan_keys
    @run_id           VARCHAR(64),
    @older_than_hours INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @cutoff DATETIME2(6), @deleted BIGINT = 0, @msg VARCHAR(4000);
 
    IF @run_id IS NULL
        THROW 50702, 'stg.usp_purge_orphan_keys: @run_id is required.', 1;
 
    -- A floor protects runs still in flight between detection and the gold load.
    IF @older_than_hours IS NULL OR @older_than_hours < 24
        THROW 50701, 'stg.usp_purge_orphan_keys: @older_than_hours must be 24 or more.', 1;
 
    SET @cutoff = DATEADD(HOUR, -@older_than_hours, SYSUTCDATETIME());
 
    BEGIN TRY
        DELETE FROM stg.changed_trip_keys
        WHERE detected_at < @cutoff
          AND EXISTS (SELECT 1 FROM log.cdf_batch AS b
                      WHERE b.step_name = 'fact_trip'
                        AND b.detected_at > stg.changed_trip_keys.detected_at
                        AND EXISTS (SELECT 1 FROM log.etl_run AS r
                                    WHERE r.run_id = b.run_id AND r.step_name = b.step_name
                                      AND r.event_type = 'succeeded' AND r.event_at >= b.detected_at)
                        AND (b.is_full_scan = 1
                             OR NOT EXISTS (SELECT 1 FROM log.cdf_batch AS o
                                            WHERE o.run_id = stg.changed_trip_keys.run_id
                                              AND o.step_name = 'fact_trip' AND o.is_full_scan = 1)));
        SET @deleted += @@ROWCOUNT;
 
        DELETE FROM stg.changed_delivery_event_keys
        WHERE detected_at < @cutoff
          AND EXISTS (SELECT 1 FROM log.cdf_batch AS b
                      WHERE b.step_name = 'fact_delivery_event'
                        AND b.detected_at > stg.changed_delivery_event_keys.detected_at
                        AND EXISTS (SELECT 1 FROM log.etl_run AS r
                                    WHERE r.run_id = b.run_id AND r.step_name = b.step_name
                                      AND r.event_type = 'succeeded' AND r.event_at >= b.detected_at)
                        AND (b.is_full_scan = 1
                             OR NOT EXISTS (SELECT 1 FROM log.cdf_batch AS o
                                            WHERE o.run_id = stg.changed_delivery_event_keys.run_id
                                              AND o.step_name = 'fact_delivery_event' AND o.is_full_scan = 1)));
        SET @deleted += @@ROWCOUNT;
 
        DELETE FROM stg.changed_fuel_purchase_keys
        WHERE detected_at < @cutoff
          AND EXISTS (SELECT 1 FROM log.cdf_batch AS b
                      WHERE b.step_name = 'fact_fuel_purchase'
                        AND b.detected_at > stg.changed_fuel_purchase_keys.detected_at
                        AND EXISTS (SELECT 1 FROM log.etl_run AS r
                                    WHERE r.run_id = b.run_id AND r.step_name = b.step_name
                                      AND r.event_type = 'succeeded' AND r.event_at >= b.detected_at)
                        AND (b.is_full_scan = 1
                             OR NOT EXISTS (SELECT 1 FROM log.cdf_batch AS o
                                            WHERE o.run_id = stg.changed_fuel_purchase_keys.run_id
                                              AND o.step_name = 'fact_fuel_purchase' AND o.is_full_scan = 1)));
        SET @deleted += @@ROWCOUNT;
 
        DELETE FROM stg.changed_maintenance_keys
        WHERE detected_at < @cutoff
          AND EXISTS (SELECT 1 FROM log.cdf_batch AS b
                      WHERE b.step_name = 'fact_maintenance'
                        AND b.detected_at > stg.changed_maintenance_keys.detected_at
                        AND EXISTS (SELECT 1 FROM log.etl_run AS r
                                    WHERE r.run_id = b.run_id AND r.step_name = b.step_name
                                      AND r.event_type = 'succeeded' AND r.event_at >= b.detected_at)
                        AND (b.is_full_scan = 1
                             OR NOT EXISTS (SELECT 1 FROM log.cdf_batch AS o
                                            WHERE o.run_id = stg.changed_maintenance_keys.run_id
                                              AND o.step_name = 'fact_maintenance' AND o.is_full_scan = 1)));
        SET @deleted += @@ROWCOUNT;
 
        DELETE FROM stg.changed_safety_incident_keys
        WHERE detected_at < @cutoff
          AND EXISTS (SELECT 1 FROM log.cdf_batch AS b
                      WHERE b.step_name = 'fact_safety_incident'
                        AND b.detected_at > stg.changed_safety_incident_keys.detected_at
                        AND EXISTS (SELECT 1 FROM log.etl_run AS r
                                    WHERE r.run_id = b.run_id AND r.step_name = b.step_name
                                      AND r.event_type = 'succeeded' AND r.event_at >= b.detected_at)
                        AND (b.is_full_scan = 1
                             OR NOT EXISTS (SELECT 1 FROM log.cdf_batch AS o
                                            WHERE o.run_id = stg.changed_safety_incident_keys.run_id
                                              AND o.step_name = 'fact_safety_incident' AND o.is_full_scan = 1)));
        SET @deleted += @@ROWCOUNT;
 
        DELETE FROM stg.changed_load_keys
        WHERE detected_at < @cutoff
          AND EXISTS (SELECT 1 FROM log.cdf_batch AS b
                      WHERE b.step_name = 'fact_load_lifecycle'
                        AND b.detected_at > stg.changed_load_keys.detected_at
                        AND EXISTS (SELECT 1 FROM log.etl_run AS r
                                    WHERE r.run_id = b.run_id AND r.step_name = b.step_name
                                      AND r.event_type = 'succeeded' AND r.event_at >= b.detected_at)
                        AND (b.is_full_scan = 1
                             OR NOT EXISTS (SELECT 1 FROM log.cdf_batch AS o
                                            WHERE o.run_id = stg.changed_load_keys.run_id
                                              AND o.step_name = 'fact_load_lifecycle' AND o.is_full_scan = 1)));
        SET @deleted += @@ROWCOUNT;
 
        SET @msg = 'superseded worklist keys removed; unconsumed keys kept for recovery';
        EXEC log.usp_write_run_event @run_id, 'purge_worklists', NULL, 'succeeded',
             NULL, NULL, NULL, @deleted, NULL, @msg;
    END TRY
    BEGIN CATCH
        SET @msg = LEFT(ERROR_MESSAGE(), 4000);
        EXEC log.usp_write_run_event @run_id, 'purge_worklists', NULL, 'failed',
             NULL, NULL, NULL, NULL, NULL, @msg;
        THROW;
    END CATCH;
END;

GO