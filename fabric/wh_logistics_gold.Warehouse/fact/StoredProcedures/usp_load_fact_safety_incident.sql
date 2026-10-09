/* ===========================================================================
   fact.usp_load_fact_safety_incident                      errors 5065x
   Silver dependency: silver_safety_incidents only. trip_id is a
   degenerate key, not a lookup, so trips are not a dependency.
   Driver and truck (Type 2) are looked up at the incident date.
   =========================================================================== */
 
CREATE   PROCEDURE fact.usp_load_fact_safety_incident
    @run_id            VARCHAR(64),
    @allow_mass_delete BIT
AS
BEGIN
    SET NOCOUNT ON;
 
    DECLARE
        @step                  VARCHAR(128) = 'fact_safety_incident',
        @source                VARCHAR(128) = 'silver_safety_incidents',
        @keys                  BIGINT = 0,
        @rows_stage            BIGINT = 0,
        @rows_distinct         BIGINT = 0,
        @rows_existing         BIGINT = 0,
        @rows_inserted         BIGINT = 0,
        @rows_updated          BIGINT = 0,
        @rows_deleted          BIGINT = 0,
        @rows_unknown          BIGINT = 0,
        @rows_explicit_deleted BIGINT = 0,
        @rows_missing_deleted  BIGINT = 0,
        @active_before         BIGINT = 0,
        @msg                   VARCHAR(4000);
 
    IF @run_id IS NULL OR @allow_mass_delete IS NULL
        THROW 50651, 'fact.usp_load_fact_safety_incident: @run_id and @allow_mass_delete are required.', 1;
 
    EXEC log.usp_write_run_event @run_id, @step, @source, 'started',
         NULL, NULL, NULL, NULL, NULL, NULL;
 
    BEGIN TRY
        IF (SELECT COUNT(DISTINCT b.source_table) FROM log.cdf_batch AS b
            WHERE b.run_id = @run_id AND b.step_name = @step
              AND b.source_table IN ('silver_safety_incidents')) <> 1
            THROW 50652, 'fact.usp_load_fact_safety_incident: change detection must record silver_safety_incidents for this run_id.', 1;
 
        DROP TABLE IF EXISTS #keys;
        CREATE TABLE #keys (incident_id VARCHAR(20) NOT NULL) WITH (DISTRIBUTION = ROUND_ROBIN);
 
        INSERT INTO #keys (incident_id)
        SELECT DISTINCT k.incident_id FROM stg.changed_safety_incident_keys AS k
        WHERE k.run_id = @run_id AND k.incident_id IS NOT NULL;
 
        SELECT @keys = COUNT(*) FROM #keys;
 
        DROP TABLE IF EXISTS #stage;
        CREATE TABLE #stage
        (
            incident_date_key    INT,
            driver_key           BIGINT,
            truck_key            BIGINT,
            location_key         BIGINT,
            incident_class_key   BIGINT,
            incident_id          VARCHAR(20),
            trip_id              VARCHAR(20),
            vehicle_damage_cost  DECIMAL(14,2),
            cargo_damage_cost    DECIMAL(14,2),
            is_deleted_in_source BIT
        ) WITH (DISTRIBUTION = ROUND_ROBIN);
 
        INSERT INTO #stage
            (incident_date_key, driver_key, truck_key, location_key, incident_class_key,
             incident_id, trip_id, vehicle_damage_cost, cargo_damage_cost, is_deleted_in_source)
        SELECT
            CASE WHEN s.incident_date IS NULL THEN 0 WHEN dd.date_key IS NULL THEN -1 ELSE dd.date_key END,
            CASE WHEN s.driver_id IS NULL THEN 0 ELSE COALESCE(dr.driver_key, -1) END,
            CASE WHEN s.truck_id  IS NULL THEN 0 ELSE COALESCE(tk.truck_key,  -1) END,
            CASE WHEN s.location_city IS NULL THEN 0 ELSE COALESCE(lc.location_key, -1) END,
            COALESCE(ic.incident_class_key, -1),
            s.incident_id, s.trip_id, s.vehicle_damage_cost, s.cargo_damage_cost,
            CAST(CASE WHEN s._is_deleted_in_source = 1 THEN 1 ELSE 0 END AS BIT)
        FROM
        (
            SELECT i.incident_id, i.trip_id, i.driver_id, i.truck_id, i.incident_date, i.location_city,
                   i.vehicle_damage_cost, i.cargo_damage_cost, i._is_deleted_in_source,
                   v.incident_type, v.incident_category, v.incident_severity, v.incident_cause,
                   v.preventable_status, v.at_fault_status, v.injury_status,
                   CASE WHEN i.incident_date IS NULL THEN 0
                        ELSE YEAR(i.incident_date) * 10000 + MONTH(i.incident_date) * 100 + DAY(i.incident_date) END AS event_key
            FROM #keys AS k
            JOIN lh_logistics_silver.dbo.silver_safety_incidents AS i ON i.incident_id = k.incident_id
            LEFT JOIN dim.vw_src_incident_class AS v ON v.incident_id = i.incident_id
        ) AS s
        LEFT JOIN dim.dim_date AS dd ON dd.date_key = s.event_key AND dd.date_key > 0
        LEFT JOIN dim.dim_driver AS dr
          ON dr.driver_id = s.driver_id AND dr.driver_key > 0
         AND s.event_key >= dr.valid_from_date_key AND s.event_key < dr.valid_to_date_key
        LEFT JOIN dim.dim_truck AS tk
          ON tk.truck_id = s.truck_id AND tk.truck_key > 0
         AND s.event_key >= tk.valid_from_date_key AND s.event_key < tk.valid_to_date_key
        LEFT JOIN dim.dim_location AS lc
          ON lc.location_city = s.location_city AND lc.location_key > 0
        LEFT JOIN dim.dim_incident_class AS ic
          ON  ic.incident_type      = s.incident_type
          AND ic.incident_category  = s.incident_category
          AND ic.incident_severity  = s.incident_severity
          AND ic.incident_cause     = s.incident_cause
          AND ic.preventable_status = s.preventable_status
          AND ic.at_fault_status    = s.at_fault_status
          AND ic.injury_status      = s.injury_status
          AND ic.incident_class_key > 0;
 
        SELECT @rows_stage = COUNT(*), @rows_distinct = COUNT(DISTINCT st.incident_id),
               @rows_unknown = COALESCE(SUM(CASE WHEN -1 IN (st.incident_date_key, st.driver_key, st.truck_key,
                                   st.location_key, st.incident_class_key) THEN 1 ELSE 0 END), 0)
        FROM #stage AS st;
 
        IF @rows_stage <> @rows_distinct
            THROW 50653, 'fact.usp_load_fact_safety_incident: an incident resolved to more than one dimension row.', 1;
 
        SELECT @rows_existing = COUNT(*) FROM fact.fact_safety_incident AS f
        JOIN #stage AS st ON st.incident_id = f.incident_id;
 
        SET @rows_inserted = @rows_stage - @rows_existing;
        SET @rows_updated  = @rows_existing;
 
        SELECT @active_before = COUNT(*) FROM fact.fact_safety_incident AS f WHERE f.is_deleted_in_source = 0;
 
        SELECT @rows_explicit_deleted = COUNT(*) FROM fact.fact_safety_incident AS f
        JOIN #stage AS st ON st.incident_id = f.incident_id
        WHERE f.is_deleted_in_source = 0 AND st.is_deleted_in_source = 1;
 
        SELECT @rows_missing_deleted = COUNT(*) FROM fact.fact_safety_incident AS f
        JOIN #keys AS k ON k.incident_id = f.incident_id
        LEFT JOIN #stage AS st ON st.incident_id = f.incident_id
        WHERE f.is_deleted_in_source = 0 AND st.incident_id IS NULL;
 
        SET @rows_deleted = @rows_explicit_deleted + @rows_missing_deleted;
 
        IF @allow_mass_delete = 0 AND @rows_deleted > 0 AND @active_before > 0
           AND @rows_deleted * 10 > @active_before
            THROW 50654, 'fact.usp_load_fact_safety_incident: more than 10 percent of active incidents would be soft-deleted; investigate silver before using @allow_mass_delete = 1.', 1;
 
        BEGIN TRANSACTION;
 
        DELETE FROM fact.fact_safety_incident WHERE incident_id IN (SELECT st.incident_id FROM #stage AS st);
 
        INSERT INTO fact.fact_safety_incident
            (incident_date_key, driver_key, truck_key, location_key, incident_class_key,
             incident_id, trip_id, vehicle_damage_cost, cargo_damage_cost, is_deleted_in_source)
        SELECT st.incident_date_key, st.driver_key, st.truck_key, st.location_key, st.incident_class_key,
               st.incident_id, st.trip_id, st.vehicle_damage_cost, st.cargo_damage_cost, st.is_deleted_in_source
        FROM #stage AS st;
 
        UPDATE fact.fact_safety_incident SET is_deleted_in_source = 1
        WHERE is_deleted_in_source = 0
          AND incident_id IN (SELECT k.incident_id FROM #keys AS k)
          AND incident_id NOT IN (SELECT st.incident_id FROM #stage AS st);
 
        DELETE FROM stg.changed_safety_incident_keys WHERE run_id = @run_id;
 
        COMMIT TRANSACTION;
 
        EXEC log.usp_write_run_event @run_id, @step, @source, 'succeeded',
             @keys, @rows_inserted, @rows_updated, @rows_deleted, @rows_unknown,
             'fact_safety_incident published; checkpoint may now advance';
 
        DROP TABLE IF EXISTS #stage;
        DROP TABLE IF EXISTS #keys;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        SET @msg = LEFT(ERROR_MESSAGE(), 4000);
        EXEC log.usp_write_run_event @run_id, @step, @source, 'failed',
             NULL, NULL, NULL, NULL, NULL, @msg;
        THROW;
    END CATCH;
END;

GO