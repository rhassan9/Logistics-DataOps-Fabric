/* ===========================================================================
   fact.usp_load_fact_maintenance                          errors 5064x
   Silver dependency: silver_maintenance_records only.
   location_key comes from facility_location, which is free text in the
   source, so it resolves to dim_location at city grain.
   Truck (Type 2) is looked up at the maintenance date.
   =========================================================================== */
 
CREATE   PROCEDURE fact.usp_load_fact_maintenance
    @run_id            VARCHAR(64),
    @allow_mass_delete BIT
AS
BEGIN
    SET NOCOUNT ON;
 
    DECLARE
        @step                  VARCHAR(128) = 'fact_maintenance',
        @source                VARCHAR(128) = 'silver_maintenance_records',
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
        THROW 50641, 'fact.usp_load_fact_maintenance: @run_id and @allow_mass_delete are required.', 1;
 
    EXEC log.usp_write_run_event @run_id, @step, @source, 'started',
         NULL, NULL, NULL, NULL, NULL, NULL;
 
    BEGIN TRY
        IF (SELECT COUNT(DISTINCT b.source_table) FROM log.cdf_batch AS b
            WHERE b.run_id = @run_id AND b.step_name = @step
              AND b.source_table IN ('silver_maintenance_records')) <> 1
            THROW 50642, 'fact.usp_load_fact_maintenance: change detection must record silver_maintenance_records for this run_id.', 1;
 
        DROP TABLE IF EXISTS #keys;
        CREATE TABLE #keys (maintenance_id VARCHAR(20) NOT NULL) WITH (DISTRIBUTION = ROUND_ROBIN);
 
        INSERT INTO #keys (maintenance_id)
        SELECT DISTINCT k.maintenance_id FROM stg.changed_maintenance_keys AS k
        WHERE k.run_id = @run_id AND k.maintenance_id IS NOT NULL;
 
        SELECT @keys = COUNT(*) FROM #keys;
 
        DROP TABLE IF EXISTS #stage;
        CREATE TABLE #stage
        (
            maintenance_date_key   INT,
            truck_key              BIGINT,
            location_key           BIGINT,
            maintenance_class_key  BIGINT,
            maintenance_id         VARCHAR(20),
            labor_hours            DECIMAL(8,2),
            labor_cost             DECIMAL(14,2),
            parts_cost             DECIMAL(14,2),
            downtime_hours         DECIMAL(10,2),
            odometer_reading       INT,
            is_deleted_in_source   BIT
        ) WITH (DISTRIBUTION = ROUND_ROBIN);
 
        INSERT INTO #stage
            (maintenance_date_key, truck_key, location_key, maintenance_class_key, maintenance_id,
             labor_hours, labor_cost, parts_cost, downtime_hours, odometer_reading, is_deleted_in_source)
        SELECT
            CASE WHEN s.maintenance_date IS NULL THEN 0 WHEN dd.date_key IS NULL THEN -1 ELSE dd.date_key END,
            CASE WHEN s.truck_id IS NULL THEN 0 ELSE COALESCE(tk.truck_key, -1) END,
            CASE WHEN s.facility_location IS NULL THEN 0 ELSE COALESCE(lc.location_key, -1) END,
            COALESCE(mc.maintenance_class_key, -1),
            s.maintenance_id, s.labor_hours, s.labor_cost, s.parts_cost, s.downtime_hours, s.odometer_reading,
            CAST(CASE WHEN s._is_deleted_in_source = 1 THEN 1 ELSE 0 END AS BIT)
        FROM
        (
            SELECT m.maintenance_id, m.truck_id, m.maintenance_date, m.facility_location,
                   m.labor_hours, m.labor_cost, m.parts_cost, m.downtime_hours, m.odometer_reading,
                   m._is_deleted_in_source,
                   v.maintenance_type, v.service_urgency,
                   CASE WHEN m.maintenance_date IS NULL THEN 0
                        ELSE YEAR(m.maintenance_date) * 10000 + MONTH(m.maintenance_date) * 100 + DAY(m.maintenance_date) END AS event_key
            FROM #keys AS k
            JOIN lh_logistics_silver.dbo.silver_maintenance_records AS m ON m.maintenance_id = k.maintenance_id
            LEFT JOIN dim.vw_src_maintenance_class AS v ON v.maintenance_id = m.maintenance_id
        ) AS s
        LEFT JOIN dim.dim_date AS dd ON dd.date_key = s.event_key AND dd.date_key > 0
        LEFT JOIN dim.dim_truck AS tk
          ON tk.truck_id = s.truck_id AND tk.truck_key > 0
         AND s.event_key >= tk.valid_from_date_key AND s.event_key < tk.valid_to_date_key
        LEFT JOIN dim.dim_location AS lc
          ON lc.location_city = s.facility_location AND lc.location_key > 0
        LEFT JOIN dim.dim_maintenance_class AS mc
          ON mc.maintenance_type = s.maintenance_type AND mc.service_urgency = s.service_urgency
         AND mc.maintenance_class_key > 0;
 
        SELECT @rows_stage = COUNT(*), @rows_distinct = COUNT(DISTINCT st.maintenance_id),
               @rows_unknown = COALESCE(SUM(CASE WHEN -1 IN (st.maintenance_date_key, st.truck_key,
                                   st.location_key, st.maintenance_class_key) THEN 1 ELSE 0 END), 0)
        FROM #stage AS st;
 
        IF @rows_stage <> @rows_distinct
            THROW 50643, 'fact.usp_load_fact_maintenance: a record resolved to more than one dimension row.', 1;
 
        SELECT @rows_existing = COUNT(*) FROM fact.fact_maintenance AS f
        JOIN #stage AS st ON st.maintenance_id = f.maintenance_id;
 
        SET @rows_inserted = @rows_stage - @rows_existing;
        SET @rows_updated  = @rows_existing;
 
        SELECT @active_before = COUNT(*) FROM fact.fact_maintenance AS f WHERE f.is_deleted_in_source = 0;
 
        SELECT @rows_explicit_deleted = COUNT(*) FROM fact.fact_maintenance AS f
        JOIN #stage AS st ON st.maintenance_id = f.maintenance_id
        WHERE f.is_deleted_in_source = 0 AND st.is_deleted_in_source = 1;
 
        SELECT @rows_missing_deleted = COUNT(*) FROM fact.fact_maintenance AS f
        JOIN #keys AS k ON k.maintenance_id = f.maintenance_id
        LEFT JOIN #stage AS st ON st.maintenance_id = f.maintenance_id
        WHERE f.is_deleted_in_source = 0 AND st.maintenance_id IS NULL;
 
        SET @rows_deleted = @rows_explicit_deleted + @rows_missing_deleted;
 
        IF @allow_mass_delete = 0 AND @rows_deleted > 0 AND @active_before > 0
           AND @rows_deleted * 10 > @active_before
            THROW 50644, 'fact.usp_load_fact_maintenance: more than 10 percent of active records would be soft-deleted; investigate silver before using @allow_mass_delete = 1.', 1;
 
        BEGIN TRANSACTION;
 
        DELETE FROM fact.fact_maintenance WHERE maintenance_id IN (SELECT st.maintenance_id FROM #stage AS st);
 
        INSERT INTO fact.fact_maintenance
            (maintenance_date_key, truck_key, location_key, maintenance_class_key, maintenance_id,
             labor_hours, labor_cost, parts_cost, downtime_hours, odometer_reading, is_deleted_in_source)
        SELECT st.maintenance_date_key, st.truck_key, st.location_key, st.maintenance_class_key, st.maintenance_id,
               st.labor_hours, st.labor_cost, st.parts_cost, st.downtime_hours, st.odometer_reading, st.is_deleted_in_source
        FROM #stage AS st;
 
        UPDATE fact.fact_maintenance SET is_deleted_in_source = 1
        WHERE is_deleted_in_source = 0
          AND maintenance_id IN (SELECT k.maintenance_id FROM #keys AS k)
          AND maintenance_id NOT IN (SELECT st.maintenance_id FROM #stage AS st);
 
        DELETE FROM stg.changed_maintenance_keys WHERE run_id = @run_id;
 
        COMMIT TRANSACTION;
 
        EXEC log.usp_write_run_event @run_id, @step, @source, 'succeeded',
             @keys, @rows_inserted, @rows_updated, @rows_deleted, @rows_unknown,
             'fact_maintenance published; checkpoint may now advance';
 
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