/* ===========================================================================
   3. fact.usp_load_fact_delivery_event                       errors 5062x
   Silver dependencies: silver_delivery_events, silver_loads.
   route_key comes from the event's own load. silver_loads is not filtered
   on its deletion flag: a live event keeps the route it ran on.
   Facility and route are Type 1, looked up at the event date: actual,
   or scheduled when no actual time is recorded.
   =========================================================================== */
 
CREATE   PROCEDURE fact.usp_load_fact_delivery_event
    @run_id            VARCHAR(64),
    @allow_mass_delete BIT
AS
BEGIN
    SET NOCOUNT ON;
 
    DECLARE
        @step                  VARCHAR(128) = 'fact_delivery_event',
        @source                VARCHAR(128) = 'silver_delivery_events,silver_loads',
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
        THROW 50621, 'fact.usp_load_fact_delivery_event: @run_id and @allow_mass_delete are required.', 1;
 
    EXEC log.usp_write_run_event @run_id, @step, @source, 'started',
         NULL, NULL, NULL, NULL, NULL, NULL;
 
    BEGIN TRY
        IF (SELECT COUNT(DISTINCT b.source_table) FROM log.cdf_batch AS b
            WHERE b.run_id = @run_id AND b.step_name = @step
              AND b.source_table IN ('silver_delivery_events', 'silver_loads')) <> 2
            THROW 50622, 'fact.usp_load_fact_delivery_event: change detection must record silver_delivery_events and silver_loads for this run_id.', 1;
 
        DROP TABLE IF EXISTS #keys;
        CREATE TABLE #keys (event_id VARCHAR(20) NOT NULL) WITH (DISTRIBUTION = ROUND_ROBIN);
 
        INSERT INTO #keys (event_id)
        SELECT DISTINCT k.event_id FROM stg.changed_delivery_event_keys AS k
        WHERE k.run_id = @run_id AND k.event_id IS NOT NULL;
 
        SELECT @keys = COUNT(*) FROM #keys;
 
        DROP TABLE IF EXISTS #stage;
        CREATE TABLE #stage
        (
            scheduled_date_key         INT,
            actual_date_key            INT,
            facility_key               BIGINT,
            route_key                  BIGINT,
            delivery_status_key        BIGINT,
            event_id                   VARCHAR(20),
            load_id                    VARCHAR(20),
            trip_id                    VARCHAR(20),
            scheduled_datetime         DATETIME2(6),
            actual_datetime            DATETIME2(6),
            detention_minutes          INT,
            billable_detention_minutes INT,
            arrival_variance_minutes   DECIMAL(10,1),
            is_deleted_in_source       BIT
        ) WITH (DISTRIBUTION = ROUND_ROBIN);
 
        INSERT INTO #stage
            (scheduled_date_key, actual_date_key, facility_key, route_key, delivery_status_key,
             event_id, load_id, trip_id, scheduled_datetime, actual_datetime,
             detention_minutes, billable_detention_minutes, arrival_variance_minutes, is_deleted_in_source)
        SELECT
            CASE WHEN s.scheduled_datetime IS NULL THEN 0 WHEN sd.date_key IS NULL THEN -1 ELSE sd.date_key END,
            CASE WHEN s.actual_datetime    IS NULL THEN 0 WHEN ad.date_key IS NULL THEN -1 ELSE ad.date_key END,
            CASE WHEN s.facility_id IS NULL THEN 0 ELSE COALESCE(fc.facility_key, -1) END,
            CASE WHEN s.load_id    IS NULL THEN 0
                 WHEN s.load_found IS NULL THEN -1
                 WHEN s.route_id   IS NULL THEN 0
                 ELSE COALESCE(rt.route_key, -1) END,
            COALESCE(ds.delivery_status_key, -1),
            s.event_id, s.load_id, s.trip_id, s.scheduled_datetime, s.actual_datetime,
            s.detention_minutes, s.billable_detention_minutes, s.arrival_variance_minutes,
            CAST(CASE WHEN s._is_deleted_in_source = 1 THEN 1 ELSE 0 END AS BIT)
        FROM
        (
            SELECT e.event_id, e.load_id, e.trip_id, e.facility_id,
                   e.scheduled_datetime, e.actual_datetime,
                   e.detention_minutes, e.billable_detention_minutes, e.arrival_variance_minutes,
                   e._is_deleted_in_source,
                   l.load_id AS load_found, l.route_id,
                   v.event_type, v.arrival_status,
                   CASE WHEN e.scheduled_datetime IS NULL THEN 0
                        ELSE YEAR(e.scheduled_datetime) * 10000 + MONTH(e.scheduled_datetime) * 100 + DAY(e.scheduled_datetime) END AS scheduled_key,
                   CASE WHEN e.actual_datetime IS NULL THEN 0
                        ELSE YEAR(e.actual_datetime) * 10000 + MONTH(e.actual_datetime) * 100 + DAY(e.actual_datetime) END AS actual_key,
                   CASE WHEN COALESCE(e.actual_datetime, e.scheduled_datetime) IS NULL THEN 0
                        ELSE YEAR(COALESCE(e.actual_datetime, e.scheduled_datetime)) * 10000
                           + MONTH(COALESCE(e.actual_datetime, e.scheduled_datetime)) * 100
                           + DAY(COALESCE(e.actual_datetime, e.scheduled_datetime)) END AS event_key
            FROM #keys AS k
            JOIN lh_logistics_silver.dbo.silver_delivery_events AS e ON e.event_id = k.event_id
            LEFT JOIN lh_logistics_silver.dbo.silver_loads AS l ON l.load_id = e.load_id
            LEFT JOIN dim.vw_src_delivery_status AS v ON v.event_id = e.event_id
        ) AS s
        LEFT JOIN dim.dim_date AS sd ON sd.date_key = s.scheduled_key AND sd.date_key > 0
        LEFT JOIN dim.dim_date AS ad ON ad.date_key = s.actual_key    AND ad.date_key > 0
        LEFT JOIN dim.dim_facility AS fc
          ON fc.facility_id = s.facility_id AND fc.facility_key > 0
         AND s.event_key >= fc.valid_from_date_key AND s.event_key < fc.valid_to_date_key
        LEFT JOIN dim.dim_route AS rt
          ON rt.route_id = s.route_id AND rt.route_key > 0
         AND s.event_key >= rt.valid_from_date_key AND s.event_key < rt.valid_to_date_key
        LEFT JOIN dim.dim_delivery_status AS ds
          ON ds.event_type = s.event_type AND ds.arrival_status = s.arrival_status
         AND ds.delivery_status_key > 0;
 
        SELECT @rows_stage = COUNT(*), @rows_distinct = COUNT(DISTINCT st.event_id),
               @rows_unknown = COALESCE(SUM(CASE WHEN -1 IN (st.scheduled_date_key, st.actual_date_key,
                                   st.facility_key, st.route_key, st.delivery_status_key) THEN 1 ELSE 0 END), 0)
        FROM #stage AS st;
 
        IF @rows_stage <> @rows_distinct
            THROW 50623, 'fact.usp_load_fact_delivery_event: an event resolved to more than one dimension row.', 1;
 
        SELECT @rows_existing = COUNT(*) FROM fact.fact_delivery_event AS f
        JOIN #stage AS st ON st.event_id = f.event_id;
 
        SET @rows_inserted = @rows_stage - @rows_existing;
        SET @rows_updated  = @rows_existing;
 
        SELECT @active_before = COUNT(*) FROM fact.fact_delivery_event AS f WHERE f.is_deleted_in_source = 0;
 
        SELECT @rows_explicit_deleted = COUNT(*) FROM fact.fact_delivery_event AS f
        JOIN #stage AS st ON st.event_id = f.event_id
        WHERE f.is_deleted_in_source = 0 AND st.is_deleted_in_source = 1;
 
        SELECT @rows_missing_deleted = COUNT(*) FROM fact.fact_delivery_event AS f
        JOIN #keys AS k ON k.event_id = f.event_id
        LEFT JOIN #stage AS st ON st.event_id = f.event_id
        WHERE f.is_deleted_in_source = 0 AND st.event_id IS NULL;
 
        SET @rows_deleted = @rows_explicit_deleted + @rows_missing_deleted;
 
        IF @allow_mass_delete = 0 AND @rows_deleted > 0 AND @active_before > 0
           AND @rows_deleted * 10 > @active_before
            THROW 50624, 'fact.usp_load_fact_delivery_event: more than 10 percent of active events would be soft-deleted; investigate silver before using @allow_mass_delete = 1.', 1;
 
        BEGIN TRANSACTION;
 
        DELETE FROM fact.fact_delivery_event WHERE event_id IN (SELECT st.event_id FROM #stage AS st);
 
        INSERT INTO fact.fact_delivery_event
            (scheduled_date_key, actual_date_key, facility_key, route_key, delivery_status_key,
             event_id, load_id, trip_id, scheduled_datetime, actual_datetime,
             detention_minutes, billable_detention_minutes, arrival_variance_minutes, is_deleted_in_source)
        SELECT st.scheduled_date_key, st.actual_date_key, st.facility_key, st.route_key, st.delivery_status_key,
               st.event_id, st.load_id, st.trip_id, st.scheduled_datetime, st.actual_datetime,
               st.detention_minutes, st.billable_detention_minutes, st.arrival_variance_minutes, st.is_deleted_in_source
        FROM #stage AS st;
 
        UPDATE fact.fact_delivery_event SET is_deleted_in_source = 1
        WHERE is_deleted_in_source = 0
          AND event_id IN (SELECT k.event_id FROM #keys AS k)
          AND event_id NOT IN (SELECT st.event_id FROM #stage AS st);
 
        DELETE FROM stg.changed_delivery_event_keys WHERE run_id = @run_id;
 
        COMMIT TRANSACTION;
 
        EXEC log.usp_write_run_event @run_id, @step, @source, 'succeeded',
             @keys, @rows_inserted, @rows_updated, @rows_deleted, @rows_unknown,
             'fact_delivery_event published; checkpoint may now advance';
 
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