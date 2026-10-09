/*
Gold fact_trip loader

Responsibilities:
  1. Validate that change detection produced both dependency batch records.
  2. Load the worklist for this run_id.
  3. Read Silver current state, including the explicit source-delete flag.
  4. Resolve date + SCD2 dimension keys using dispatch_date.
  5. Rebuild staged rows for changed keys, healing prior Unknown keys and
     keys that point at the wrong SCD2 version.
  6. Soft-delete explicitly deleted Silver rows.
  7. Soft-delete Gold-only keys as a defensive hard-delete fallback.
  8. Guard against an accidental >10% active-row deletion.
  9. Consume the staging worklist transactionally with the fact write.
 10. Log success only after COMMIT, so the CDF checkpoint advances only after
     the Gold publication succeeded.

Important semantic decision:
  silver_trips._is_deleted_in_source controls the fact deletion state.
  silver_loads is NOT filtered on its delete flag. A live trip can still use
  the retained load row to preserve the route/last-known load attributes.
*/

CREATE   PROCEDURE fact.usp_load_fact_trip
    @run_id            VARCHAR(64),
    @allow_mass_delete BIT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @step                    VARCHAR(128) = 'fact_trip',
        @source                  VARCHAR(128) = 'silver_trips,silver_loads',
        @keys                    BIGINT = 0,
        @rows_read               BIGINT = 0,
        @rows_distinct           BIGINT = 0,
        @rows_existing           BIGINT = 0,
        @rows_inserted           BIGINT = 0,
        @rows_updated            BIGINT = 0,
        @rows_deleted            BIGINT = 0,
        @rows_unknown            BIGINT = 0,
        @rows_explicit_deleted   BIGINT = 0,
        @rows_missing_deleted    BIGINT = 0,
        @active_before           BIGINT = 0,
        @msg                     VARCHAR(4000);

    IF @run_id IS NULL OR @allow_mass_delete IS NULL
        THROW 50611,
              'fact.usp_load_fact_trip: @run_id and @allow_mass_delete are required.',
              1;

    EXEC log.usp_write_run_event
         @run_id, @step, @source, 'started',
         NULL, NULL, NULL, NULL, NULL, NULL;

    BEGIN TRY
        /* Both Silver dependencies are part of the fact contract. */
        IF
        (
            SELECT COUNT(DISTINCT b.source_table)
            FROM log.cdf_batch AS b
            WHERE b.run_id = @run_id
              AND b.step_name = @step
              AND b.source_table IN ('silver_trips', 'silver_loads')
        ) <> 2
        BEGIN
            THROW 50612,
                  'fact.usp_load_fact_trip: change detection must record both silver_trips and silver_loads for this run_id.',
                  1;
        END;

        DROP TABLE IF EXISTS #trip_keys;
        CREATE TABLE #trip_keys
        (
            trip_id VARCHAR(20) NOT NULL
        ) WITH (DISTRIBUTION = ROUND_ROBIN);

        INSERT INTO #trip_keys (trip_id)
        SELECT DISTINCT k.trip_id
        FROM stg.changed_trip_keys AS k
        WHERE k.run_id = @run_id
          AND k.trip_id IS NOT NULL;

        SELECT @keys = COUNT(*) FROM #trip_keys;
        SET @rows_read = @keys;

        DROP TABLE IF EXISTS #trip_stage;
        CREATE TABLE #trip_stage
        (
            dispatch_date_key      INT,
            driver_key             BIGINT,
            truck_key              BIGINT,
            trailer_key            BIGINT,
            route_key              BIGINT,
            trip_id                VARCHAR(20),
            load_id                VARCHAR(20),
            actual_distance_miles  INT,
            actual_duration_hours  DECIMAL(10,2),
            fuel_gallons_used      DECIMAL(12,2),
            idle_time_hours        DECIMAL(10,2),
            is_idle_implausible    BIT,
            is_deleted_in_source   BIT
        ) WITH (DISTRIBUTION = ROUND_ROBIN);

        /*
        Read current Silver state without filtering deleted trips away.
        That flag is a real state on the fact row and must reach Gold.
        */
        INSERT INTO #trip_stage
        (
            dispatch_date_key,
            driver_key,
            truck_key,
            trailer_key,
            route_key,
            trip_id,
            load_id,
            actual_distance_miles,
            actual_duration_hours,
            fuel_gallons_used,
            idle_time_hours,
            is_idle_implausible,
            is_deleted_in_source
        )
        SELECT
            CASE
                WHEN t.dispatch_date IS NULL THEN 0
                WHEN dd.date_key IS NULL THEN -1
                ELSE dd.date_key
            END AS dispatch_date_key,
            CASE
                WHEN t.driver_id IS NULL THEN 0
                ELSE COALESCE(dr.driver_key, -1)
            END AS driver_key,
            CASE
                WHEN t.truck_id IS NULL THEN 0
                ELSE COALESCE(tk.truck_key, -1)
            END AS truck_key,
            CASE
                WHEN t.trailer_id IS NULL THEN 0
                ELSE COALESCE(tl.trailer_key, -1)
            END AS trailer_key,
            CASE
                WHEN t.load_id  IS NULL THEN 0    -- trip records no load: Missing
                WHEN l.load_id  IS NULL THEN -1   -- load_id fails its lookup: Unknown
                WHEN l.route_id IS NULL THEN 0    -- load records no route: Missing
                ELSE COALESCE(rt.route_key, -1)
            END AS route_key,
            t.trip_id,
            t.load_id,
            t.actual_distance_miles,
            t.actual_duration_hours,
            t.fuel_gallons_used,
            t.idle_time_hours,
            t.is_idle_implausible,
            CAST(CASE WHEN t._is_deleted_in_source = 1 THEN 1 ELSE 0 END AS BIT)
                AS is_deleted_in_source
        FROM #trip_keys AS ck
        INNER JOIN lh_logistics_silver.dbo.silver_trips AS t
            ON t.trip_id = ck.trip_id
        LEFT JOIN lh_logistics_silver.dbo.silver_loads AS l
            ON l.load_id = t.load_id
        LEFT JOIN dim.dim_date AS dd
            ON dd.date_key =
               CASE WHEN t.dispatch_date IS NULL THEN 0
                    ELSE YEAR(t.dispatch_date) * 10000
                       + MONTH(t.dispatch_date) * 100
                       + DAY(t.dispatch_date)
               END
           AND dd.date_key > 0
        LEFT JOIN dim.dim_driver AS dr
            ON dr.driver_id = t.driver_id
           AND dr.driver_key > 0
           AND (
                CASE WHEN t.dispatch_date IS NULL THEN 0
                     ELSE YEAR(t.dispatch_date) * 10000
                        + MONTH(t.dispatch_date) * 100
                        + DAY(t.dispatch_date)
                END
               ) >= dr.valid_from_date_key
           AND (
                CASE WHEN t.dispatch_date IS NULL THEN 0
                     ELSE YEAR(t.dispatch_date) * 10000
                        + MONTH(t.dispatch_date) * 100
                        + DAY(t.dispatch_date)
                END
               ) < dr.valid_to_date_key
        LEFT JOIN dim.dim_truck AS tk
            ON tk.truck_id = t.truck_id
           AND tk.truck_key > 0
           AND (
                CASE WHEN t.dispatch_date IS NULL THEN 0
                     ELSE YEAR(t.dispatch_date) * 10000
                        + MONTH(t.dispatch_date) * 100
                        + DAY(t.dispatch_date)
                END
               ) >= tk.valid_from_date_key
           AND (
                CASE WHEN t.dispatch_date IS NULL THEN 0
                     ELSE YEAR(t.dispatch_date) * 10000
                        + MONTH(t.dispatch_date) * 100
                        + DAY(t.dispatch_date)
                END
               ) < tk.valid_to_date_key
        LEFT JOIN dim.dim_trailer AS tl
            ON tl.trailer_id = t.trailer_id
           AND tl.trailer_key > 0
           AND (
                CASE WHEN t.dispatch_date IS NULL THEN 0
                     ELSE YEAR(t.dispatch_date) * 10000
                        + MONTH(t.dispatch_date) * 100
                        + DAY(t.dispatch_date)
                END
               ) >= tl.valid_from_date_key
           AND (
                CASE WHEN t.dispatch_date IS NULL THEN 0
                     ELSE YEAR(t.dispatch_date) * 10000
                        + MONTH(t.dispatch_date) * 100
                        + DAY(t.dispatch_date)
                END
               ) < tl.valid_to_date_key
        LEFT JOIN dim.dim_route AS rt
            ON rt.route_id = l.route_id
           AND rt.route_key > 0
           AND (
                CASE WHEN t.dispatch_date IS NULL THEN 0
                     ELSE YEAR(t.dispatch_date) * 10000
                        + MONTH(t.dispatch_date) * 100
                        + DAY(t.dispatch_date)
                END
               ) >= rt.valid_from_date_key
           AND (
                CASE WHEN t.dispatch_date IS NULL THEN 0
                     ELSE YEAR(t.dispatch_date) * 10000
                        + MONTH(t.dispatch_date) * 100
                        + DAY(t.dispatch_date)
                END
               ) < rt.valid_to_date_key;

        SELECT
            @rows_distinct = COUNT(DISTINCT st.trip_id),
            @rows_unknown = COALESCE(
                SUM(
                    CASE
                        WHEN st.dispatch_date_key = -1
                          OR st.driver_key = -1
                          OR st.truck_key = -1
                          OR st.trailer_key = -1
                          OR st.route_key = -1
                        THEN 1 ELSE 0
                    END
                ), 0
            )
        FROM #trip_stage AS st;

        IF (SELECT COUNT(*) FROM #trip_stage) <> @rows_distinct
            THROW 50613,
                  'fact.usp_load_fact_trip: a trip resolved to more than one dimension version.',
                  1;

        SELECT @rows_existing = COUNT(*)
        FROM fact.fact_trip AS f
        INNER JOIN #trip_stage AS st
            ON st.trip_id = f.trip_id;

        SET @rows_inserted = (SELECT COUNT(*) FROM #trip_stage) - @rows_existing;
        SET @rows_updated = @rows_existing;

        SELECT @active_before = COUNT(*)
        FROM fact.fact_trip AS f
        WHERE f.is_deleted_in_source = 0;

        /* Explicit Silver soft deletes. */
        SELECT @rows_explicit_deleted = COUNT(*)
        FROM fact.fact_trip AS f
        INNER JOIN #trip_stage AS st
            ON st.trip_id = f.trip_id
        WHERE f.is_deleted_in_source = 0
          AND st.is_deleted_in_source = 1;

        /* Defensive fallback for an unexpected physical absence from Silver. */
        SELECT @rows_missing_deleted = COUNT(*)
        FROM fact.fact_trip AS f
        INNER JOIN #trip_keys AS ck
            ON ck.trip_id = f.trip_id
        LEFT JOIN #trip_stage AS st
            ON st.trip_id = f.trip_id
        WHERE f.is_deleted_in_source = 0
          AND st.trip_id IS NULL;

        SET @rows_deleted = @rows_explicit_deleted + @rows_missing_deleted;

        IF @allow_mass_delete = 0
           AND @rows_deleted > 0
           AND @active_before > 0
           AND (@rows_deleted * 10 > @active_before)
        BEGIN
            THROW 50614,
                  'fact.usp_load_fact_trip: more than 10 percent of active trips would be soft-deleted; investigate Silver before using @allow_mass_delete = 1.',
                  1;
        END;

        BEGIN TRANSACTION;

        /* Rebuild each changed/current row. This also heals Unknown keys and restores
           rows whose Silver delete flag returned to 0. */
        DELETE FROM fact.fact_trip
        WHERE trip_id IN (SELECT trip_id FROM #trip_stage);

        INSERT INTO fact.fact_trip
        (
            dispatch_date_key,
            driver_key,
            truck_key,
            trailer_key,
            route_key,
            trip_id,
            load_id,
            actual_distance_miles,
            actual_duration_hours,
            fuel_gallons_used,
            idle_time_hours,
            is_idle_implausible,
            is_deleted_in_source
        )
        SELECT
            st.dispatch_date_key,
            st.driver_key,
            st.truck_key,
            st.trailer_key,
            st.route_key,
            st.trip_id,
            st.load_id,
            st.actual_distance_miles,
            st.actual_duration_hours,
            st.fuel_gallons_used,
            st.idle_time_hours,
            st.is_idle_implausible,
            st.is_deleted_in_source
        FROM #trip_stage AS st;

        /* Gold-only keys are soft-deleted rather than physically removed. */
        UPDATE fact.fact_trip
        SET is_deleted_in_source = 1
        WHERE is_deleted_in_source = 0
          AND trip_id IN (SELECT ck.trip_id FROM #trip_keys AS ck)
          AND trip_id NOT IN (SELECT st.trip_id FROM #trip_stage AS st);

        /* Consume work only after fact publication is part of the same transaction. */
        DELETE FROM stg.changed_trip_keys
        WHERE run_id = @run_id;

        COMMIT TRANSACTION;

        EXEC log.usp_write_run_event
             @run_id, @step, @source, 'succeeded',
             @rows_read, @rows_inserted, @rows_updated,
             @rows_deleted, @rows_unknown,
             'fact_trip published; checkpoint may now advance';

        DROP TABLE IF EXISTS #trip_stage;
        DROP TABLE IF EXISTS #trip_keys;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        SET @msg = LEFT(ERROR_MESSAGE(), 4000);

        EXEC log.usp_write_run_event
             @run_id, @step, @source, 'failed',
             NULL, NULL, NULL, NULL, NULL, @msg;

        THROW;
    END CATCH;
END;

GO