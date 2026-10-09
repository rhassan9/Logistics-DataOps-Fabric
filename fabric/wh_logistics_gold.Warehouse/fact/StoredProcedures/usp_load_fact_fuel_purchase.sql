/* ===========================================================================
   fact.usp_load_fact_fuel_purchase                        errors 5063x
   Silver dependencies: silver_fuel_purchases, silver_trips, silver_loads.
   route_key is resolved purchase -> trip -> load -> route. Neither trips
   nor loads are filtered on their deletion flags.
   Driver and truck (Type 2) are looked up at the purchase date.
   =========================================================================== */
 
CREATE   PROCEDURE fact.usp_load_fact_fuel_purchase
    @run_id            VARCHAR(64),
    @allow_mass_delete BIT
AS
BEGIN
    SET NOCOUNT ON;
 
    DECLARE
        @step                  VARCHAR(128) = 'fact_fuel_purchase',
        @source                VARCHAR(128) = 'silver_fuel_purchases,silver_trips,silver_loads',
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
        THROW 50631, 'fact.usp_load_fact_fuel_purchase: @run_id and @allow_mass_delete are required.', 1;
 
    EXEC log.usp_write_run_event @run_id, @step, @source, 'started',
         NULL, NULL, NULL, NULL, NULL, NULL;
 
    BEGIN TRY
        IF (SELECT COUNT(DISTINCT b.source_table) FROM log.cdf_batch AS b
            WHERE b.run_id = @run_id AND b.step_name = @step
              AND b.source_table IN ('silver_fuel_purchases', 'silver_trips', 'silver_loads')) <> 3
            THROW 50632, 'fact.usp_load_fact_fuel_purchase: change detection must record silver_fuel_purchases, silver_trips and silver_loads for this run_id.', 1;
 
        DROP TABLE IF EXISTS #keys;
        CREATE TABLE #keys (fuel_purchase_id VARCHAR(20) NOT NULL) WITH (DISTRIBUTION = ROUND_ROBIN);
 
        INSERT INTO #keys (fuel_purchase_id)
        SELECT DISTINCT k.fuel_purchase_id FROM stg.changed_fuel_purchase_keys AS k
        WHERE k.run_id = @run_id AND k.fuel_purchase_id IS NOT NULL;
 
        SELECT @keys = COUNT(*) FROM #keys;
 
        DROP TABLE IF EXISTS #stage;
        CREATE TABLE #stage
        (
            purchase_date_key     INT,
            truck_key             BIGINT,
            driver_key            BIGINT,
            route_key             BIGINT,
            location_key          BIGINT,
            fuel_purchase_id      VARCHAR(20),
            trip_id               VARCHAR(20),
            gallons               DECIMAL(10,2),
            total_cost            DECIMAL(14,2),
            is_capacity_exceeded  BIT,
            is_deleted_in_source  BIT
        ) WITH (DISTRIBUTION = ROUND_ROBIN);
 
        INSERT INTO #stage
            (purchase_date_key, truck_key, driver_key, route_key, location_key,
             fuel_purchase_id, trip_id, gallons, total_cost, is_capacity_exceeded, is_deleted_in_source)
        SELECT
            CASE WHEN s.purchase_date IS NULL THEN 0 WHEN dd.date_key IS NULL THEN -1 ELSE dd.date_key END,
            CASE WHEN s.truck_id  IS NULL THEN 0 ELSE COALESCE(tk.truck_key,  -1) END,
            CASE WHEN s.driver_id IS NULL THEN 0 ELSE COALESCE(dr.driver_key, -1) END,
            CASE WHEN s.trip_id    IS NULL THEN 0
                 WHEN s.trip_found IS NULL THEN -1
                 WHEN s.load_id    IS NULL THEN 0
                 WHEN s.load_found IS NULL THEN -1
                 WHEN s.route_id   IS NULL THEN 0
                 ELSE COALESCE(rt.route_key, -1) END,
            CASE WHEN s.location_city IS NULL THEN 0 ELSE COALESCE(lc.location_key, -1) END,
            s.fuel_purchase_id, s.trip_id, s.gallons, s.total_cost, s.is_capacity_exceeded,
            CAST(CASE WHEN s._is_deleted_in_source = 1 THEN 1 ELSE 0 END AS BIT)
        FROM
        (
            SELECT p.fuel_purchase_id, p.trip_id, p.truck_id, p.driver_id, p.purchase_date,
                   p.location_city, p.gallons, p.total_cost, p.is_capacity_exceeded,
                   p._is_deleted_in_source,
                   t.trip_id AS trip_found, t.load_id,
                   l.load_id AS load_found, l.route_id,
                   CASE WHEN p.purchase_date IS NULL THEN 0
                        ELSE YEAR(p.purchase_date) * 10000 + MONTH(p.purchase_date) * 100 + DAY(p.purchase_date) END AS event_key
            FROM #keys AS k
            JOIN lh_logistics_silver.dbo.silver_fuel_purchases AS p ON p.fuel_purchase_id = k.fuel_purchase_id
            LEFT JOIN lh_logistics_silver.dbo.silver_trips AS t ON t.trip_id = p.trip_id
            LEFT JOIN lh_logistics_silver.dbo.silver_loads AS l ON l.load_id = t.load_id
        ) AS s
        LEFT JOIN dim.dim_date AS dd ON dd.date_key = s.event_key AND dd.date_key > 0
        LEFT JOIN dim.dim_truck AS tk
          ON tk.truck_id = s.truck_id AND tk.truck_key > 0
         AND s.event_key >= tk.valid_from_date_key AND s.event_key < tk.valid_to_date_key
        LEFT JOIN dim.dim_driver AS dr
          ON dr.driver_id = s.driver_id AND dr.driver_key > 0
         AND s.event_key >= dr.valid_from_date_key AND s.event_key < dr.valid_to_date_key
        LEFT JOIN dim.dim_route AS rt
          ON rt.route_id = s.route_id AND rt.route_key > 0
         AND s.event_key >= rt.valid_from_date_key AND s.event_key < rt.valid_to_date_key
        LEFT JOIN dim.dim_location AS lc
          ON lc.location_city = s.location_city AND lc.location_key > 0;
 
        SELECT @rows_stage = COUNT(*), @rows_distinct = COUNT(DISTINCT st.fuel_purchase_id),
               @rows_unknown = COALESCE(SUM(CASE WHEN -1 IN (st.purchase_date_key, st.truck_key, st.driver_key,
                                   st.route_key, st.location_key) THEN 1 ELSE 0 END), 0)
        FROM #stage AS st;
 
        IF @rows_stage <> @rows_distinct
            THROW 50633, 'fact.usp_load_fact_fuel_purchase: a purchase resolved to more than one dimension row.', 1;
 
        SELECT @rows_existing = COUNT(*) FROM fact.fact_fuel_purchase AS f
        JOIN #stage AS st ON st.fuel_purchase_id = f.fuel_purchase_id;
 
        SET @rows_inserted = @rows_stage - @rows_existing;
        SET @rows_updated  = @rows_existing;
 
        SELECT @active_before = COUNT(*) FROM fact.fact_fuel_purchase AS f WHERE f.is_deleted_in_source = 0;
 
        SELECT @rows_explicit_deleted = COUNT(*) FROM fact.fact_fuel_purchase AS f
        JOIN #stage AS st ON st.fuel_purchase_id = f.fuel_purchase_id
        WHERE f.is_deleted_in_source = 0 AND st.is_deleted_in_source = 1;
 
        SELECT @rows_missing_deleted = COUNT(*) FROM fact.fact_fuel_purchase AS f
        JOIN #keys AS k ON k.fuel_purchase_id = f.fuel_purchase_id
        LEFT JOIN #stage AS st ON st.fuel_purchase_id = f.fuel_purchase_id
        WHERE f.is_deleted_in_source = 0 AND st.fuel_purchase_id IS NULL;
 
        SET @rows_deleted = @rows_explicit_deleted + @rows_missing_deleted;
 
        IF @allow_mass_delete = 0 AND @rows_deleted > 0 AND @active_before > 0
           AND @rows_deleted * 10 > @active_before
            THROW 50634, 'fact.usp_load_fact_fuel_purchase: more than 10 percent of active purchases would be soft-deleted; investigate silver before using @allow_mass_delete = 1.', 1;
 
        BEGIN TRANSACTION;
 
        DELETE FROM fact.fact_fuel_purchase WHERE fuel_purchase_id IN (SELECT st.fuel_purchase_id FROM #stage AS st);
 
        INSERT INTO fact.fact_fuel_purchase
            (purchase_date_key, truck_key, driver_key, route_key, location_key,
             fuel_purchase_id, trip_id, gallons, total_cost, is_capacity_exceeded, is_deleted_in_source)
        SELECT st.purchase_date_key, st.truck_key, st.driver_key, st.route_key, st.location_key,
               st.fuel_purchase_id, st.trip_id, st.gallons, st.total_cost, st.is_capacity_exceeded, st.is_deleted_in_source
        FROM #stage AS st;
 
        UPDATE fact.fact_fuel_purchase SET is_deleted_in_source = 1
        WHERE is_deleted_in_source = 0
          AND fuel_purchase_id IN (SELECT k.fuel_purchase_id FROM #keys AS k)
          AND fuel_purchase_id NOT IN (SELECT st.fuel_purchase_id FROM #stage AS st);
 
        DELETE FROM stg.changed_fuel_purchase_keys WHERE run_id = @run_id;
 
        COMMIT TRANSACTION;
 
        EXEC log.usp_write_run_event @run_id, @step, @source, 'succeeded',
             @keys, @rows_inserted, @rows_updated, @rows_deleted, @rows_unknown,
             'fact_fuel_purchase published; checkpoint may now advance';
 
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