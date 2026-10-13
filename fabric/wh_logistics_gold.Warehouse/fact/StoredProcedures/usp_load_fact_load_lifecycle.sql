CREATE   PROCEDURE fact.usp_load_fact_load_lifecycle
    @run_id            VARCHAR(64),
    @allow_mass_delete BIT
AS
BEGIN
    SET NOCOUNT ON;
 
    DECLARE
        @step                  VARCHAR(128) = 'fact_load_lifecycle',
        @source                VARCHAR(128) = 'silver_loads,silver_trips,silver_delivery_events',
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
        @ambiguous             BIGINT = 0,
        @msg                   VARCHAR(4000);
 
    IF @run_id IS NULL OR @allow_mass_delete IS NULL
        THROW 50661, 'fact.usp_load_fact_load_lifecycle: @run_id and @allow_mass_delete are required.', 1;
 
    EXEC log.usp_write_run_event @run_id, @step, @source, 'started',
         NULL, NULL, NULL, NULL, NULL, NULL;
 
    BEGIN TRY
        /* All three silver dependencies are part of the fact contract. */
        IF (SELECT COUNT(DISTINCT b.source_table) FROM log.cdf_batch AS b
            WHERE b.run_id = @run_id AND b.step_name = @step
              AND b.source_table IN ('silver_loads', 'silver_trips', 'silver_delivery_events')) <> 3
            THROW 50662, 'fact.usp_load_fact_load_lifecycle: change detection must record silver_loads, silver_trips and silver_delivery_events for this run_id.', 1;
 
        DROP TABLE IF EXISTS #keys;
        CREATE TABLE #keys (load_id VARCHAR(20) NOT NULL) WITH (DISTRIBUTION = ROUND_ROBIN);
 
        INSERT INTO #keys (load_id)
        SELECT DISTINCT k.load_id FROM stg.changed_load_keys AS k
        WHERE k.run_id = @run_id AND k.load_id IS NOT NULL;
 
        SELECT @keys = COUNT(*) FROM #keys;
 
        /* Live trips of the changed loads. */
        DROP TABLE IF EXISTS #trips;
        CREATE TABLE #trips
        (
            load_id       VARCHAR(20) NOT NULL,
            trip_id       VARCHAR(20) NOT NULL,
            dispatch_date DATE        NULL,
            driver_id     VARCHAR(20) NULL,
            truck_id      VARCHAR(20) NULL,
            trailer_id    VARCHAR(20) NULL
        ) WITH (DISTRIBUTION = ROUND_ROBIN);
 
        INSERT INTO #trips (load_id, trip_id, dispatch_date, driver_id, truck_id, trailer_id)
        SELECT t.load_id, t.trip_id, t.dispatch_date, t.driver_id, t.truck_id, t.trailer_id
        FROM #keys AS k
        JOIN lh_logistics_silver.dbo.silver_trips AS t ON t.load_id = k.load_id
        WHERE t._is_deleted_in_source = 0;
 
        /* Live milestone events on those trips, for the same load. */
        DROP TABLE IF EXISTS #events;
        CREATE TABLE #events
        (
            load_id         VARCHAR(20)  NOT NULL,
            trip_id         VARCHAR(20)  NOT NULL,
            event_type      VARCHAR(20)  NOT NULL,
            actual_datetime DATETIME2(6) NULL
        ) WITH (DISTRIBUTION = ROUND_ROBIN);
 
        INSERT INTO #events (load_id, trip_id, event_type, actual_datetime)
        SELECT e.load_id, e.trip_id, e.event_type, e.actual_datetime
        FROM #trips AS tr
        JOIN lh_logistics_silver.dbo.silver_delivery_events AS e
          ON e.trip_id = tr.trip_id AND e.load_id = tr.load_id
        WHERE e._is_deleted_in_source = 0
          AND e.event_type IN ('Pickup', 'Delivery');
 
        SELECT @ambiguous = COUNT(*)
        FROM
        (
            SELECT ev.trip_id AS id
            FROM #events AS ev
            GROUP BY ev.trip_id, ev.event_type
            HAVING COUNT(*) > 1
            UNION ALL
            SELECT ev.load_id AS id
            FROM #events AS ev
            WHERE ev.event_type = 'Delivery' AND ev.actual_datetime IS NOT NULL
            GROUP BY ev.load_id
            HAVING COUNT(*) > 1
        ) AS a;
 
        IF @ambiguous > 0
            THROW 50665, 'fact.usp_load_fact_load_lifecycle: a trip has more than one live Pickup or Delivery event, or a load has more than one completed delivery; the milestone is ambiguous.', 1;
 
        /* One fulfilling trip per load: the delivering trip, else the latest. */
        DROP TABLE IF EXISTS #fulfilling;
        CREATE TABLE #fulfilling
        (
            load_id VARCHAR(20) NOT NULL,
            trip_id VARCHAR(20) NOT NULL
        ) WITH (DISTRIBUTION = ROUND_ROBIN);
 
        INSERT INTO #fulfilling (load_id, trip_id)
        SELECT r.load_id, r.trip_id
        FROM
        (
            SELECT tr.load_id, tr.trip_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY tr.load_id
                       ORDER BY CASE WHEN dl.trip_id IS NOT NULL THEN 0 ELSE 1 END,
                                tr.dispatch_date DESC,
                                tr.trip_id DESC) AS rn
            FROM #trips AS tr
            LEFT JOIN #events AS dl
              ON dl.trip_id = tr.trip_id
             AND dl.event_type = 'Delivery'
             AND dl.actual_datetime IS NOT NULL
        ) AS r
        WHERE r.rn = 1;
 
        DROP TABLE IF EXISTS #stage;
        CREATE TABLE #stage
        (
            booked_date_key           INT,
            dispatched_date_key       INT,
            pickup_date_key           INT,
            delivery_date_key         INT,
            customer_key              BIGINT,
            route_key                 BIGINT,
            load_type_key             BIGINT,
            fulfilling_driver_key     BIGINT,
            fulfilling_truck_key      BIGINT,
            fulfilling_trailer_key    BIGINT,
            load_id                   VARCHAR(20),
            revenue                   DECIMAL(14,2),
            fuel_surcharge            DECIMAL(14,2),
            accessorial_charges       DECIMAL(14,2),
            weight_lbs                INT,
            pieces                    INT,
            booking_to_dispatch_days  INT,
            dispatch_to_pickup_hours  DECIMAL(10,2),
            pickup_to_delivery_hours  DECIMAL(10,2),
            booking_to_delivery_hours DECIMAL(10,2),
            is_timestamp_reversed     BIT,
            is_deleted_in_source      BIT
        ) WITH (DISTRIBUTION = ROUND_ROBIN);
 
        INSERT INTO #stage
            (booked_date_key, dispatched_date_key, pickup_date_key, delivery_date_key,
             customer_key, route_key, load_type_key,
             fulfilling_driver_key, fulfilling_truck_key, fulfilling_trailer_key,
             load_id, revenue, fuel_surcharge, accessorial_charges, weight_lbs, pieces,
             booking_to_dispatch_days, dispatch_to_pickup_hours, pickup_to_delivery_hours,
             booking_to_delivery_hours, is_timestamp_reversed, is_deleted_in_source)
        SELECT
            CASE WHEN s.load_date     IS NULL THEN 0  WHEN bd.date_key IS NULL THEN -1 ELSE bd.date_key END,
            CASE WHEN s.trip_id       IS NULL THEN -2
                 WHEN s.dispatch_date IS NULL THEN 0  WHEN dd.date_key IS NULL THEN -1 ELSE dd.date_key END,
            CASE WHEN s.picked_at     IS NULL THEN -2 WHEN pd.date_key IS NULL THEN -1 ELSE pd.date_key END,
            CASE WHEN s.delivered_at  IS NULL THEN -2 WHEN vd.date_key IS NULL THEN -1 ELSE vd.date_key END,
            CASE WHEN s.customer_id   IS NULL THEN 0 ELSE COALESCE(cu.customer_key, -1) END,
            CASE WHEN s.route_id      IS NULL THEN 0 ELSE COALESCE(rt.route_key, -1) END,
            COALESCE(lt.load_type_key, -1),
            CASE WHEN s.trip_id IS NULL THEN -2 WHEN s.driver_id  IS NULL THEN 0 ELSE COALESCE(dr.driver_key, -1)  END,
            CASE WHEN s.trip_id IS NULL THEN -2 WHEN s.truck_id   IS NULL THEN 0 ELSE COALESCE(tk.truck_key, -1)   END,
            CASE WHEN s.trip_id IS NULL THEN -2 WHEN s.trailer_id IS NULL THEN 0 ELSE COALESCE(tl.trailer_key, -1) END,
            s.load_id,
            s.revenue, s.fuel_surcharge, s.accessorial_charges, s.weight_lbs, s.pieces,
            DATEDIFF(day, s.load_date, s.dispatch_date),
            CAST(ROUND(DATEDIFF_BIG(microsecond, CAST(s.dispatch_date AS DATETIME2(6)), s.picked_at)
                       / 3600000000.0, 2) AS DECIMAL(10,2)),
            CASE WHEN s.delivered_at < s.picked_at THEN NULL
                 ELSE CAST(ROUND(DATEDIFF_BIG(microsecond, s.picked_at, s.delivered_at)
                                 / 3600000000.0, 2) AS DECIMAL(10,2)) END,
            CAST(ROUND(DATEDIFF_BIG(microsecond, CAST(s.load_date AS DATETIME2(6)), s.delivered_at)
                       / 3600000000.0, 2) AS DECIMAL(10,2)),
            CAST(CASE WHEN s.delivered_at < s.picked_at THEN 1 ELSE 0 END AS BIT),
            CAST(CASE WHEN s._is_deleted_in_source = 1 THEN 1 ELSE 0 END AS BIT)
        FROM
        (
            SELECT l.load_id, l.customer_id, l.route_id, l.load_date,
                   l.revenue, l.fuel_surcharge, l.accessorial_charges, l.weight_lbs, l.pieces,
                   l._is_deleted_in_source,
                   v.booking_type, v.load_type,
                   tr.trip_id, tr.dispatch_date, tr.driver_id, tr.truck_id, tr.trailer_id,
                   pk.actual_datetime AS picked_at,
                   dv.actual_datetime AS delivered_at,
                   CASE WHEN l.load_date IS NULL THEN 0
                        ELSE YEAR(l.load_date) * 10000 + MONTH(l.load_date) * 100 + DAY(l.load_date) END AS booked_key,
                   CASE WHEN tr.dispatch_date IS NULL THEN 0
                        ELSE YEAR(tr.dispatch_date) * 10000 + MONTH(tr.dispatch_date) * 100 + DAY(tr.dispatch_date) END AS dispatch_key,
                   CASE WHEN pk.actual_datetime IS NULL THEN 0
                        ELSE YEAR(pk.actual_datetime) * 10000 + MONTH(pk.actual_datetime) * 100 + DAY(pk.actual_datetime) END AS picked_key,
                   CASE WHEN dv.actual_datetime IS NULL THEN 0
                        ELSE YEAR(dv.actual_datetime) * 10000 + MONTH(dv.actual_datetime) * 100 + DAY(dv.actual_datetime) END AS delivered_key
            FROM #keys AS k
            JOIN lh_logistics_silver.dbo.silver_loads AS l ON l.load_id = k.load_id
            LEFT JOIN dim.vw_src_load_type AS v ON v.load_id = l.load_id
            LEFT JOIN #fulfilling AS fu ON fu.load_id = l.load_id
            LEFT JOIN #trips  AS tr ON tr.trip_id = fu.trip_id
            LEFT JOIN #events AS pk ON pk.trip_id = fu.trip_id AND pk.event_type = 'Pickup'
            LEFT JOIN #events AS dv ON dv.trip_id = fu.trip_id AND dv.event_type = 'Delivery'
        ) AS s
        LEFT JOIN dim.dim_date AS bd ON bd.date_key = s.booked_key    AND bd.date_key > 0
        LEFT JOIN dim.dim_date AS dd ON dd.date_key = s.dispatch_key  AND dd.date_key > 0
        LEFT JOIN dim.dim_date AS pd ON pd.date_key = s.picked_key    AND pd.date_key > 0
        LEFT JOIN dim.dim_date AS vd ON vd.date_key = s.delivered_key AND vd.date_key > 0
        LEFT JOIN dim.dim_customer AS cu
          ON cu.customer_id = s.customer_id AND cu.customer_key > 0
         AND s.booked_key >= cu.valid_from_date_key AND s.booked_key < cu.valid_to_date_key
        LEFT JOIN dim.dim_route AS rt
          ON rt.route_id = s.route_id AND rt.route_key > 0
         AND s.booked_key >= rt.valid_from_date_key AND s.booked_key < rt.valid_to_date_key
        LEFT JOIN dim.dim_load_type AS lt
          ON lt.booking_type = s.booking_type AND lt.load_type = s.load_type
         AND lt.load_type_key > 0
        LEFT JOIN dim.dim_driver AS dr
          ON dr.driver_id = s.driver_id AND dr.driver_key > 0
         AND s.dispatch_key >= dr.valid_from_date_key AND s.dispatch_key < dr.valid_to_date_key
        LEFT JOIN dim.dim_truck AS tk
          ON tk.truck_id = s.truck_id AND tk.truck_key > 0
         AND s.dispatch_key >= tk.valid_from_date_key AND s.dispatch_key < tk.valid_to_date_key
        LEFT JOIN dim.dim_trailer AS tl
          ON tl.trailer_id = s.trailer_id AND tl.trailer_key > 0
         AND s.dispatch_key >= tl.valid_from_date_key AND s.dispatch_key < tl.valid_to_date_key;
 
        SELECT @rows_stage = COUNT(*), @rows_distinct = COUNT(DISTINCT st.load_id),
               @rows_unknown = COALESCE(SUM(CASE WHEN -1 IN (st.booked_date_key, st.dispatched_date_key,
                                   st.pickup_date_key, st.delivery_date_key, st.customer_key, st.route_key,
                                   st.load_type_key, st.fulfilling_driver_key, st.fulfilling_truck_key,
                                   st.fulfilling_trailer_key) THEN 1 ELSE 0 END), 0)
        FROM #stage AS st;
 
        IF @rows_stage <> @rows_distinct
            THROW 50663, 'fact.usp_load_fact_load_lifecycle: a load resolved to more than one dimension row.', 1;
 
        SELECT @rows_existing = COUNT(*) FROM fact.fact_load_lifecycle AS f
        JOIN #stage AS st ON st.load_id = f.load_id;
 
        SET @rows_inserted = @rows_stage - @rows_existing;
        SET @rows_updated  = @rows_existing;
 
        SELECT @active_before = COUNT(*) FROM fact.fact_load_lifecycle AS f WHERE f.is_deleted_in_source = 0;
 
        SELECT @rows_explicit_deleted = COUNT(*) FROM fact.fact_load_lifecycle AS f
        JOIN #stage AS st ON st.load_id = f.load_id
        WHERE f.is_deleted_in_source = 0 AND st.is_deleted_in_source = 1;
 
        SELECT @rows_missing_deleted = COUNT(*) FROM fact.fact_load_lifecycle AS f
        JOIN #keys AS k ON k.load_id = f.load_id
        LEFT JOIN #stage AS st ON st.load_id = f.load_id
        WHERE f.is_deleted_in_source = 0 AND st.load_id IS NULL;
 
        SET @rows_deleted = @rows_explicit_deleted + @rows_missing_deleted;
 
        IF @allow_mass_delete = 0 AND @rows_deleted > 0 AND @active_before > 0
           AND @rows_deleted * 10 > @active_before
            THROW 50664, 'fact.usp_load_fact_load_lifecycle: more than 10 percent of active loads would be soft-deleted; investigate silver before using @allow_mass_delete = 1.', 1;
 
        BEGIN TRANSACTION;
 
        DELETE FROM fact.fact_load_lifecycle WHERE load_id IN (SELECT st.load_id FROM #stage AS st);
 
        INSERT INTO fact.fact_load_lifecycle
            (booked_date_key, dispatched_date_key, pickup_date_key, delivery_date_key,
             customer_key, route_key, load_type_key,
             fulfilling_driver_key, fulfilling_truck_key, fulfilling_trailer_key,
             load_id, revenue, fuel_surcharge, accessorial_charges, weight_lbs, pieces,
             booking_to_dispatch_days, dispatch_to_pickup_hours, pickup_to_delivery_hours,
             booking_to_delivery_hours, is_timestamp_reversed, is_deleted_in_source)
        SELECT st.booked_date_key, st.dispatched_date_key, st.pickup_date_key, st.delivery_date_key,
               st.customer_key, st.route_key, st.load_type_key,
               st.fulfilling_driver_key, st.fulfilling_truck_key, st.fulfilling_trailer_key,
               st.load_id, st.revenue, st.fuel_surcharge, st.accessorial_charges, st.weight_lbs, st.pieces,
               st.booking_to_dispatch_days, st.dispatch_to_pickup_hours, st.pickup_to_delivery_hours,
               st.booking_to_delivery_hours, st.is_timestamp_reversed, st.is_deleted_in_source
        FROM #stage AS st;
 
        /* Gold-only keys are soft-deleted rather than physically removed. */
        UPDATE fact.fact_load_lifecycle SET is_deleted_in_source = 1
        WHERE is_deleted_in_source = 0
          AND load_id IN (SELECT k.load_id FROM #keys AS k)
          AND load_id NOT IN (SELECT st.load_id FROM #stage AS st);
 
        DELETE FROM stg.changed_load_keys WHERE run_id = @run_id;
 
        COMMIT TRANSACTION;
 
        EXEC log.usp_write_run_event @run_id, @step, @source, 'succeeded',
             @keys, @rows_inserted, @rows_updated, @rows_deleted, @rows_unknown,
             'fact_load_lifecycle published; checkpoint may now advance';
 
        DROP TABLE IF EXISTS #stage;
        DROP TABLE IF EXISTS #fulfilling;
        DROP TABLE IF EXISTS #events;
        DROP TABLE IF EXISTS #trips;
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