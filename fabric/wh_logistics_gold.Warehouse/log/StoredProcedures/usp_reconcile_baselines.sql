CREATE   PROCEDURE log.usp_reconcile_baselines
    @run_id VARCHAR(64)
AS
BEGIN
    SET NOCOUNT ON;
 
    DECLARE
        @step       VARCHAR(128) = 'reconcile_baselines',
        @source     VARCHAR(128) = 'validation_driver_monthly_metrics,validation_truck_utilization_metrics',
        @checked_at DATETIME2(6) = SYSUTCDATETIME(),
        @compared   BIGINT = 0,
        @failed     INT = 0,
        @unknown    INT = 0,
        @mismatch   INT = 0,
        @msg        VARCHAR(4000);
 
    IF @run_id IS NULL
        THROW 50801, 'log.usp_reconcile_baselines: @run_id is required.', 1;
 
    EXEC log.usp_write_run_event @run_id, @step, @source, 'started',
         NULL, NULL, NULL, NULL, NULL, NULL;
 
    BEGIN TRY
        /* Live trips with their resources' business ids and dispatch month. */
        DROP TABLE IF EXISTS #trip;
        CREATE TABLE #trip
        (
            trip_id     VARCHAR(20)   NOT NULL,
            driver_id   VARCHAR(20)   NULL,
            truck_id    VARCHAR(20)   NULL,
            month_start DATE          NOT NULL,
            miles       INT           NULL,
            gallons     DECIMAL(12,2) NULL,
            idle_hours  DECIMAL(10,2) NULL
        ) WITH (DISTRIBUTION = ROUND_ROBIN);
 
        INSERT INTO #trip (trip_id, driver_id, truck_id, month_start, miles, gallons, idle_hours)
        SELECT f.trip_id, dr.driver_id, tk.truck_id,
               DATEFROMPARTS(f.dispatch_date_key / 10000, (f.dispatch_date_key / 100) % 100, 1),
               f.actual_distance_miles, f.fuel_gallons_used, f.idle_time_hours
        FROM fact.fact_trip AS f
        LEFT JOIN dim.dim_driver AS dr ON dr.driver_key = f.driver_key AND f.driver_key > 0
        LEFT JOIN dim.dim_truck  AS tk ON tk.truck_key  = f.truck_key  AND f.truck_key  > 0
        WHERE f.is_deleted_in_source = 0
          AND f.dispatch_date_key > 0;
 
        /* Revenue by fulfilling resource and dispatched month. */
        DROP TABLE IF EXISTS #revenue;
        CREATE TABLE #revenue
        (
            resource_type VARCHAR(10)   NOT NULL,
            resource_id   VARCHAR(20)   NOT NULL,
            month_start   DATE          NOT NULL,
            revenue       DECIMAL(18,2) NULL
        ) WITH (DISTRIBUTION = ROUND_ROBIN);
 
        INSERT INTO #revenue (resource_type, resource_id, month_start, revenue)
        SELECT 'driver', dr.driver_id,
               DATEFROMPARTS(f.dispatched_date_key / 10000, (f.dispatched_date_key / 100) % 100, 1),
               SUM(f.revenue)
        FROM fact.fact_load_lifecycle AS f
        JOIN dim.dim_driver AS dr ON dr.driver_key = f.fulfilling_driver_key
        WHERE f.is_deleted_in_source = 0 AND f.fulfilling_driver_key > 0 AND f.dispatched_date_key > 0
        GROUP BY dr.driver_id,
                 DATEFROMPARTS(f.dispatched_date_key / 10000, (f.dispatched_date_key / 100) % 100, 1)
        UNION ALL
        SELECT 'truck', tk.truck_id,
               DATEFROMPARTS(f.dispatched_date_key / 10000, (f.dispatched_date_key / 100) % 100, 1),
               SUM(f.revenue)
        FROM fact.fact_load_lifecycle AS f
        JOIN dim.dim_truck AS tk ON tk.truck_key = f.fulfilling_truck_key
        WHERE f.is_deleted_in_source = 0 AND f.fulfilling_truck_key > 0 AND f.dispatched_date_key > 0
        GROUP BY tk.truck_id,
                 DATEFROMPARTS(f.dispatched_date_key / 10000, (f.dispatched_date_key / 100) % 100, 1);
 
        /* Delivery punctuality on each driver's trips. */
        DROP TABLE IF EXISTS #on_time;
        CREATE TABLE #on_time
        (
            driver_id   VARCHAR(20) NOT NULL,
            month_start DATE        NOT NULL,
            deliveries  INT         NOT NULL,
            on_time     INT         NOT NULL
        ) WITH (DISTRIBUTION = ROUND_ROBIN);
 
        INSERT INTO #on_time (driver_id, month_start, deliveries, on_time)
        SELECT t.driver_id, t.month_start, COUNT(*),
               SUM(CASE WHEN ds.arrival_status = 'On Time' THEN 1 ELSE 0 END)
        FROM fact.fact_delivery_event AS e
        JOIN dim.dim_delivery_status AS ds
          ON ds.delivery_status_key = e.delivery_status_key AND ds.event_type = 'Delivery'
        JOIN #trip AS t ON t.trip_id = e.trip_id
        WHERE e.is_deleted_in_source = 0 AND t.driver_id IS NOT NULL
        GROUP BY t.driver_id, t.month_start;
 
        /* Maintenance by truck and maintenance month. */
        DROP TABLE IF EXISTS #maintenance;
        CREATE TABLE #maintenance
        (
            truck_id    VARCHAR(20)   NOT NULL,
            month_start DATE          NOT NULL,
            events      INT           NOT NULL,
            cost        DECIMAL(18,2) NULL,
            downtime    DECIMAL(18,2) NULL
        ) WITH (DISTRIBUTION = ROUND_ROBIN);
 
        INSERT INTO #maintenance (truck_id, month_start, events, cost, downtime)
        SELECT tk.truck_id,
               DATEFROMPARTS(m.maintenance_date_key / 10000, (m.maintenance_date_key / 100) % 100, 1),
               COUNT(*), SUM(m.labor_cost + m.parts_cost), SUM(m.downtime_hours)
        FROM fact.fact_maintenance AS m
        JOIN dim.dim_truck AS tk ON tk.truck_key = m.truck_key
        WHERE m.is_deleted_in_source = 0 AND m.truck_key > 0 AND m.maintenance_date_key > 0
        GROUP BY tk.truck_id,
                 DATEFROMPARTS(m.maintenance_date_key / 10000, (m.maintenance_date_key / 100) % 100, 1);
 
        /* Gold driver-months, joined to the baseline in both directions. */
        DROP TABLE IF EXISTS #driver;
        CREATE TABLE #driver
        (
            in_baseline      BIT,
            in_gold          BIT,
            b_trips          INT,
            b_miles          INT,
            b_revenue        DECIMAL(14,2),
            b_mpg            DECIMAL(8,2),
            b_gallons        DECIMAL(12,2),
            b_on_time_rate   DECIMAL(6,4),
            b_idle           DECIMAL(8,2),
            g_trips          INT,
            g_miles          BIGINT,
            g_revenue        DECIMAL(18,2),
            g_gallons        DECIMAL(18,2),
            g_idle           DECIMAL(18,6),
            g_deliveries     INT,
            g_on_time        INT
        ) WITH (DISTRIBUTION = ROUND_ROBIN);
 
        INSERT INTO #driver
        SELECT CASE WHEN b.driver_id IS NULL THEN 0 ELSE 1 END,
               CASE WHEN g.driver_id IS NULL THEN 0 ELSE 1 END,
               b.trips_completed, b.total_miles, b.total_revenue, b.average_mpg,
               b.total_fuel_gallons, b.on_time_delivery_rate, b.average_idle_hours,
               g.trips, g.miles, r.revenue, g.gallons, g.idle, o.deliveries, o.on_time
        FROM
        (
            SELECT t.driver_id, t.month_start, COUNT(*) AS trips, SUM(CAST(t.miles AS BIGINT)) AS miles,
                   SUM(t.gallons) AS gallons, AVG(CAST(t.idle_hours AS DECIMAL(18,6))) AS idle
            FROM #trip AS t
            WHERE t.driver_id IS NOT NULL
            GROUP BY t.driver_id, t.month_start
        ) AS g
        FULL OUTER JOIN lh_logistics_ops.dbo.validation_driver_monthly_metrics AS b
          ON b.driver_id = g.driver_id AND b.month = g.month_start
        LEFT JOIN #revenue AS r
          ON r.resource_type = 'driver' AND r.resource_id = g.driver_id AND r.month_start = g.month_start
        LEFT JOIN #on_time AS o
          ON o.driver_id = g.driver_id AND o.month_start = g.month_start;
 
        /* Gold truck-months, joined to the baseline in both directions. */
        DROP TABLE IF EXISTS #truck;
        CREATE TABLE #truck
        (
            in_baseline   BIT,
            in_gold       BIT,
            b_trips       INT,
            b_miles       INT,
            b_revenue     DECIMAL(14,2),
            b_mpg         DECIMAL(8,2),
            b_events      INT,
            b_cost        DECIMAL(14,2),
            b_downtime    DECIMAL(10,2),
            g_trips       INT,
            g_miles       BIGINT,
            g_revenue     DECIMAL(18,2),
            g_gallons     DECIMAL(18,2),
            g_events      INT,
            g_cost        DECIMAL(18,2),
            g_downtime    DECIMAL(18,2)
        ) WITH (DISTRIBUTION = ROUND_ROBIN);
 
        INSERT INTO #truck
        SELECT CASE WHEN b.truck_id IS NULL THEN 0 ELSE 1 END,
               CASE WHEN g.truck_id IS NULL THEN 0 ELSE 1 END,
               b.trips_completed, b.total_miles, b.total_revenue, b.average_mpg,
               b.maintenance_events, b.maintenance_cost, b.downtime_hours,
               g.trips, g.miles, r.revenue, g.gallons,
               COALESCE(m.events, 0), COALESCE(m.cost, 0), COALESCE(m.downtime, 0)
        FROM
        (
            SELECT t.truck_id, t.month_start, COUNT(*) AS trips, SUM(CAST(t.miles AS BIGINT)) AS miles,
                   SUM(t.gallons) AS gallons
            FROM #trip AS t
            WHERE t.truck_id IS NOT NULL
            GROUP BY t.truck_id, t.month_start
        ) AS g
        FULL OUTER JOIN lh_logistics_ops.dbo.validation_truck_utilization_metrics AS b
          ON b.truck_id = g.truck_id AND b.month = g.month_start
        LEFT JOIN #revenue AS r
          ON r.resource_type = 'truck' AND r.resource_id = g.truck_id AND r.month_start = g.month_start
        LEFT JOIN #maintenance AS m
          ON m.truck_id = g.truck_id AND m.month_start = g.month_start;
 
        /* One row per resource-month and metric, gold rounded to the baseline scale. */
        DROP TABLE IF EXISTS #pairs;
        CREATE TABLE #pairs
        (
            baseline_name  VARCHAR(64),
            metric_name    VARCHAR(64),
            is_verified    BIT,
            in_baseline    BIT,
            in_gold        BIT,
            baseline_value DECIMAL(18,4),
            gold_value     DECIMAL(18,4)
        ) WITH (DISTRIBUTION = ROUND_ROBIN);
 
        INSERT INTO #pairs
        SELECT 'driver_monthly_metrics', 'trips_completed', 1, d.in_baseline, d.in_gold,
               d.b_trips, d.g_trips FROM #driver AS d
        UNION ALL
        SELECT 'driver_monthly_metrics', 'total_miles', 1, d.in_baseline, d.in_gold,
               d.b_miles, d.g_miles FROM #driver AS d
        UNION ALL
        SELECT 'driver_monthly_metrics', 'total_revenue', 1, d.in_baseline, d.in_gold,
               d.b_revenue, ROUND(d.g_revenue, 2) FROM #driver AS d
        UNION ALL
        SELECT 'driver_monthly_metrics', 'total_fuel_gallons', 1, d.in_baseline, d.in_gold,
               d.b_gallons, ROUND(d.g_gallons, 2) FROM #driver AS d
        UNION ALL
        SELECT 'driver_monthly_metrics', 'average_mpg', 0, d.in_baseline, d.in_gold,
               d.b_mpg, ROUND(CAST(d.g_miles AS DECIMAL(18,6)) / NULLIF(d.g_gallons, 0), 2) FROM #driver AS d
        UNION ALL
        SELECT 'driver_monthly_metrics', 'average_idle_hours', 0, d.in_baseline, d.in_gold,
               d.b_idle, ROUND(d.g_idle, 1) FROM #driver AS d
        UNION ALL
        SELECT 'driver_monthly_metrics', 'on_time_delivery_rate', 0, d.in_baseline, d.in_gold,
               d.b_on_time_rate,
               ROUND(CAST(d.g_on_time AS DECIMAL(18,6)) / NULLIF(d.g_deliveries, 0), 3) FROM #driver AS d
        UNION ALL
        SELECT 'truck_utilization_metrics', 'trips_completed', 1, t.in_baseline, t.in_gold,
               t.b_trips, t.g_trips FROM #truck AS t
        UNION ALL
        SELECT 'truck_utilization_metrics', 'total_miles', 1, t.in_baseline, t.in_gold,
               t.b_miles, t.g_miles FROM #truck AS t
        UNION ALL
        SELECT 'truck_utilization_metrics', 'total_revenue', 1, t.in_baseline, t.in_gold,
               t.b_revenue, ROUND(t.g_revenue, 2) FROM #truck AS t
        UNION ALL
        SELECT 'truck_utilization_metrics', 'average_mpg', 0, t.in_baseline, t.in_gold,
               t.b_mpg, ROUND(CAST(t.g_miles AS DECIMAL(18,6)) / NULLIF(t.g_gallons, 0), 2) FROM #truck AS t
        UNION ALL
        SELECT 'truck_utilization_metrics', 'maintenance_events', 1, t.in_baseline, t.in_gold,
               t.b_events, t.g_events FROM #truck AS t
        UNION ALL
        SELECT 'truck_utilization_metrics', 'maintenance_cost', 1, t.in_baseline, t.in_gold,
               t.b_cost, ROUND(t.g_cost, 2) FROM #truck AS t
        UNION ALL
        SELECT 'truck_utilization_metrics', 'downtime_hours', 1, t.in_baseline, t.in_gold,
               t.b_downtime, ROUND(t.g_downtime, 2) FROM #truck AS t;
 
        SELECT @compared = COUNT(*) FROM #pairs AS p WHERE p.in_baseline = 1 AND p.in_gold = 1;
 
        /* One attempt's results commit together, so a retried run never leaves a
           partial attempt behind. log.vw_reconciliation_latest reads the newest one. */
        BEGIN TRANSACTION;
 
        /* A metric passes only with every row present on both sides and equal. */
        INSERT INTO log.reconciliation_result
            (run_id, checked_at, baseline_name, metric_name, is_verified, rows_compared,
             rows_matched, baseline_only, gold_only, max_abs_difference, status)
        SELECT @run_id, @checked_at, s.baseline_name, s.metric_name, s.is_verified,
               s.rows_compared, s.rows_matched, s.baseline_only, s.gold_only, s.max_abs_difference,
               CASE WHEN s.rows_compared = s.rows_matched AND s.baseline_only = 0 AND s.gold_only = 0
                    THEN CASE WHEN s.is_verified = 1 THEN 'pass' ELSE 'match' END
                    /* An unverified metric with a stated definition that does not reproduce
                       every row is a mismatch: reported, never failing. A definition is
                       unknown only where no stated definition reproduces the baseline. */
                    ELSE CASE WHEN s.is_verified = 1 THEN 'fail'
                              WHEN s.metric_name = 'average_mpg' THEN 'definition-unknown'
                              ELSE 'mismatch' END
               END
        FROM
        (
            SELECT p.baseline_name, p.metric_name, p.is_verified,
                   SUM(CASE WHEN p.in_baseline = 1 AND p.in_gold = 1 THEN 1 ELSE 0 END) AS rows_compared,
                   SUM(CASE WHEN p.in_baseline = 1 AND p.in_gold = 1
                             AND (p.baseline_value = p.gold_value
                                  OR (p.baseline_value IS NULL AND p.gold_value IS NULL))
                            THEN 1 ELSE 0 END) AS rows_matched,
                   SUM(CASE WHEN p.in_gold = 0 THEN 1 ELSE 0 END) AS baseline_only,
                   SUM(CASE WHEN p.in_baseline = 0 THEN 1 ELSE 0 END) AS gold_only,
                   MAX(CASE WHEN p.in_baseline = 1 AND p.in_gold = 1
                            THEN ABS(p.baseline_value - p.gold_value) END) AS max_abs_difference
            FROM #pairs AS p
            GROUP BY p.baseline_name, p.metric_name, p.is_verified
        ) AS s;
 
        INSERT INTO log.reconciliation_result
            (run_id, checked_at, baseline_name, metric_name, is_verified, rows_compared,
             rows_matched, baseline_only, gold_only, max_abs_difference, status)
        VALUES (@run_id, @checked_at, 'truck_utilization_metrics', 'utilization_rate', 0,
                NULL, NULL, NULL, NULL, NULL, 'definition-unknown');
 
        COMMIT TRANSACTION;
 
        SELECT @failed  = SUM(CASE WHEN r.status = 'fail' THEN 1 ELSE 0 END),
               @unknown = SUM(CASE WHEN r.status = 'definition-unknown' THEN 1 ELSE 0 END),
               @mismatch = SUM(CASE WHEN r.status = 'mismatch' THEN 1 ELSE 0 END)
        FROM log.reconciliation_result AS r
        WHERE r.run_id = @run_id AND r.checked_at = @checked_at;
 
        IF @failed > 0
        BEGIN
            SELECT @msg = 'log.usp_reconcile_baselines: verified metric mismatch: '
                        + STRING_AGG(CAST(r.baseline_name + '.' + r.metric_name AS VARCHAR(4000)), ', ')
            FROM log.reconciliation_result AS r
            WHERE r.run_id = @run_id AND r.checked_at = @checked_at AND r.status = 'fail';
 
            THROW 50802, @msg, 1;
        END;
 
        SET @msg = 'baselines reconciled; verified metrics pass; '
                 + CAST(@mismatch AS VARCHAR(10)) + ' unverified metrics mismatch; '
                 + CAST(@unknown AS VARCHAR(10)) + ' metrics definition-unknown';
 
        EXEC log.usp_write_run_event @run_id, @step, @source, 'succeeded',
             @compared, NULL, NULL, NULL, NULL, @msg;
 
        DROP TABLE IF EXISTS #pairs;
        DROP TABLE IF EXISTS #truck;
        DROP TABLE IF EXISTS #driver;
        DROP TABLE IF EXISTS #maintenance;
        DROP TABLE IF EXISTS #on_time;
        DROP TABLE IF EXISTS #revenue;
        DROP TABLE IF EXISTS #trip;
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