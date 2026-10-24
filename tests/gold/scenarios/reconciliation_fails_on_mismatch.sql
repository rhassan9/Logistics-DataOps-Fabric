/* Scenario: reconciliation stops the run when gold drifts from a verified baseline.

   One trip's miles change inside a transaction that is always rolled back, so
   nothing persists. The procedure must raise 50802 naming the two total_miles
   metrics.

   Run in: wh_logistics_gold. Safe to run anywhere: every change is rolled back.
   Pass:   error_number = 50802, and the message names
           driver_monthly_metrics.total_miles and truck_utilization_metrics.total_miles.
           'NOT RAISED: the test failed' means the procedure missed the mismatch. */

BEGIN TRY
    BEGIN TRANSACTION;

    UPDATE fact.fact_trip
    SET actual_distance_miles = actual_distance_miles + 1
    WHERE trip_id = (SELECT MIN(f.trip_id) FROM fact.fact_trip AS f
                     WHERE f.is_deleted_in_source = 0 AND f.driver_key > 0 AND f.truck_key > 0);

    EXEC log.usp_reconcile_baselines @run_id = 'test-reconcile-mismatch';

    ROLLBACK TRANSACTION;
    SELECT 'NOT RAISED: the test failed' AS outcome;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    SELECT ERROR_NUMBER() AS error_number, ERROR_MESSAGE() AS error_message;
END CATCH;

/* Afterwards: the rollback also removes the procedure's own log rows, so
   log.reconciliation_result has no 'test-reconcile-mismatch' rows, and
   tests/gold/checks/11_reconciliation_passes.sql still returns no rows. */
