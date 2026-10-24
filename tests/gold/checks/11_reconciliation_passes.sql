/* Check 11: the latest reconciliation against the dataset publisher's baselines
   has no failing verified metric.

   log.usp_reconcile_baselines runs at the end of every pipeline run and fails
   the run with 50802 on a verified mismatch. This check reads its most recent
   attempt, so it also catches a run that never reached reconciliation leaving
   an older failure as the latest result.

   To produce a fresh result first:
       EXEC log.usp_reconcile_baselines @run_id = 'manual-<yyyymmdd>-check';

   The baselines are static, so any real change in silver makes this check
   fail by design; see docs/Gold.md, Reconciliation.

   Run in: wh_logistics_gold. Read only.
   Pass:   no rows. */

WITH latest_run AS (
    SELECT TOP 1 r.run_id
    FROM log.reconciliation_result AS r
    ORDER BY r.checked_at DESC
)
SELECT 'no reconciliation result recorded' AS problem,
       CAST(NULL AS VARCHAR(64)) AS run_id, CAST(NULL AS VARCHAR(64)) AS baseline_name,
       CAST(NULL AS VARCHAR(64)) AS metric_name, CAST(NULL AS VARCHAR(20)) AS status
WHERE NOT EXISTS (SELECT 1 FROM log.reconciliation_result)

UNION ALL
SELECT 'verified metric failed', l.run_id, l.baseline_name, l.metric_name, l.status
FROM log.vw_reconciliation_latest AS l
JOIN latest_run AS lr ON lr.run_id = l.run_id
WHERE l.is_verified = 1 AND l.status <> 'pass';
