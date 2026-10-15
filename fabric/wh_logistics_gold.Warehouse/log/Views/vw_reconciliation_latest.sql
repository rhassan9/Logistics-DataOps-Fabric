CREATE   VIEW log.vw_reconciliation_latest
AS
SELECT r.run_id, r.checked_at, r.baseline_name, r.metric_name, r.is_verified,
       r.rows_compared, r.rows_matched, r.baseline_only, r.gold_only,
       r.max_abs_difference, r.status
FROM log.reconciliation_result AS r
WHERE r.checked_at = (SELECT MAX(x.checked_at)
                      FROM log.reconciliation_result AS x
                      WHERE x.run_id = r.run_id);

GO