CREATE   VIEW log.vw_unfinished_runs
AS
SELECT b.run_id, b.step_name, MIN(b.detected_at) AS detected_at
FROM log.cdf_batch AS b
WHERE NOT EXISTS (SELECT 1 FROM log.etl_run AS r
                  WHERE r.run_id = b.run_id AND r.step_name = b.step_name
                    AND r.event_type = 'succeeded')
GROUP BY b.run_id, b.step_name;

GO