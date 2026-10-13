CREATE   VIEW log.vw_cdf_checkpoint
AS
SELECT w.step_name,
       w.source_table,
       w.version_to AS last_version
FROM
(
    SELECT b.step_name,
           b.source_table,
           b.version_to,
           ROW_NUMBER() OVER (PARTITION BY b.step_name, b.source_table
                              ORDER BY b.detected_at DESC, b.version_to DESC) AS rn
    FROM log.cdf_batch AS b
    WHERE EXISTS (SELECT 1 FROM log.etl_run AS r
                  WHERE r.run_id = b.run_id
                    AND r.step_name = b.step_name
                    AND r.event_type = 'succeeded'
                    AND r.event_at >= b.detected_at)
) AS w
WHERE w.rn = 1;

GO