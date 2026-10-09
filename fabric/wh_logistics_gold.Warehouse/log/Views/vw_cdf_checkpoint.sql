CREATE   VIEW log.vw_cdf_checkpoint
AS
SELECT b.step_name,
       b.source_table,
       MAX(b.version_to) AS last_version
FROM log.cdf_batch AS b
WHERE EXISTS (SELECT 1 FROM log.etl_run AS r
              WHERE r.run_id = b.run_id
                AND r.step_name = b.step_name
                AND r.event_type = 'succeeded')
GROUP BY b.step_name, b.source_table;

GO