CREATE VIEW log.vw_batch_next
AS
/* The one decision the next run takes, as a single row.

   In order of precedence:
     in_flight             a batch is claimed or silver_applied: that run must
                           finish it, or an operator records it failed
     stop_conflict         a batch is in conflict: two different manifests for
                           one batch_seq, or a batch_id reused with other content
     wait_for_predecessor  the earliest open batch has a gap before it
     retry                 the earliest open batch failed: it is retried first
     claim                 the earliest open batch is discovered, or it is
                           rejected and a later batch supersedes it
     stop_rejected         the earliest open batch is rejected: every later
                           batch waits for a superseding batch or a waiver
     nothing_to_do         every registered batch is closed
     stop_unexpected_state the earliest open batch is in a state no rule handles */
WITH s AS (
    SELECT b.batch_seq, b.batch_id, b.batch_mode, b.supersedes_batch_id, b.is_valid,
           b.state, b.owner_run_id, b.is_closed, b.state_message
    FROM log.vw_batch_state AS b
),
frontier AS (
    SELECT TOP 1 s.batch_seq, s.batch_id, s.batch_mode, s.state,
           (SELECT COUNT(*) FROM s AS x WHERE x.batch_seq < s.batch_seq) AS earlier_batches
    FROM s
    WHERE s.is_closed = 0
    ORDER BY s.batch_seq
),
candidates AS (
    SELECT 1 AS priority, 'in_flight' AS decision, s.batch_seq, s.batch_id, s.batch_mode,
           CONCAT('batch_seq ', s.batch_seq, ' is ', s.state, ' by run ', s.owner_run_id,
                  '; that run finishes it, or an operator records it failed') AS reason
    FROM s
    WHERE s.state IN ('claimed', 'silver_applied')
    UNION ALL
    SELECT 2, 'stop_conflict', s.batch_seq, s.batch_id, s.batch_mode,
           CONCAT('batch_seq ', s.batch_seq, ' is in conflict: ', s.state_message,
                  '; an operator investigates and waives it')
    FROM s
    WHERE s.state = 'conflict'
    UNION ALL
    SELECT 3, 'wait_for_predecessor', f.batch_seq, f.batch_id, f.batch_mode,
           CONCAT('batch_seq ', f.batch_seq, ' waits: only ', f.earlier_batches,
                  ' earlier batches have landed')
    FROM frontier AS f
    WHERE f.earlier_batches <> f.batch_seq
    UNION ALL
    SELECT 4, 'retry', f.batch_seq, f.batch_id, f.batch_mode,
           CONCAT('batch_seq ', f.batch_seq, ' failed and is retried first')
    FROM frontier AS f
    WHERE f.state = 'failed'
    UNION ALL
    SELECT 4, 'claim', f.batch_seq, f.batch_id, f.batch_mode,
           CONCAT('batch_seq ', f.batch_seq, ' is next')
    FROM frontier AS f
    WHERE f.state = 'discovered'
    UNION ALL
    SELECT 4, 'claim', s.batch_seq, s.batch_id, s.batch_mode,
           CONCAT('batch_seq ', s.batch_seq, ' supersedes rejected batch_seq ', f.batch_seq)
    FROM frontier AS f
    JOIN s ON s.supersedes_batch_id = f.batch_id AND s.batch_seq > f.batch_seq
          AND s.is_valid = 1 AND s.state = 'discovered'
    WHERE f.state = 'rejected'
    UNION ALL
    SELECT 5, 'stop_rejected', f.batch_seq, f.batch_id, f.batch_mode,
           CONCAT('batch_seq ', f.batch_seq,
                  ' is rejected; later batches wait for a superseding batch or a waiver')
    FROM frontier AS f
    WHERE f.state = 'rejected'
    UNION ALL
    SELECT 6, 'nothing_to_do', NULL, NULL, NULL, 'every registered batch is closed'
    WHERE NOT EXISTS (SELECT 1 FROM frontier)
    UNION ALL
    SELECT 7, 'stop_unexpected_state', f.batch_seq, f.batch_id, f.batch_mode,
           CONCAT('batch_seq ', f.batch_seq, ' is ', f.state, ', which no rule handles')
    FROM frontier AS f
)
SELECT TOP 1 c.decision, c.batch_seq, c.batch_id, c.batch_mode, c.reason
FROM candidates AS c
ORDER BY c.priority, c.batch_seq;

GO
