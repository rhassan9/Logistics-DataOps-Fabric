CREATE VIEW log.vw_batch_state
AS
/* One row per landed batch with its current state.

   log.batch_manifest holds one registration per batch folder, written by
   nb_batch_discover. log.batch_event holds the events written by
   log.usp_claim_next_batch and log.usp_record_batch_event: claimed,
   silver_applied, completed, failed, rejected, waived.

   States derived here, in order of precedence:
     waived             an operator closed the batch (see usp_record_batch_event)
     conflict           two different manifests registered for one batch_seq,
                        or a batch_id already registered at an earlier
                        batch_seq with different content
     (latest event)     claimed, silver_applied, completed, failed, rejected
     rejected           the manifest failed validation at discovery
     duplicate_ignored  the batch_id and content of an earlier batch_seq
     discovered         registered, nothing has happened to it yet
   Then, after the batch order is applied:
     superseded         rejected, and a later batch naming it in
                        supersedes_batch_id has completed
     blocked            discovered, but an earlier batch is still open or has
                        not landed
   A closed batch (completed, waived, duplicate_ignored, superseded) lets the
   next batch proceed. */
WITH registration AS (
    SELECT m.batch_seq, m.batch_id, m.batch_mode, m.business_date, m.as_of_date,
           m.previous_batch_id, m.supersedes_batch_id, m.content_hash, m.is_valid,
           m.problems, m.landed_at, m.discovered_at,
           ROW_NUMBER() OVER (PARTITION BY m.batch_seq ORDER BY m.discovered_at, m.batch_id) AS rn
    FROM log.batch_manifest AS m
),
seq_versions AS (
    SELECT m.batch_seq,
           COUNT(DISTINCT CONCAT(m.batch_id, '|', COALESCE(m.content_hash, ''))) AS manifest_versions
    FROM log.batch_manifest AS m
    GROUP BY m.batch_seq
),
first_registration AS (
    SELECT r.batch_seq, r.batch_id, r.content_hash, r.is_valid
    FROM registration AS r
    WHERE r.rn = 1
),
duplicates AS (
    SELECT later.batch_seq, MIN(earlier.batch_seq) AS duplicate_of_seq
    FROM first_registration AS later
    JOIN first_registration AS earlier
      ON earlier.batch_seq < later.batch_seq AND earlier.batch_id = later.batch_id
     AND earlier.content_hash = later.content_hash
    WHERE later.is_valid = 1
    GROUP BY later.batch_seq
),
clashes AS (
    SELECT later.batch_seq, MIN(earlier.batch_seq) AS clash_seq
    FROM first_registration AS later
    JOIN first_registration AS earlier
      ON earlier.batch_seq < later.batch_seq AND earlier.batch_id = later.batch_id
     AND COALESCE(earlier.content_hash, '') <> COALESCE(later.content_hash, '')
    GROUP BY later.batch_seq
),
latest_event AS (
    SELECT e.batch_seq, e.event_type, e.event_at, e.message,
           ROW_NUMBER() OVER (PARTITION BY e.batch_seq
                              ORDER BY e.event_at DESC,
                                       CASE e.event_type WHEN 'waived' THEN 1 WHEN 'completed' THEN 2
                                            WHEN 'rejected' THEN 3 WHEN 'failed' THEN 4
                                            WHEN 'silver_applied' THEN 5 ELSE 6 END) AS rn
    FROM log.batch_event AS e
),
latest_claim AS (
    SELECT e.batch_seq, e.run_id,
           ROW_NUMBER() OVER (PARTITION BY e.batch_seq ORDER BY e.event_at DESC) AS rn
    FROM log.batch_event AS e
    WHERE e.event_type = 'claimed'
),
base AS (
    SELECT r.batch_seq, r.batch_id, r.batch_mode, r.business_date, r.as_of_date,
           r.previous_batch_id, r.supersedes_batch_id, r.content_hash, r.is_valid,
           r.landed_at, r.discovered_at,
           dup.duplicate_of_seq,
           CASE WHEN le.event_type = 'waived'                          THEN 'waived'
                WHEN v.manifest_versions > 1 OR clash.clash_seq IS NOT NULL   THEN 'conflict'
                WHEN le.event_type IS NOT NULL                         THEN le.event_type
                WHEN r.is_valid = 0                                    THEN 'rejected'
                WHEN dup.duplicate_of_seq IS NOT NULL                  THEN 'duplicate_ignored'
                ELSE 'discovered' END                                  AS base_state,
           COALESCE(le.event_at, r.discovered_at)                      AS state_at,
           lc.run_id                                                   AS owner_run_id,
           CASE WHEN le.event_type = 'waived' THEN le.message
                WHEN v.manifest_versions > 1
                     THEN CONCAT('batch_seq ', r.batch_seq, ' has ', v.manifest_versions,
                                 ' different manifests registered')
                WHEN clash.clash_seq IS NOT NULL
                     THEN CONCAT('batch_id ', r.batch_id, ' is registered at batch_seq ',
                                 clash.clash_seq, ' with different content')
                ELSE COALESCE(le.message, r.problems) END              AS state_message
    FROM registration AS r
    JOIN seq_versions AS v ON v.batch_seq = r.batch_seq
    LEFT JOIN latest_event AS le ON le.batch_seq = r.batch_seq AND le.rn = 1
    LEFT JOIN latest_claim AS lc ON lc.batch_seq = r.batch_seq AND lc.rn = 1
    LEFT JOIN duplicates AS dup ON dup.batch_seq = r.batch_seq
    LEFT JOIN clashes AS clash ON clash.batch_seq = r.batch_seq
    WHERE r.rn = 1
),
settled AS (
    SELECT b.*,
           CASE WHEN b.base_state = 'rejected'
                 AND EXISTS (SELECT 1 FROM base AS s
                             WHERE s.supersedes_batch_id = b.batch_id
                               AND s.batch_seq > b.batch_seq
                               AND s.base_state = 'completed')
                THEN 'superseded' ELSE b.base_state END AS settled_state
    FROM base AS b
),
frontier AS (
    SELECT MIN(s.batch_seq) AS batch_seq
    FROM settled AS s
    WHERE s.settled_state NOT IN ('completed', 'waived', 'duplicate_ignored', 'superseded')
),
frontier_row AS (
    SELECT s.batch_seq, s.batch_id, s.settled_state,
           CASE WHEN (SELECT COUNT(*) FROM settled AS x WHERE x.batch_seq < s.batch_seq) = s.batch_seq
                THEN 0 ELSE 1 END AS has_gap
    FROM settled AS s
    JOIN frontier AS f ON f.batch_seq = s.batch_seq
)
SELECT s.batch_seq,
       s.batch_id,
       s.batch_mode,
       s.business_date,
       s.as_of_date,
       s.previous_batch_id,
       s.supersedes_batch_id,
       s.content_hash,
       s.is_valid,
       s.duplicate_of_seq,
       s.landed_at,
       s.discovered_at,
       CASE WHEN s.settled_state = 'discovered' AND fr.batch_seq IS NOT NULL
                 AND ((s.batch_seq = fr.batch_seq AND fr.has_gap = 1)
                      OR (s.batch_seq > fr.batch_seq
                          AND NOT (fr.settled_state = 'rejected'
                                   AND COALESCE(s.supersedes_batch_id, '') = fr.batch_id)))
            THEN 'blocked'
            ELSE s.settled_state END                                       AS state,
       s.state_at,
       s.owner_run_id,
       s.state_message,
       CASE WHEN s.settled_state IN ('completed', 'waived', 'duplicate_ignored', 'superseded')
            THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END                    AS is_closed
FROM settled AS s
LEFT JOIN frontier_row AS fr ON 1 = 1;

GO

