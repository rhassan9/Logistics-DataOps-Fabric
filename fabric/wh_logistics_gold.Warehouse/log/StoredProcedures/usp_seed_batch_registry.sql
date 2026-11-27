CREATE PROCEDURE log.usp_seed_batch_registry
AS
BEGIN
    SET NOCOUNT ON;

    -- The single row every registry change updates inside its transaction.
    -- Safe to run again: it inserts the row only when it is missing.
    IF NOT EXISTS (SELECT 1 FROM log.batch_claim_lock AS l WHERE l.lock_name = 'batch_registry')
        INSERT INTO log.batch_claim_lock (lock_name, held_by, touched_at)
        VALUES ('batch_registry', NULL, NULL);

    IF (SELECT COUNT(*) FROM log.batch_claim_lock AS l WHERE l.lock_name = 'batch_registry') <> 1
        THROW 50901, 'log.usp_seed_batch_registry: log.batch_claim_lock must hold exactly one batch_registry row.', 1;
END;

GO
