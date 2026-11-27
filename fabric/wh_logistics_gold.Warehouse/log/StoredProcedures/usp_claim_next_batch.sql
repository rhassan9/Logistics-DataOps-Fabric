CREATE PROCEDURE log.usp_claim_next_batch
    @run_id VARCHAR(64),
    @actor  VARCHAR(128)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @decision VARCHAR(30), @batch_seq INT, @batch_id VARCHAR(128),
            @batch_mode VARCHAR(20), @reason VARCHAR(4000), @locked INT, @outcome VARCHAR(30);

    IF @run_id IS NULL OR LTRIM(RTRIM(@run_id)) = ''
        THROW 50902, 'log.usp_claim_next_batch: @run_id is required.', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Every registry change updates this one row first. Fabric Warehouse
        -- detects write-write conflicts per table and commits only the first
        -- of two concurrent transactions, so two claims cannot both succeed:
        -- the second fails and writes nothing. Pipeline concurrency 1 is the
        -- second barrier.
        UPDATE log.batch_claim_lock
        SET held_by = @run_id, touched_at = SYSUTCDATETIME()
        WHERE lock_name = 'batch_registry';
        SET @locked = @@ROWCOUNT;
        IF @locked <> 1
            THROW 50903, 'log.usp_claim_next_batch: run log.usp_seed_batch_registry first.', 1;

        -- A retry of a claim that committed: this run already owns a claimed
        -- batch, so the same claim is returned and nothing is written.
        SELECT TOP 1 @batch_seq = s.batch_seq, @batch_id = s.batch_id, @batch_mode = s.batch_mode
        FROM log.vw_batch_state AS s
        WHERE s.state = 'claimed' AND s.owner_run_id = @run_id
        ORDER BY s.batch_seq;

        IF @batch_seq IS NOT NULL
            SELECT @outcome = 'claimed', @decision = 'already_claimed',
                   @reason = CONCAT('batch_seq ', @batch_seq, ' is already claimed by this run');
        ELSE
        BEGIN
            -- One run applies one batch: a run whose batch has moved on needs a
            -- new run_id for the next one.
            IF EXISTS (SELECT 1 FROM log.batch_event AS e WHERE e.run_id = @run_id AND e.event_type = 'claimed')
                THROW 50904, 'log.usp_claim_next_batch: this run_id has already claimed a batch that is no longer claimed; a new claim needs a new run_id.', 1;

            SELECT TOP 1 @decision = n.decision, @batch_seq = n.batch_seq, @batch_id = n.batch_id,
                         @batch_mode = n.batch_mode, @reason = n.reason
            FROM log.vw_batch_next AS n;

            IF @decision IN ('claim', 'retry')
            BEGIN
                INSERT INTO log.batch_event
                    (batch_seq, batch_id, event_type, event_at, run_id, actor, message, integrity_note, rows_written)
                VALUES
                    (@batch_seq, @batch_id, 'claimed', SYSUTCDATETIME(), @run_id, @actor, @reason, NULL, NULL);
                SET @outcome = 'claimed';
            END
            ELSE
                SET @outcome = @decision;
        END;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH

    -- outcome is 'claimed' when this run owns batch_seq; otherwise it is the
    -- decision that stopped the claim (nothing_to_do, in_flight,
    -- wait_for_predecessor, stop_rejected, stop_conflict,
    -- stop_unexpected_state), and nothing was written.
    SELECT @outcome    AS outcome,
           @decision   AS decision,
           @batch_seq  AS batch_seq,
           @batch_id   AS batch_id,
           @batch_mode AS batch_mode,
           @run_id     AS run_id,
           @reason     AS reason;
END;

GO

