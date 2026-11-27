CREATE PROCEDURE log.usp_record_batch_event
    @batch_seq      INT,
    @event_type     VARCHAR(20),
    @run_id         VARCHAR(64),
    @actor          VARCHAR(128),
    @message        VARCHAR(4000),
    @integrity_note VARCHAR(4000),
    @rows_written   BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @state VARCHAR(30), @owner VARCHAR(64), @batch_id VARCHAR(128),
            @batch_mode VARCHAR(20), @locked INT, @msg VARCHAR(4000);

    IF @batch_seq IS NULL OR @event_type IS NULL
        THROW 50905, 'log.usp_record_batch_event: @batch_seq and @event_type are required.', 1;

    IF @event_type NOT IN ('silver_applied', 'completed', 'failed', 'rejected', 'waived')
        THROW 50906, 'log.usp_record_batch_event: @event_type must be silver_applied, completed, failed, rejected or waived; claims are made by log.usp_claim_next_batch.', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Serialises registry changes with claims (see log.usp_claim_next_batch).
        UPDATE log.batch_claim_lock
        SET held_by = COALESCE(@run_id, @actor), touched_at = SYSUTCDATETIME()
        WHERE lock_name = 'batch_registry';
        SET @locked = @@ROWCOUNT;
        IF @locked <> 1
            THROW 50903, 'log.usp_record_batch_event: run log.usp_seed_batch_registry first.', 1;

        SELECT @state = s.state, @owner = s.owner_run_id, @batch_id = s.batch_id, @batch_mode = s.batch_mode
        FROM log.vw_batch_state AS s
        WHERE s.batch_seq = @batch_seq;

        IF @state IS NULL
            THROW 50907, 'log.usp_record_batch_event: @batch_seq is not registered; run nb_batch_discover first.', 1;

        -- A retry of a call that committed: the batch already holds this event
        -- from this run, so nothing is written.
        IF @state = @event_type AND @run_id IS NOT NULL
           AND EXISTS (SELECT 1 FROM log.batch_event AS e
                       WHERE e.batch_seq = @batch_seq AND e.event_type = @event_type
                         AND e.run_id = @run_id
                         AND e.event_at = (SELECT MAX(x.event_at) FROM log.batch_event AS x
                                           WHERE x.batch_seq = @batch_seq))
        BEGIN
            COMMIT TRANSACTION;
            RETURN;
        END;

        -- Only the run that owns the claim moves a batch forward or rejects it.
        IF @event_type IN ('silver_applied', 'completed', 'rejected')
           AND (@run_id IS NULL OR @owner IS NULL OR @run_id <> @owner)
        BEGIN
            SET @msg = CONCAT('log.usp_record_batch_event: batch_seq ', @batch_seq, ' is owned by run ',
                              COALESCE(@owner, '(none)'), ', not ', COALESCE(@run_id, '(none)'), '.');
            THROW 50908, @msg, 1;
        END;

        IF (@event_type IN ('silver_applied', 'rejected') AND @state <> 'claimed')
           OR (@event_type = 'completed' AND @state <> 'silver_applied')
           OR (@event_type = 'failed' AND @state NOT IN ('claimed', 'silver_applied'))
           OR (@event_type = 'waived' AND @state NOT IN ('rejected', 'conflict'))
        BEGIN
            SET @msg = CONCAT('log.usp_record_batch_event: batch_seq ', @batch_seq, ' is ', @state,
                              '; ', @event_type, ' is not a valid next state.');
            THROW 50909, @msg, 1;
        END;

        IF @event_type IN ('rejected', 'failed') AND (@message IS NULL OR LTRIM(RTRIM(@message)) = '')
            THROW 50910, 'log.usp_record_batch_event: a rejection or failure needs @message with the reason.', 1;

        -- A failure recorded by anyone but the owning run resolves a stale
        -- claim. It is an operator decision, so it names the operator.
        IF @event_type = 'failed' AND (@run_id IS NULL OR @owner IS NULL OR @run_id <> @owner)
           AND (@actor IS NULL OR LTRIM(RTRIM(@actor)) = '')
            THROW 50911, 'log.usp_record_batch_event: recording another run''s batch as failed needs @actor, the operator resolving the stale claim.', 1;

        -- A waiver is never automatic: who, why, and what it means for the data.
        IF @event_type = 'waived'
           AND (@actor IS NULL OR @message IS NULL OR @integrity_note IS NULL
                OR LTRIM(RTRIM(@actor)) = '' OR LTRIM(RTRIM(@message)) = '' OR LTRIM(RTRIM(@integrity_note)) = '')
            THROW 50912, 'log.usp_record_batch_event: a waiver needs @actor, @message (the reason) and @integrity_note (what skipping the batch means for the data).', 1;

        -- Completed only after gold has finished under the same run: every
        -- gold step succeeded, and for the snapshot the publisher comparisons.
        IF @event_type = 'completed'
        BEGIN
            EXEC log.usp_assert_gold_run_complete @run_id;
            IF @batch_mode = 'INITIAL_SNAPSHOT'
               AND NOT EXISTS (SELECT 1 FROM log.etl_run AS r
                               WHERE r.run_id = @run_id AND r.step_name = 'reconcile_baselines'
                                 AND r.event_type = 'succeeded')
                THROW 50913, 'log.usp_record_batch_event: the snapshot completes only after reconcile_baselines has succeeded under the same run_id.', 1;
        END;

        INSERT INTO log.batch_event
            (batch_seq, batch_id, event_type, event_at, run_id, actor, message, integrity_note, rows_written)
        VALUES
            (@batch_seq, @batch_id, @event_type, SYSUTCDATETIME(), @run_id, @actor, @message,
             @integrity_note, @rows_written);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;

GO
