CREATE   PROCEDURE log.usp_write_run_event
    @run_id               VARCHAR(64),
    @step_name            VARCHAR(128),
    @source_table         VARCHAR(128),
    @event_type           VARCHAR(16),
    @rows_read            BIGINT,
    @rows_inserted        BIGINT,
    @rows_updated         BIGINT,
    @rows_deleted         BIGINT,
    @rows_unknown_member  BIGINT,
    @message              VARCHAR(4000)
AS
BEGIN
    SET NOCOUNT ON;

    IF @run_id IS NULL OR @step_name IS NULL
        THROW 50501, 'log.usp_write_run_event: run_id and step_name are required.', 1;

    IF @event_type IS NULL OR @event_type NOT IN ('started', 'succeeded', 'failed', 'skipped')
        THROW 50502, 'log.usp_write_run_event: event_type must be started, succeeded, failed or skipped.', 1;

    INSERT INTO log.etl_run
        (run_id, step_name, source_table, event_type, event_at,
         rows_read, rows_inserted, rows_updated, rows_deleted, rows_unknown_member, message)
    VALUES
        (@run_id, @step_name, @source_table, @event_type, SYSUTCDATETIME(),
         @rows_read, @rows_inserted, @rows_updated, @rows_deleted, @rows_unknown_member, @message);
END;

GO