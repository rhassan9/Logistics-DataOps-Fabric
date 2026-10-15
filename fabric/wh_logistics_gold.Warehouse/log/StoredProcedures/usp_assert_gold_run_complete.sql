CREATE   PROCEDURE log.usp_assert_gold_run_complete
    @run_id VARCHAR(64)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @missing VARCHAR(4000), @msg VARCHAR(4000);

    IF @run_id IS NULL
        THROW 50811, 'log.usp_assert_gold_run_complete: @run_id is required.', 1;

    -- Every gold step must have succeeded under this run_id. Catches a step
    -- that never ran, such as an activity wired to the wrong procedure.
    SELECT @missing = STRING_AGG(CAST(s.step_name AS VARCHAR(4000)), ', ')
    FROM (VALUES ('load_dimensions'), ('fact_trip'), ('fact_delivery_event'),
                 ('fact_fuel_purchase'), ('fact_maintenance'),
                 ('fact_safety_incident'), ('fact_load_lifecycle')) AS s(step_name)
    WHERE NOT EXISTS (SELECT 1 FROM log.etl_run AS r
                      WHERE r.run_id = @run_id AND r.step_name = s.step_name
                        AND r.event_type = 'succeeded');

    IF @missing IS NOT NULL
    BEGIN
        SET @msg = 'log.usp_assert_gold_run_complete: no success logged for ' + @missing
                 + ' under run_id ' + @run_id + '.';
        THROW 50812, @msg, 1;
    END;
END;

GO