CREATE   PROCEDURE dim.usp_load_dim_driver
    @effective_date DATE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @initial_from INT, @eff_key INT, @dupes INT, @backdated INT;

    IF @effective_date IS NULL
        THROW 50311, 'dim.usp_load_dim_driver: @effective_date is required.', 1;

    SET @eff_key = YEAR(@effective_date) * 10000 + MONTH(@effective_date) * 100 + DAY(@effective_date);

    SELECT @initial_from = MIN(d.date_key) FROM dim.dim_date AS d WHERE d.date_key > 0;
    IF @initial_from IS NULL
        THROW 50312, 'dim.usp_load_dim_driver: load dim.dim_date first.', 1;

    SELECT @dupes = COUNT(*) - COUNT(DISTINCT v.driver_id) FROM dim.vw_src_driver AS v;
    IF @dupes <> 0
        THROW 50313, 'dim.usp_load_dim_driver: duplicate driver_id in silver.', 1;

    SELECT @backdated = COUNT(*)
    FROM dim.dim_driver AS t
    JOIN dim.vw_src_driver AS v ON v.driver_id = t.driver_id
    WHERE t.driver_key > 0 AND t.is_current = 1 AND t.valid_from_date_key > @eff_key
      AND EXISTS (SELECT t.driver_home_terminal, t.employment_status, t.driver_termination_date
                  EXCEPT SELECT v.driver_home_terminal, v.employment_status, v.driver_termination_date);
    IF @backdated <> 0
        THROW 50314, 'dim.usp_load_dim_driver: @effective_date precedes a current version it would replace.', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Type 1: corrections apply to every version of the member
        UPDATE t
        SET driver_name          = v.driver_name,
            driver_hire_date     = v.driver_hire_date,
            years_experience     = v.years_experience,
            driver_version_label = t.driver_id + ' ' + v.driver_name + ' (' + t.driver_home_terminal + ', ' + t.employment_status + ')'
        FROM dim.dim_driver AS t
        JOIN dim.vw_src_driver AS v ON v.driver_id = t.driver_id
        WHERE t.driver_key > 0
          AND EXISTS (SELECT t.driver_name, t.driver_hire_date, t.years_experience
                      EXCEPT SELECT v.driver_name, v.driver_hire_date, v.years_experience);

        -- Type 2 change on the current version's own start date: overwrite, no zero-length version
        UPDATE t
        SET driver_home_terminal    = v.driver_home_terminal,
            employment_status       = v.employment_status,
            driver_termination_date = v.driver_termination_date,
            driver_version_label    = t.driver_id + ' ' + v.driver_name + ' (' + v.driver_home_terminal + ', ' + v.employment_status + ')'
        FROM dim.dim_driver AS t
        JOIN dim.vw_src_driver AS v ON v.driver_id = t.driver_id
        WHERE t.driver_key > 0 AND t.is_current = 1 AND t.valid_from_date_key = @eff_key
          AND EXISTS (SELECT t.driver_home_terminal, t.employment_status, t.driver_termination_date
                      EXCEPT SELECT v.driver_home_terminal, v.employment_status, v.driver_termination_date);

        -- Type 2: expire the current version
        UPDATE t
        SET valid_to_date_key = @eff_key,
            is_current        = 0
        FROM dim.dim_driver AS t
        JOIN dim.vw_src_driver AS v ON v.driver_id = t.driver_id
        WHERE t.driver_key > 0 AND t.is_current = 1 AND t.valid_from_date_key < @eff_key
          AND EXISTS (SELECT t.driver_home_terminal, t.employment_status, t.driver_termination_date
                      EXCEPT SELECT v.driver_home_terminal, v.employment_status, v.driver_termination_date);

        -- Type 2: insert the new current version, starting where the old one ended
        INSERT INTO dim.dim_driver
            (driver_id, driver_name, driver_home_terminal, employment_status, driver_hire_date,
             driver_termination_date, years_experience, driver_version_label,
             valid_from_date_key, valid_to_date_key, is_current, version_reason)
        SELECT v.driver_id, v.driver_name, v.driver_home_terminal, v.employment_status, v.driver_hire_date,
               v.driver_termination_date, v.years_experience,
               v.driver_id + ' ' + v.driver_name + ' (' + v.driver_home_terminal + ', ' + v.employment_status + ')',
               @eff_key, 99991231, 1,
               'Changed:'
               + CASE WHEN p.driver_home_terminal <> v.driver_home_terminal THEN ' home_terminal' ELSE '' END
               + CASE WHEN p.employment_status <> v.employment_status THEN ' employment_status' ELSE '' END
               + CASE WHEN EXISTS (SELECT p.driver_termination_date EXCEPT SELECT v.driver_termination_date)
                      THEN ' termination_date' ELSE '' END
        FROM dim.vw_src_driver AS v
        JOIN dim.dim_driver AS p
          ON p.driver_id = v.driver_id AND p.is_current = 0 AND p.valid_to_date_key = @eff_key
        WHERE NOT EXISTS (SELECT 1 FROM dim.dim_driver AS c WHERE c.driver_id = v.driver_id AND c.is_current = 1);

        -- New members: first version starts at the earliest date in dim_date
        INSERT INTO dim.dim_driver
            (driver_id, driver_name, driver_home_terminal, employment_status, driver_hire_date,
             driver_termination_date, years_experience, driver_version_label,
             valid_from_date_key, valid_to_date_key, is_current, version_reason)
        SELECT v.driver_id, v.driver_name, v.driver_home_terminal, v.employment_status, v.driver_hire_date,
               v.driver_termination_date, v.years_experience,
               v.driver_id + ' ' + v.driver_name + ' (' + v.driver_home_terminal + ', ' + v.employment_status + ')',
               @initial_from, 99991231, 1, 'Initial version'
        FROM dim.vw_src_driver AS v
        WHERE NOT EXISTS (SELECT 1 FROM dim.dim_driver AS t WHERE t.driver_id = v.driver_id);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;

GO