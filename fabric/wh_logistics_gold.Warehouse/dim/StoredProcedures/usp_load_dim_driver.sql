CREATE   PROCEDURE dim.usp_load_dim_driver
    @allow_mass_delete BIT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @initial_from INT, @dupes INT, @nulls INT, @backdated INT, @flagged INT, @active INT;
 
    IF @allow_mass_delete IS NULL
        THROW 50311, 'dim.usp_load_dim_driver: @allow_mass_delete is required.', 1;
 
    SELECT @initial_from = MIN(d.date_key) FROM dim.dim_date AS d WHERE d.date_key > 0;
    IF @initial_from IS NULL
        THROW 50312, 'dim.usp_load_dim_driver: load dim.dim_date first.', 1;
 
    SELECT @dupes = COUNT(*) - COUNT(DISTINCT v.driver_id) FROM dim.vw_src_driver AS v;
    IF @dupes <> 0
        THROW 50313, 'dim.usp_load_dim_driver: duplicate driver_id in silver.', 1;
 
    SELECT @nulls = COUNT(*) FROM dim.vw_src_driver AS v
    WHERE v.is_deleted_in_source IS NULL OR v.source_changed_date_key IS NULL;
    IF @nulls <> 0
        THROW 50316, 'dim.usp_load_dim_driver: silver rows without _is_deleted_in_source or _source_changed_at.', 1;
 
    SELECT @backdated = COUNT(*)
    FROM dim.dim_driver AS t
    JOIN dim.vw_src_driver AS v ON v.driver_id = t.driver_id
    WHERE t.driver_key > 0 AND t.is_current = 1 AND t.valid_from_date_key > v.source_changed_date_key
      AND EXISTS (SELECT t.driver_home_terminal, t.employment_status, t.driver_termination_date
                  EXCEPT SELECT v.driver_home_terminal, v.employment_status, v.driver_termination_date);
    IF @backdated <> 0
        THROW 50314, 'dim.usp_load_dim_driver: a silver change date precedes the current version it would replace.', 1;
 
    DROP TABLE IF EXISTS #driver_status;
    CREATE TABLE #driver_status (driver_id VARCHAR(20) NOT NULL, target_deleted BIT NOT NULL)
        WITH (DISTRIBUTION = ROUND_ROBIN);
 
    INSERT INTO #driver_status (driver_id, target_deleted)
    SELECT m.driver_id,
           CASE WHEN v.driver_id IS NULL OR v.is_deleted_in_source = 1 THEN 1 ELSE 0 END
    FROM (SELECT DISTINCT d.driver_id FROM dim.dim_driver AS d WHERE d.driver_key > 0) AS m
    LEFT JOIN dim.vw_src_driver AS v ON v.driver_id = m.driver_id;
 
    SELECT @flagged = COUNT(*)
    FROM #driver_status AS s
    JOIN dim.dim_driver AS t ON t.driver_id = s.driver_id AND t.is_current = 1
    WHERE s.target_deleted = 1 AND t.is_deleted = 0;
 
    SELECT @active = COUNT(*) FROM dim.dim_driver AS t
    WHERE t.driver_key > 0 AND t.is_current = 1 AND t.is_deleted = 0;
 
    IF @allow_mass_delete = 0 AND @flagged > 0 AND @flagged * 10 > @active
        THROW 50315, 'dim.usp_load_dim_driver: more than 10 percent of active drivers would be marked deleted; investigate silver, then rerun with @allow_mass_delete = 1 if genuine.', 1;
 
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
 
        -- Type 2 change dated on the current version's own start date: overwrite, no zero-length version
        UPDATE t
        SET driver_home_terminal    = v.driver_home_terminal,
            employment_status       = v.employment_status,
            driver_termination_date = v.driver_termination_date,
            driver_version_label    = t.driver_id + ' ' + v.driver_name + ' (' + v.driver_home_terminal + ', ' + v.employment_status + ')'
        FROM dim.dim_driver AS t
        JOIN dim.vw_src_driver AS v ON v.driver_id = t.driver_id
        WHERE t.driver_key > 0 AND t.is_current = 1 AND t.valid_from_date_key = v.source_changed_date_key
          AND EXISTS (SELECT t.driver_home_terminal, t.employment_status, t.driver_termination_date
                      EXCEPT SELECT v.driver_home_terminal, v.employment_status, v.driver_termination_date);
 
        -- Type 2: expire the current version on the member's change date
        UPDATE t
        SET valid_to_date_key = v.source_changed_date_key,
            is_current        = 0
        FROM dim.dim_driver AS t
        JOIN dim.vw_src_driver AS v ON v.driver_id = t.driver_id
        WHERE t.driver_key > 0 AND t.is_current = 1 AND t.valid_from_date_key < v.source_changed_date_key
          AND EXISTS (SELECT t.driver_home_terminal, t.employment_status, t.driver_termination_date
                      EXCEPT SELECT v.driver_home_terminal, v.employment_status, v.driver_termination_date);
 
        -- Type 2: insert the new current version, starting where the old one ended
        INSERT INTO dim.dim_driver
            (driver_id, driver_name, driver_home_terminal, employment_status, driver_hire_date,
             driver_termination_date, years_experience, driver_version_label,
             valid_from_date_key, valid_to_date_key, is_current, is_deleted, version_reason)
        SELECT v.driver_id, v.driver_name, v.driver_home_terminal, v.employment_status, v.driver_hire_date,
               v.driver_termination_date, v.years_experience,
               v.driver_id + ' ' + v.driver_name + ' (' + v.driver_home_terminal + ', ' + v.employment_status + ')',
               v.source_changed_date_key, 99991231, 1, v.is_deleted_in_source,
               'Changed:'
               + CASE WHEN p.driver_home_terminal <> v.driver_home_terminal THEN ' home_terminal' ELSE '' END
               + CASE WHEN p.employment_status <> v.employment_status THEN ' employment_status' ELSE '' END
               + CASE WHEN EXISTS (SELECT p.driver_termination_date EXCEPT SELECT v.driver_termination_date)
                      THEN ' termination_date' ELSE '' END
        FROM dim.vw_src_driver AS v
        JOIN dim.dim_driver AS p
          ON p.driver_id = v.driver_id AND p.is_current = 0 AND p.valid_to_date_key = v.source_changed_date_key
        WHERE NOT EXISTS (SELECT 1 FROM dim.dim_driver AS c WHERE c.driver_id = v.driver_id AND c.is_current = 1);
 
        -- New members: first version starts at the earliest date in dim_date
        INSERT INTO dim.dim_driver
            (driver_id, driver_name, driver_home_terminal, employment_status, driver_hire_date,
             driver_termination_date, years_experience, driver_version_label,
             valid_from_date_key, valid_to_date_key, is_current, is_deleted, version_reason)
        SELECT v.driver_id, v.driver_name, v.driver_home_terminal, v.employment_status, v.driver_hire_date,
               v.driver_termination_date, v.years_experience,
               v.driver_id + ' ' + v.driver_name + ' (' + v.driver_home_terminal + ', ' + v.employment_status + ')',
               @initial_from, 99991231, 1, v.is_deleted_in_source, 'Initial version'
        FROM dim.vw_src_driver AS v
        WHERE NOT EXISTS (SELECT 1 FROM dim.dim_driver AS t WHERE t.driver_id = v.driver_id);
 
        -- Deletion state on every version of existing members
        UPDATE t
        SET is_deleted = s.target_deleted
        FROM dim.dim_driver AS t
        JOIN #driver_status AS s ON s.driver_id = t.driver_id
        WHERE t.driver_key > 0 AND t.is_deleted <> s.target_deleted;
 
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
 
    DROP TABLE IF EXISTS #driver_status;
END;

GO