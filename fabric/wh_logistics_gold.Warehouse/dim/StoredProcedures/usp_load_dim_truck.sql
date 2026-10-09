CREATE   PROCEDURE dim.usp_load_dim_truck
    @allow_mass_delete BIT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @initial_from INT, @dupes INT, @nulls INT, @backdated INT, @flagged INT, @active INT;
 
    IF @allow_mass_delete IS NULL
        THROW 50351, 'dim.usp_load_dim_truck: @allow_mass_delete is required.', 1;
 
    SELECT @initial_from = MIN(d.date_key) FROM dim.dim_date AS d WHERE d.date_key > 0;
    IF @initial_from IS NULL
        THROW 50352, 'dim.usp_load_dim_truck: load dim.dim_date first.', 1;
 
    SELECT @dupes = COUNT(*) - COUNT(DISTINCT v.truck_id) FROM dim.vw_src_truck AS v;
    IF @dupes <> 0
        THROW 50353, 'dim.usp_load_dim_truck: duplicate truck_id in silver.', 1;
 
    SELECT @nulls = COUNT(*) FROM dim.vw_src_truck AS v
    WHERE v.is_deleted_in_source IS NULL OR v.source_changed_date_key IS NULL;
    IF @nulls <> 0
        THROW 50356, 'dim.usp_load_dim_truck: silver rows without _is_deleted_in_source or _source_changed_at.', 1;
 
    SELECT @backdated = COUNT(*)
    FROM dim.dim_truck AS t
    JOIN dim.vw_src_truck AS v ON v.truck_id = t.truck_id
    WHERE t.truck_key > 0 AND t.is_current = 1 AND t.valid_from_date_key > v.source_changed_date_key
      AND EXISTS (SELECT t.truck_status, t.truck_home_terminal EXCEPT SELECT v.truck_status, v.truck_home_terminal);
    IF @backdated <> 0
        THROW 50354, 'dim.usp_load_dim_truck: a silver change date precedes the current version it would replace.', 1;
 
    DROP TABLE IF EXISTS #truck_status;
    CREATE TABLE #truck_status (truck_id VARCHAR(20) NOT NULL, target_deleted BIT NOT NULL)
        WITH (DISTRIBUTION = ROUND_ROBIN);
 
    INSERT INTO #truck_status (truck_id, target_deleted)
    SELECT m.truck_id,
           CASE WHEN v.truck_id IS NULL OR v.is_deleted_in_source = 1 THEN 1 ELSE 0 END
    FROM (SELECT DISTINCT d.truck_id FROM dim.dim_truck AS d WHERE d.truck_key > 0) AS m
    LEFT JOIN dim.vw_src_truck AS v ON v.truck_id = m.truck_id;
 
    SELECT @flagged = COUNT(*)
    FROM #truck_status AS s
    JOIN dim.dim_truck AS t ON t.truck_id = s.truck_id AND t.is_current = 1
    WHERE s.target_deleted = 1 AND t.is_deleted = 0;
 
    SELECT @active = COUNT(*) FROM dim.dim_truck AS t
    WHERE t.truck_key > 0 AND t.is_current = 1 AND t.is_deleted = 0;
 
    IF @allow_mass_delete = 0 AND @flagged > 0 AND @flagged * 10 > @active
        THROW 50355, 'dim.usp_load_dim_truck: more than 10 percent of active trucks would be marked deleted; investigate silver, then rerun with @allow_mass_delete = 1 if genuine.', 1;
 
    BEGIN TRY
        BEGIN TRANSACTION;
 
        -- Type 1: corrections apply to every version of the member
        UPDATE t
        SET unit_number            = v.unit_number,
            truck_make             = v.truck_make,
            truck_model_year       = v.truck_model_year,
            truck_acquisition_date = v.truck_acquisition_date,
            acquisition_mileage    = v.acquisition_mileage,
            tank_capacity_gallons  = v.tank_capacity_gallons,
            truck_version_label    = t.truck_id + ' ' + v.unit_number + ' (' + t.truck_home_terminal + ', ' + t.truck_status + ')'
        FROM dim.dim_truck AS t
        JOIN dim.vw_src_truck AS v ON v.truck_id = t.truck_id
        WHERE t.truck_key > 0
          AND EXISTS (SELECT t.unit_number, t.truck_make, t.truck_model_year, t.truck_acquisition_date,
                             t.acquisition_mileage, t.tank_capacity_gallons
                      EXCEPT
                      SELECT v.unit_number, v.truck_make, v.truck_model_year, v.truck_acquisition_date,
                             v.acquisition_mileage, v.tank_capacity_gallons);
 
        -- Type 2 change dated on the current version's own start date: overwrite
        UPDATE t
        SET truck_status        = v.truck_status,
            truck_home_terminal = v.truck_home_terminal,
            truck_version_label = t.truck_id + ' ' + v.unit_number + ' (' + v.truck_home_terminal + ', ' + v.truck_status + ')'
        FROM dim.dim_truck AS t
        JOIN dim.vw_src_truck AS v ON v.truck_id = t.truck_id
        WHERE t.truck_key > 0 AND t.is_current = 1 AND t.valid_from_date_key = v.source_changed_date_key
          AND EXISTS (SELECT t.truck_status, t.truck_home_terminal EXCEPT SELECT v.truck_status, v.truck_home_terminal);
 
        -- Type 2: expire the current version on the member's change date
        UPDATE t
        SET valid_to_date_key = v.source_changed_date_key,
            is_current        = 0
        FROM dim.dim_truck AS t
        JOIN dim.vw_src_truck AS v ON v.truck_id = t.truck_id
        WHERE t.truck_key > 0 AND t.is_current = 1 AND t.valid_from_date_key < v.source_changed_date_key
          AND EXISTS (SELECT t.truck_status, t.truck_home_terminal EXCEPT SELECT v.truck_status, v.truck_home_terminal);
 
        -- Type 2: insert the new current version
        INSERT INTO dim.dim_truck
            (truck_id, unit_number, truck_make, truck_status, truck_home_terminal, truck_model_year,
             truck_acquisition_date, acquisition_mileage, tank_capacity_gallons, truck_version_label,
             valid_from_date_key, valid_to_date_key, is_current, is_deleted, version_reason)
        SELECT v.truck_id, v.unit_number, v.truck_make, v.truck_status, v.truck_home_terminal, v.truck_model_year,
               v.truck_acquisition_date, v.acquisition_mileage, v.tank_capacity_gallons,
               v.truck_id + ' ' + v.unit_number + ' (' + v.truck_home_terminal + ', ' + v.truck_status + ')',
               v.source_changed_date_key, 99991231, 1, v.is_deleted_in_source,
               'Changed:'
               + CASE WHEN p.truck_status <> v.truck_status THEN ' status' ELSE '' END
               + CASE WHEN p.truck_home_terminal <> v.truck_home_terminal THEN ' home_terminal' ELSE '' END
        FROM dim.vw_src_truck AS v
        JOIN dim.dim_truck AS p
          ON p.truck_id = v.truck_id AND p.is_current = 0 AND p.valid_to_date_key = v.source_changed_date_key
        WHERE NOT EXISTS (SELECT 1 FROM dim.dim_truck AS c WHERE c.truck_id = v.truck_id AND c.is_current = 1);
 
        -- New members
        INSERT INTO dim.dim_truck
            (truck_id, unit_number, truck_make, truck_status, truck_home_terminal, truck_model_year,
             truck_acquisition_date, acquisition_mileage, tank_capacity_gallons, truck_version_label,
             valid_from_date_key, valid_to_date_key, is_current, is_deleted, version_reason)
        SELECT v.truck_id, v.unit_number, v.truck_make, v.truck_status, v.truck_home_terminal, v.truck_model_year,
               v.truck_acquisition_date, v.acquisition_mileage, v.tank_capacity_gallons,
               v.truck_id + ' ' + v.unit_number + ' (' + v.truck_home_terminal + ', ' + v.truck_status + ')',
               @initial_from, 99991231, 1, v.is_deleted_in_source, 'Initial version'
        FROM dim.vw_src_truck AS v
        WHERE NOT EXISTS (SELECT 1 FROM dim.dim_truck AS t WHERE t.truck_id = v.truck_id);
 
        -- Deletion state on every version of existing members
        UPDATE t
        SET is_deleted = s.target_deleted
        FROM dim.dim_truck AS t
        JOIN #truck_status AS s ON s.truck_id = t.truck_id
        WHERE t.truck_key > 0 AND t.is_deleted <> s.target_deleted;
 
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
 
    DROP TABLE IF EXISTS #truck_status;
END;

GO