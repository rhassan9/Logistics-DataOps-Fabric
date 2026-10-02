CREATE   PROCEDURE dim.usp_load_dim_truck
    @effective_date DATE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @initial_from INT, @eff_key INT, @dupes INT, @backdated INT;

    IF @effective_date IS NULL
        THROW 50351, 'dim.usp_load_dim_truck: @effective_date is required.', 1;

    SET @eff_key = YEAR(@effective_date) * 10000 + MONTH(@effective_date) * 100 + DAY(@effective_date);

    SELECT @initial_from = MIN(d.date_key) FROM dim.dim_date AS d WHERE d.date_key > 0;
    IF @initial_from IS NULL
        THROW 50352, 'dim.usp_load_dim_truck: load dim.dim_date first.', 1;

    SELECT @dupes = COUNT(*) - COUNT(DISTINCT v.truck_id) FROM dim.vw_src_truck AS v;
    IF @dupes <> 0
        THROW 50353, 'dim.usp_load_dim_truck: duplicate truck_id in silver.', 1;

    SELECT @backdated = COUNT(*)
    FROM dim.dim_truck AS t
    JOIN dim.vw_src_truck AS v ON v.truck_id = t.truck_id
    WHERE t.truck_key > 0 AND t.is_current = 1 AND t.valid_from_date_key > @eff_key
      AND EXISTS (SELECT t.truck_status, t.truck_home_terminal EXCEPT SELECT v.truck_status, v.truck_home_terminal);
    IF @backdated <> 0
        THROW 50354, 'dim.usp_load_dim_truck: @effective_date precedes a current version it would replace.', 1;

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

        -- Type 2 change on the current version's own start date: overwrite
        UPDATE t
        SET truck_status        = v.truck_status,
            truck_home_terminal = v.truck_home_terminal,
            truck_version_label = t.truck_id + ' ' + v.unit_number + ' (' + v.truck_home_terminal + ', ' + v.truck_status + ')'
        FROM dim.dim_truck AS t
        JOIN dim.vw_src_truck AS v ON v.truck_id = t.truck_id
        WHERE t.truck_key > 0 AND t.is_current = 1 AND t.valid_from_date_key = @eff_key
          AND EXISTS (SELECT t.truck_status, t.truck_home_terminal EXCEPT SELECT v.truck_status, v.truck_home_terminal);

        -- Type 2: expire
        UPDATE t
        SET valid_to_date_key = @eff_key,
            is_current        = 0
        FROM dim.dim_truck AS t
        JOIN dim.vw_src_truck AS v ON v.truck_id = t.truck_id
        WHERE t.truck_key > 0 AND t.is_current = 1 AND t.valid_from_date_key < @eff_key
          AND EXISTS (SELECT t.truck_status, t.truck_home_terminal EXCEPT SELECT v.truck_status, v.truck_home_terminal);

        -- Type 2: insert the new current version
        INSERT INTO dim.dim_truck
            (truck_id, unit_number, truck_make, truck_status, truck_home_terminal, truck_model_year,
             truck_acquisition_date, acquisition_mileage, tank_capacity_gallons, truck_version_label,
             valid_from_date_key, valid_to_date_key, is_current, version_reason)
        SELECT v.truck_id, v.unit_number, v.truck_make, v.truck_status, v.truck_home_terminal, v.truck_model_year,
               v.truck_acquisition_date, v.acquisition_mileage, v.tank_capacity_gallons,
               v.truck_id + ' ' + v.unit_number + ' (' + v.truck_home_terminal + ', ' + v.truck_status + ')',
               @eff_key, 99991231, 1,
               'Changed:'
               + CASE WHEN p.truck_status <> v.truck_status THEN ' status' ELSE '' END
               + CASE WHEN p.truck_home_terminal <> v.truck_home_terminal THEN ' home_terminal' ELSE '' END
        FROM dim.vw_src_truck AS v
        JOIN dim.dim_truck AS p
          ON p.truck_id = v.truck_id AND p.is_current = 0 AND p.valid_to_date_key = @eff_key
        WHERE NOT EXISTS (SELECT 1 FROM dim.dim_truck AS c WHERE c.truck_id = v.truck_id AND c.is_current = 1);

        -- New members
        INSERT INTO dim.dim_truck
            (truck_id, unit_number, truck_make, truck_status, truck_home_terminal, truck_model_year,
             truck_acquisition_date, acquisition_mileage, tank_capacity_gallons, truck_version_label,
             valid_from_date_key, valid_to_date_key, is_current, version_reason)
        SELECT v.truck_id, v.unit_number, v.truck_make, v.truck_status, v.truck_home_terminal, v.truck_model_year,
               v.truck_acquisition_date, v.acquisition_mileage, v.tank_capacity_gallons,
               v.truck_id + ' ' + v.unit_number + ' (' + v.truck_home_terminal + ', ' + v.truck_status + ')',
               @initial_from, 99991231, 1, 'Initial version'
        FROM dim.vw_src_truck AS v
        WHERE NOT EXISTS (SELECT 1 FROM dim.dim_truck AS t WHERE t.truck_id = v.truck_id);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;

GO