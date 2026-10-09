CREATE   PROCEDURE dim.usp_load_dim_route
    @allow_mass_delete BIT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @initial_from INT, @dupes INT, @nulls INT, @flagged INT, @active INT;
 
    IF @allow_mass_delete IS NULL
        THROW 50333, 'dim.usp_load_dim_route: @allow_mass_delete is required.', 1;
 
    SELECT @initial_from = MIN(d.date_key) FROM dim.dim_date AS d WHERE d.date_key > 0;
    IF @initial_from IS NULL
        THROW 50331, 'dim.usp_load_dim_route: load dim.dim_date first.', 1;
 
    SELECT @dupes = COUNT(*) - COUNT(DISTINCT v.route_id) FROM dim.vw_src_route AS v;
    IF @dupes <> 0
        THROW 50332, 'dim.usp_load_dim_route: duplicate route_id in silver.', 1;
 
    SELECT @nulls = COUNT(*) FROM dim.vw_src_route AS v WHERE v.is_deleted_in_source IS NULL;
    IF @nulls <> 0
        THROW 50336, 'dim.usp_load_dim_route: silver rows without _is_deleted_in_source.', 1;
 
    DROP TABLE IF EXISTS #route_status;
    CREATE TABLE #route_status (route_id VARCHAR(20) NOT NULL, target_deleted BIT NOT NULL)
        WITH (DISTRIBUTION = ROUND_ROBIN);
 
    INSERT INTO #route_status (route_id, target_deleted)
    SELECT m.route_id,
           CASE WHEN v.route_id IS NULL OR v.is_deleted_in_source = 1 THEN 1 ELSE 0 END
    FROM (SELECT DISTINCT d.route_id FROM dim.dim_route AS d WHERE d.route_key > 0) AS m
    LEFT JOIN dim.vw_src_route AS v ON v.route_id = m.route_id;
 
    SELECT @flagged = COUNT(*)
    FROM #route_status AS s
    JOIN dim.dim_route AS t ON t.route_id = s.route_id AND t.is_current = 1
    WHERE s.target_deleted = 1 AND t.is_deleted = 0;
 
    SELECT @active = COUNT(*) FROM dim.dim_route AS t
    WHERE t.route_key > 0 AND t.is_current = 1 AND t.is_deleted = 0;
 
    IF @allow_mass_delete = 0 AND @flagged > 0 AND @flagged * 10 > @active
        THROW 50335, 'dim.usp_load_dim_route: more than 10 percent of active routes would be marked deleted; investigate silver, then rerun with @allow_mass_delete = 1 if genuine.', 1;
 
    BEGIN TRY
        BEGIN TRANSACTION;
 
        UPDATE t
        SET route_name             = v.route_name,
            origin_city            = v.origin_city,
            origin_state_code      = v.origin_state_code,
            origin_state_name      = v.origin_state_name,
            destination_city       = v.destination_city,
            destination_state_code = v.destination_state_code,
            destination_state_name = v.destination_state_name,
            typical_distance_miles = v.typical_distance_miles,
            base_rate_per_mile     = v.base_rate_per_mile,
            fuel_surcharge_rate    = v.fuel_surcharge_rate,
            typical_transit_days   = v.typical_transit_days
        FROM dim.dim_route AS t
        JOIN dim.vw_src_route AS v ON v.route_id = t.route_id
        WHERE t.route_key > 0
          AND EXISTS (SELECT t.route_name, t.origin_city, t.origin_state_code, t.origin_state_name,
                             t.destination_city, t.destination_state_code, t.destination_state_name,
                             t.typical_distance_miles, t.base_rate_per_mile, t.fuel_surcharge_rate, t.typical_transit_days
                      EXCEPT
                      SELECT v.route_name, v.origin_city, v.origin_state_code, v.origin_state_name,
                             v.destination_city, v.destination_state_code, v.destination_state_name,
                             v.typical_distance_miles, v.base_rate_per_mile, v.fuel_surcharge_rate, v.typical_transit_days);
 
        INSERT INTO dim.dim_route
            (route_id, route_name, origin_city, origin_state_code, origin_state_name,
             destination_city, destination_state_code, destination_state_name,
             typical_distance_miles, base_rate_per_mile, fuel_surcharge_rate, typical_transit_days,
             valid_from_date_key, valid_to_date_key, is_current, is_deleted, version_reason)
        SELECT v.route_id, v.route_name, v.origin_city, v.origin_state_code, v.origin_state_name,
               v.destination_city, v.destination_state_code, v.destination_state_name,
               v.typical_distance_miles, v.base_rate_per_mile, v.fuel_surcharge_rate, v.typical_transit_days,
               @initial_from, 99991231, 1, v.is_deleted_in_source, 'Initial version'
        FROM dim.vw_src_route AS v
        WHERE NOT EXISTS (SELECT 1 FROM dim.dim_route AS t WHERE t.route_id = v.route_id);
 
        UPDATE t
        SET is_deleted = s.target_deleted
        FROM dim.dim_route AS t
        JOIN #route_status AS s ON s.route_id = t.route_id
        WHERE t.route_key > 0 AND t.is_deleted <> s.target_deleted;
 
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
 
    DROP TABLE IF EXISTS #route_status;
END;

GO