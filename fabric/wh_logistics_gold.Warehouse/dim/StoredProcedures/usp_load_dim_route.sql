CREATE   PROCEDURE dim.usp_load_dim_route
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @initial_from INT, @dupes INT;

    SELECT @initial_from = MIN(d.date_key) FROM dim.dim_date AS d WHERE d.date_key > 0;
    IF @initial_from IS NULL
        THROW 50331, 'dim.usp_load_dim_route: load dim.dim_date first.', 1;

    SELECT @dupes = COUNT(*) - COUNT(DISTINCT v.route_id) FROM dim.vw_src_route AS v;
    IF @dupes <> 0
        THROW 50332, 'dim.usp_load_dim_route: duplicate route_id in silver.', 1;

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
             valid_from_date_key, valid_to_date_key, is_current, version_reason)
        SELECT v.route_id, v.route_name, v.origin_city, v.origin_state_code, v.origin_state_name,
               v.destination_city, v.destination_state_code, v.destination_state_name,
               v.typical_distance_miles, v.base_rate_per_mile, v.fuel_surcharge_rate, v.typical_transit_days,
               @initial_from, 99991231, 1, 'Initial version'
        FROM dim.vw_src_route AS v
        WHERE NOT EXISTS (SELECT 1 FROM dim.dim_route AS t WHERE t.route_id = v.route_id);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;

GO