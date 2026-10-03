CREATE   PROCEDURE dim.usp_load_dim_location
AS
BEGIN
    SET NOCOUNT ON;
    -- Only cities whose state is resolved by trusted tables; unresolved cities get no row,
    -- so their facts resolve to -1 Unknown.
    INSERT INTO dim.dim_location
        (location_city, location_state_code, location_state_name, location_country, state_source)
    SELECT v.location_city, v.location_state_code, v.location_state_name, 'United States', v.state_source
    FROM dim.vw_src_location AS v
    WHERE v.location_state_code IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM dim.dim_location AS d WHERE d.location_city = v.location_city);
END;

GO