CREATE   PROCEDURE dim.usp_load_dim_facility
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @initial_from INT, @dupes INT;

    SELECT @initial_from = MIN(d.date_key) FROM dim.dim_date AS d WHERE d.date_key > 0;
    IF @initial_from IS NULL
        THROW 50341, 'dim.usp_load_dim_facility: load dim.dim_date first.', 1;

    SELECT @dupes = COUNT(*) - COUNT(DISTINCT v.facility_id) FROM dim.vw_src_facility AS v;
    IF @dupes <> 0
        THROW 50342, 'dim.usp_load_dim_facility: duplicate facility_id in silver.', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        UPDATE t
        SET facility_name          = v.facility_name,
            facility_type          = v.facility_type,
            facility_city          = v.facility_city,
            facility_state_code    = v.facility_state_code,
            facility_state_name    = v.facility_state_name,
            operating_hours        = v.operating_hours,
            dock_doors             = v.dock_doors,
            facility_latitude      = v.facility_latitude,
            facility_longitude     = v.facility_longitude,
            is_centroid_coordinate = v.is_centroid_coordinate
        FROM dim.dim_facility AS t
        JOIN dim.vw_src_facility AS v ON v.facility_id = t.facility_id
        WHERE t.facility_key > 0
          AND EXISTS (SELECT t.facility_name, t.facility_type, t.facility_city, t.facility_state_code, t.facility_state_name,
                             t.operating_hours, t.dock_doors, t.facility_latitude, t.facility_longitude, t.is_centroid_coordinate
                      EXCEPT
                      SELECT v.facility_name, v.facility_type, v.facility_city, v.facility_state_code, v.facility_state_name,
                             v.operating_hours, v.dock_doors, v.facility_latitude, v.facility_longitude, v.is_centroid_coordinate);

        INSERT INTO dim.dim_facility
            (facility_id, facility_name, facility_type, facility_city, facility_state_code, facility_state_name,
             operating_hours, dock_doors, facility_latitude, facility_longitude, is_centroid_coordinate,
             valid_from_date_key, valid_to_date_key, is_current, version_reason)
        SELECT v.facility_id, v.facility_name, v.facility_type, v.facility_city, v.facility_state_code, v.facility_state_name,
               v.operating_hours, v.dock_doors, v.facility_latitude, v.facility_longitude, v.is_centroid_coordinate,
               @initial_from, 99991231, 1, 'Initial version'
        FROM dim.vw_src_facility AS v
        WHERE NOT EXISTS (SELECT 1 FROM dim.dim_facility AS t WHERE t.facility_id = v.facility_id);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;

GO