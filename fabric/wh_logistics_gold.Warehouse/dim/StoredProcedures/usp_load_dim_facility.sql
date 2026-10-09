CREATE   PROCEDURE dim.usp_load_dim_facility
    @allow_mass_delete BIT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @initial_from INT, @dupes INT, @nulls INT, @flagged INT, @active INT;
 
    IF @allow_mass_delete IS NULL
        THROW 50343, 'dim.usp_load_dim_facility: @allow_mass_delete is required.', 1;
 
    SELECT @initial_from = MIN(d.date_key) FROM dim.dim_date AS d WHERE d.date_key > 0;
    IF @initial_from IS NULL
        THROW 50341, 'dim.usp_load_dim_facility: load dim.dim_date first.', 1;
 
    SELECT @dupes = COUNT(*) - COUNT(DISTINCT v.facility_id) FROM dim.vw_src_facility AS v;
    IF @dupes <> 0
        THROW 50342, 'dim.usp_load_dim_facility: duplicate facility_id in silver.', 1;
 
    SELECT @nulls = COUNT(*) FROM dim.vw_src_facility AS v WHERE v.is_deleted_in_source IS NULL;
    IF @nulls <> 0
        THROW 50346, 'dim.usp_load_dim_facility: silver rows without _is_deleted_in_source.', 1;
 
    DROP TABLE IF EXISTS #facility_status;
    CREATE TABLE #facility_status (facility_id VARCHAR(20) NOT NULL, target_deleted BIT NOT NULL)
        WITH (DISTRIBUTION = ROUND_ROBIN);
 
    INSERT INTO #facility_status (facility_id, target_deleted)
    SELECT m.facility_id,
           CASE WHEN v.facility_id IS NULL OR v.is_deleted_in_source = 1 THEN 1 ELSE 0 END
    FROM (SELECT DISTINCT d.facility_id FROM dim.dim_facility AS d WHERE d.facility_key > 0) AS m
    LEFT JOIN dim.vw_src_facility AS v ON v.facility_id = m.facility_id;
 
    SELECT @flagged = COUNT(*)
    FROM #facility_status AS s
    JOIN dim.dim_facility AS t ON t.facility_id = s.facility_id AND t.is_current = 1
    WHERE s.target_deleted = 1 AND t.is_deleted = 0;
 
    SELECT @active = COUNT(*) FROM dim.dim_facility AS t
    WHERE t.facility_key > 0 AND t.is_current = 1 AND t.is_deleted = 0;
 
    IF @allow_mass_delete = 0 AND @flagged > 0 AND @flagged * 10 > @active
        THROW 50345, 'dim.usp_load_dim_facility: more than 10 percent of active facilities would be marked deleted; investigate silver, then rerun with @allow_mass_delete = 1 if genuine.', 1;
 
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
             valid_from_date_key, valid_to_date_key, is_current, is_deleted, version_reason)
        SELECT v.facility_id, v.facility_name, v.facility_type, v.facility_city, v.facility_state_code, v.facility_state_name,
               v.operating_hours, v.dock_doors, v.facility_latitude, v.facility_longitude, v.is_centroid_coordinate,
               @initial_from, 99991231, 1, v.is_deleted_in_source, 'Initial version'
        FROM dim.vw_src_facility AS v
        WHERE NOT EXISTS (SELECT 1 FROM dim.dim_facility AS t WHERE t.facility_id = v.facility_id);
 
        UPDATE t
        SET is_deleted = s.target_deleted
        FROM dim.dim_facility AS t
        JOIN #facility_status AS s ON s.facility_id = t.facility_id
        WHERE t.facility_key > 0 AND t.is_deleted <> s.target_deleted;
 
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
 
    DROP TABLE IF EXISTS #facility_status;
END;

GO