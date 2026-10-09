CREATE   PROCEDURE dim.usp_load_dim_trailer
    @allow_mass_delete BIT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @initial_from INT, @dupes INT, @nulls INT, @flagged INT, @active INT;
 
    IF @allow_mass_delete IS NULL
        THROW 50323, 'dim.usp_load_dim_trailer: @allow_mass_delete is required.', 1;
 
    SELECT @initial_from = MIN(d.date_key) FROM dim.dim_date AS d WHERE d.date_key > 0;
    IF @initial_from IS NULL
        THROW 50321, 'dim.usp_load_dim_trailer: load dim.dim_date first.', 1;
 
    SELECT @dupes = COUNT(*) - COUNT(DISTINCT v.trailer_id) FROM dim.vw_src_trailer AS v;
    IF @dupes <> 0
        THROW 50322, 'dim.usp_load_dim_trailer: duplicate trailer_id in silver.', 1;
 
    SELECT @nulls = COUNT(*) FROM dim.vw_src_trailer AS v WHERE v.is_deleted_in_source IS NULL;
    IF @nulls <> 0
        THROW 50326, 'dim.usp_load_dim_trailer: silver rows without _is_deleted_in_source.', 1;
 
    DROP TABLE IF EXISTS #trailer_status;
    CREATE TABLE #trailer_status (trailer_id VARCHAR(20) NOT NULL, target_deleted BIT NOT NULL)
        WITH (DISTRIBUTION = ROUND_ROBIN);
 
    INSERT INTO #trailer_status (trailer_id, target_deleted)
    SELECT m.trailer_id,
           CASE WHEN v.trailer_id IS NULL OR v.is_deleted_in_source = 1 THEN 1 ELSE 0 END
    FROM (SELECT DISTINCT d.trailer_id FROM dim.dim_trailer AS d WHERE d.trailer_key > 0) AS m
    LEFT JOIN dim.vw_src_trailer AS v ON v.trailer_id = m.trailer_id;
 
    SELECT @flagged = COUNT(*)
    FROM #trailer_status AS s
    JOIN dim.dim_trailer AS t ON t.trailer_id = s.trailer_id AND t.is_current = 1
    WHERE s.target_deleted = 1 AND t.is_deleted = 0;
 
    SELECT @active = COUNT(*) FROM dim.dim_trailer AS t
    WHERE t.trailer_key > 0 AND t.is_current = 1 AND t.is_deleted = 0;
 
    IF @allow_mass_delete = 0 AND @flagged > 0 AND @flagged * 10 > @active
        THROW 50325, 'dim.usp_load_dim_trailer: more than 10 percent of active trailers would be marked deleted; investigate silver, then rerun with @allow_mass_delete = 1 if genuine.', 1;
 
    BEGIN TRY
        BEGIN TRANSACTION;
 
        UPDATE t
        SET trailer_number           = v.trailer_number,
            trailer_type             = v.trailer_type,
            trailer_model_year       = v.trailer_model_year,
            trailer_acquisition_date = v.trailer_acquisition_date
        FROM dim.dim_trailer AS t
        JOIN dim.vw_src_trailer AS v ON v.trailer_id = t.trailer_id
        WHERE t.trailer_key > 0
          AND EXISTS (SELECT t.trailer_number, t.trailer_type, t.trailer_model_year, t.trailer_acquisition_date
                      EXCEPT
                      SELECT v.trailer_number, v.trailer_type, v.trailer_model_year, v.trailer_acquisition_date);
 
        INSERT INTO dim.dim_trailer
            (trailer_id, trailer_number, trailer_type, trailer_model_year, trailer_acquisition_date,
             valid_from_date_key, valid_to_date_key, is_current, is_deleted, version_reason)
        SELECT v.trailer_id, v.trailer_number, v.trailer_type, v.trailer_model_year, v.trailer_acquisition_date,
               @initial_from, 99991231, 1, v.is_deleted_in_source, 'Initial version'
        FROM dim.vw_src_trailer AS v
        WHERE NOT EXISTS (SELECT 1 FROM dim.dim_trailer AS t WHERE t.trailer_id = v.trailer_id);
 
        UPDATE t
        SET is_deleted = s.target_deleted
        FROM dim.dim_trailer AS t
        JOIN #trailer_status AS s ON s.trailer_id = t.trailer_id
        WHERE t.trailer_key > 0 AND t.is_deleted <> s.target_deleted;
 
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
 
    DROP TABLE IF EXISTS #trailer_status;
END;

GO