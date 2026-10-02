CREATE   PROCEDURE dim.usp_load_dim_trailer
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @initial_from INT, @dupes INT;

    SELECT @initial_from = MIN(d.date_key) FROM dim.dim_date AS d WHERE d.date_key > 0;
    IF @initial_from IS NULL
        THROW 50321, 'dim.usp_load_dim_trailer: load dim.dim_date first.', 1;

    SELECT @dupes = COUNT(*) - COUNT(DISTINCT v.trailer_id) FROM dim.vw_src_trailer AS v;
    IF @dupes <> 0
        THROW 50322, 'dim.usp_load_dim_trailer: duplicate trailer_id in silver.', 1;

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
             valid_from_date_key, valid_to_date_key, is_current, version_reason)
        SELECT v.trailer_id, v.trailer_number, v.trailer_type, v.trailer_model_year, v.trailer_acquisition_date,
               @initial_from, 99991231, 1, 'Initial version'
        FROM dim.vw_src_trailer AS v
        WHERE NOT EXISTS (SELECT 1 FROM dim.dim_trailer AS t WHERE t.trailer_id = v.trailer_id);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;

GO