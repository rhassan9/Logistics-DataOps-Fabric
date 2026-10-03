CREATE   PROCEDURE dim.usp_load_dim_load_type
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dim.dim_load_type (booking_type, load_type)
    SELECT DISTINCT v.booking_type, v.load_type
    FROM dim.vw_src_load_type AS v
    WHERE NOT EXISTS (SELECT 1 FROM dim.dim_load_type AS j
                      WHERE j.booking_type = v.booking_type AND j.load_type = v.load_type);
END;

GO