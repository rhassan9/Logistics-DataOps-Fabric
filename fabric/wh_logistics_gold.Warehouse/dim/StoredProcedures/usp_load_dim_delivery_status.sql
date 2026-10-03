CREATE   PROCEDURE dim.usp_load_dim_delivery_status
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dim.dim_delivery_status (event_type, arrival_status)
    SELECT DISTINCT v.event_type, v.arrival_status
    FROM dim.vw_src_delivery_status AS v
    WHERE NOT EXISTS (SELECT 1 FROM dim.dim_delivery_status AS j
                      WHERE j.event_type = v.event_type AND j.arrival_status = v.arrival_status);
END;

GO