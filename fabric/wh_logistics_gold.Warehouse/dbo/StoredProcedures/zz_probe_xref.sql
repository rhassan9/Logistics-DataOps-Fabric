CREATE PROCEDURE dbo.zz_probe_xref
AS
BEGIN
    SELECT t.trip_id, l.load_id
    FROM lh_logistics_silver.dbo.silver_trips AS t
    JOIN lh_logistics_silver.dbo.silver_loads AS l
      ON l.load_id = t.load_id
    WHERE 1 = 0;
END;

GO