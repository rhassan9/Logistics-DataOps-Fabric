CREATE   VIEW dim.vw_src_delivery_status
AS
SELECT e.event_id,
       COALESCE(e.event_type,     'Not Recorded') AS event_type,
       COALESCE(e.arrival_status, 'Not Recorded') AS arrival_status
FROM lh_logistics_silver.dbo.silver_delivery_events AS e;

GO