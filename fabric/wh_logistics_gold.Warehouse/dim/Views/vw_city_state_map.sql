CREATE   VIEW dim.vw_city_state_map
AS
SELECT p.city                     AS location_city,
       MIN(p.state_code)          AS location_state_code,
       CASE MIN(p.source_rank) WHEN 1 THEN 'silver_facilities'
                               WHEN 2 THEN 'silver_routes'
                               ELSE 'silver_delivery_events' END AS state_source
FROM
(
    SELECT f.city AS city, f.state AS state_code, 1 AS source_rank
    FROM lh_logistics_silver.dbo.silver_facilities AS f
    UNION ALL
    SELECT r.origin_city, r.origin_state, 2 FROM lh_logistics_silver.dbo.silver_routes AS r
    UNION ALL
    SELECT r.destination_city, r.destination_state, 2 FROM lh_logistics_silver.dbo.silver_routes AS r
    UNION ALL
    SELECT e.location_city, e.location_state, 3 FROM lh_logistics_silver.dbo.silver_delivery_events AS e
) AS p
WHERE p.city IS NOT NULL AND p.state_code IS NOT NULL
GROUP BY p.city
HAVING COUNT(DISTINCT p.state_code) = 1;

GO