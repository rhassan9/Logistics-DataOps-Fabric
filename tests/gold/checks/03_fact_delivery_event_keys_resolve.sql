/* Check 03: every fact_delivery_event key points at the right dimension row.

   Dates must be the event's own YYYYMMDD key (Missing when null). Facility and
   route resolve on the version valid at the event's date: actual, else
   scheduled. Route comes from the event's own load. The stop status key must
   point at the junk row whose attributes equal the event's.

   rows_with_unknown is reported, not asserted.

   Run in: wh_logistics_gold. Read only.
   Pass:   no rows. */

WITH s AS (
    SELECT e.event_id, e.facility_id, e.load_id, e.scheduled_datetime, e.actual_datetime,
           l.load_id AS load_found, l.route_id, v.event_type, v.arrival_status,
           CASE WHEN COALESCE(e.actual_datetime, e.scheduled_datetime) IS NULL THEN 0
                ELSE YEAR(COALESCE(e.actual_datetime, e.scheduled_datetime)) * 10000
                   + MONTH(COALESCE(e.actual_datetime, e.scheduled_datetime)) * 100
                   + DAY(COALESCE(e.actual_datetime, e.scheduled_datetime)) END AS event_key
    FROM lh_logistics_silver.dbo.silver_delivery_events AS e
    LEFT JOIN lh_logistics_silver.dbo.silver_loads AS l ON l.load_id = e.load_id
    LEFT JOIN dim.vw_src_delivery_status AS v ON v.event_id = e.event_id
),
r AS (
    SELECT
        SUM(CASE WHEN f.scheduled_date_key = CASE WHEN s.scheduled_datetime IS NULL THEN 0
                 ELSE YEAR(s.scheduled_datetime) * 10000 + MONTH(s.scheduled_datetime) * 100 + DAY(s.scheduled_datetime) END
                 THEN 0 ELSE 1 END) AS bad_scheduled_date,
        SUM(CASE WHEN f.actual_date_key = CASE WHEN s.actual_datetime IS NULL THEN 0
                 ELSE YEAR(s.actual_datetime) * 10000 + MONTH(s.actual_datetime) * 100 + DAY(s.actual_datetime) END
                 THEN 0 ELSE 1 END) AS bad_actual_date,
        SUM(CASE WHEN s.facility_id IS NULL THEN CASE WHEN f.facility_key = 0 THEN 0 ELSE 1 END
                 WHEN fc.facility_id = s.facility_id AND s.event_key >= fc.valid_from_date_key
                      AND s.event_key < fc.valid_to_date_key THEN 0
                 WHEN f.facility_key = -1 AND fcm.facility_id IS NULL THEN 0
                 ELSE 1 END) AS bad_facility,
        SUM(CASE WHEN s.load_id IS NULL    THEN CASE WHEN f.route_key = 0  THEN 0 ELSE 1 END
                 WHEN s.load_found IS NULL THEN CASE WHEN f.route_key = -1 THEN 0 ELSE 1 END
                 WHEN s.route_id IS NULL   THEN CASE WHEN f.route_key = 0  THEN 0 ELSE 1 END
                 WHEN rt.route_id = s.route_id AND s.event_key >= rt.valid_from_date_key
                      AND s.event_key < rt.valid_to_date_key THEN 0
                 WHEN f.route_key = -1 AND rtm.route_id IS NULL THEN 0
                 ELSE 1 END) AS bad_route,
        SUM(CASE WHEN ds.event_type = s.event_type AND ds.arrival_status = s.arrival_status
                  AND f.delivery_status_key > 0 THEN 0 ELSE 1 END) AS bad_delivery_status,
        SUM(CASE WHEN -1 IN (f.scheduled_date_key, f.actual_date_key, f.facility_key, f.route_key,
                             f.delivery_status_key) THEN 1 ELSE 0 END) AS rows_with_unknown
    FROM fact.fact_delivery_event AS f
    JOIN s ON s.event_id = f.event_id
    LEFT JOIN dim.dim_facility AS fc ON fc.facility_key = f.facility_key
    LEFT JOIN dim.dim_route AS rt ON rt.route_key = f.route_key
    LEFT JOIN dim.dim_delivery_status AS ds ON ds.delivery_status_key = f.delivery_status_key
    LEFT JOIN (SELECT DISTINCT d.facility_id FROM dim.dim_facility AS d WHERE d.facility_key > 0) AS fcm ON fcm.facility_id = s.facility_id
    LEFT JOIN (SELECT DISTINCT d.route_id    FROM dim.dim_route    AS d WHERE d.route_key    > 0) AS rtm ON rtm.route_id    = s.route_id
)
SELECT 'fact_delivery_event' AS fact, r.*
FROM r
WHERE r.bad_scheduled_date > 0 OR r.bad_actual_date > 0 OR r.bad_facility > 0
   OR r.bad_route > 0 OR r.bad_delivery_status > 0;
