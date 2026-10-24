/* Check 01: every row-per-key fact holds exactly silver's rows and values.

   For fact_trip, fact_delivery_event, fact_fuel_purchase, fact_maintenance and
   fact_safety_incident: gold has one row per silver row and key, the same number
   of live rows, and every carried column equal to silver's in both directions
   (EXCEPT each way). fact_load_lifecycle is covered by check 07.

   Run in: wh_logistics_gold. Read only.
   Pass:   no rows. Each returned row names a failing fact with its counts. */

WITH c AS (
    SELECT 'fact_trip' AS fact,
        (SELECT COUNT(*) FROM lh_logistics_silver.dbo.silver_trips) AS silver_rows,
        (SELECT COUNT(*) FROM fact.fact_trip) AS gold_rows,
        (SELECT COUNT(DISTINCT f.trip_id) FROM fact.fact_trip AS f) AS gold_keys,
        (SELECT COUNT(*) FROM lh_logistics_silver.dbo.silver_trips AS s WHERE s._is_deleted_in_source = 0) AS silver_active,
        (SELECT COUNT(*) FROM fact.fact_trip AS f WHERE f.is_deleted_in_source = 0) AS gold_active,
        (SELECT COUNT(*) FROM (
            SELECT f.trip_id, f.load_id, f.actual_distance_miles, f.actual_duration_hours,
                   f.fuel_gallons_used, f.idle_time_hours, f.is_idle_implausible, f.is_deleted_in_source
            FROM fact.fact_trip AS f
            EXCEPT
            SELECT s.trip_id, s.load_id, CAST(s.actual_distance_miles AS INT),
                   CAST(s.actual_duration_hours AS DECIMAL(10,2)), CAST(s.fuel_gallons_used AS DECIMAL(12,2)),
                   CAST(s.idle_time_hours AS DECIMAL(10,2)), CAST(s.is_idle_implausible AS BIT),
                   CAST(s._is_deleted_in_source AS BIT)
            FROM lh_logistics_silver.dbo.silver_trips AS s) AS x) AS gold_not_in_silver,
        (SELECT COUNT(*) FROM (
            SELECT s.trip_id, s.load_id, CAST(s.actual_distance_miles AS INT),
                   CAST(s.actual_duration_hours AS DECIMAL(10,2)), CAST(s.fuel_gallons_used AS DECIMAL(12,2)),
                   CAST(s.idle_time_hours AS DECIMAL(10,2)), CAST(s.is_idle_implausible AS BIT),
                   CAST(s._is_deleted_in_source AS BIT)
            FROM lh_logistics_silver.dbo.silver_trips AS s
            EXCEPT
            SELECT f.trip_id, f.load_id, f.actual_distance_miles, f.actual_duration_hours,
                   f.fuel_gallons_used, f.idle_time_hours, f.is_idle_implausible, f.is_deleted_in_source
            FROM fact.fact_trip AS f) AS y) AS silver_not_in_gold

    UNION ALL
    SELECT 'fact_delivery_event',
        (SELECT COUNT(*) FROM lh_logistics_silver.dbo.silver_delivery_events),
        (SELECT COUNT(*) FROM fact.fact_delivery_event),
        (SELECT COUNT(DISTINCT f.event_id) FROM fact.fact_delivery_event AS f),
        (SELECT COUNT(*) FROM lh_logistics_silver.dbo.silver_delivery_events AS s WHERE s._is_deleted_in_source = 0),
        (SELECT COUNT(*) FROM fact.fact_delivery_event AS f WHERE f.is_deleted_in_source = 0),
        (SELECT COUNT(*) FROM (
            SELECT f.event_id, f.load_id, f.trip_id, f.scheduled_datetime, f.actual_datetime, f.detention_minutes,
                   f.billable_detention_minutes, f.arrival_variance_minutes, f.is_deleted_in_source
            FROM fact.fact_delivery_event AS f
            EXCEPT
            SELECT s.event_id, s.load_id, s.trip_id, CAST(s.scheduled_datetime AS DATETIME2(6)), CAST(s.actual_datetime AS DATETIME2(6)),
                   CAST(s.detention_minutes AS INT), CAST(s.billable_detention_minutes AS INT),
                   CAST(s.arrival_variance_minutes AS DECIMAL(10,1)), CAST(s._is_deleted_in_source AS BIT)
            FROM lh_logistics_silver.dbo.silver_delivery_events AS s) AS x),
        (SELECT COUNT(*) FROM (
            SELECT s.event_id, s.load_id, s.trip_id, CAST(s.scheduled_datetime AS DATETIME2(6)), CAST(s.actual_datetime AS DATETIME2(6)),
                   CAST(s.detention_minutes AS INT), CAST(s.billable_detention_minutes AS INT),
                   CAST(s.arrival_variance_minutes AS DECIMAL(10,1)), CAST(s._is_deleted_in_source AS BIT)
            FROM lh_logistics_silver.dbo.silver_delivery_events AS s
            EXCEPT
            SELECT f.event_id, f.load_id, f.trip_id, f.scheduled_datetime, f.actual_datetime, f.detention_minutes,
                   f.billable_detention_minutes, f.arrival_variance_minutes, f.is_deleted_in_source
            FROM fact.fact_delivery_event AS f) AS y)

    UNION ALL
    SELECT 'fact_fuel_purchase',
        (SELECT COUNT(*) FROM lh_logistics_silver.dbo.silver_fuel_purchases),
        (SELECT COUNT(*) FROM fact.fact_fuel_purchase),
        (SELECT COUNT(DISTINCT f.fuel_purchase_id) FROM fact.fact_fuel_purchase AS f),
        (SELECT COUNT(*) FROM lh_logistics_silver.dbo.silver_fuel_purchases AS s WHERE s._is_deleted_in_source = 0),
        (SELECT COUNT(*) FROM fact.fact_fuel_purchase AS f WHERE f.is_deleted_in_source = 0),
        (SELECT COUNT(*) FROM (
            SELECT f.fuel_purchase_id, f.trip_id, f.gallons, f.total_cost, f.is_capacity_exceeded, f.is_deleted_in_source
            FROM fact.fact_fuel_purchase AS f
            EXCEPT
            SELECT s.fuel_purchase_id, s.trip_id, CAST(s.gallons AS DECIMAL(10,2)), CAST(s.total_cost AS DECIMAL(14,2)),
                   CAST(s.is_capacity_exceeded AS BIT), CAST(s._is_deleted_in_source AS BIT)
            FROM lh_logistics_silver.dbo.silver_fuel_purchases AS s) AS x),
        (SELECT COUNT(*) FROM (
            SELECT s.fuel_purchase_id, s.trip_id, CAST(s.gallons AS DECIMAL(10,2)), CAST(s.total_cost AS DECIMAL(14,2)),
                   CAST(s.is_capacity_exceeded AS BIT), CAST(s._is_deleted_in_source AS BIT)
            FROM lh_logistics_silver.dbo.silver_fuel_purchases AS s
            EXCEPT
            SELECT f.fuel_purchase_id, f.trip_id, f.gallons, f.total_cost, f.is_capacity_exceeded, f.is_deleted_in_source
            FROM fact.fact_fuel_purchase AS f) AS y)

    UNION ALL
    SELECT 'fact_maintenance',
        (SELECT COUNT(*) FROM lh_logistics_silver.dbo.silver_maintenance_records),
        (SELECT COUNT(*) FROM fact.fact_maintenance),
        (SELECT COUNT(DISTINCT f.maintenance_id) FROM fact.fact_maintenance AS f),
        (SELECT COUNT(*) FROM lh_logistics_silver.dbo.silver_maintenance_records AS s WHERE s._is_deleted_in_source = 0),
        (SELECT COUNT(*) FROM fact.fact_maintenance AS f WHERE f.is_deleted_in_source = 0),
        (SELECT COUNT(*) FROM (
            SELECT f.maintenance_id, f.labor_hours, f.labor_cost, f.parts_cost, f.downtime_hours, f.odometer_reading, f.is_deleted_in_source
            FROM fact.fact_maintenance AS f
            EXCEPT
            SELECT s.maintenance_id, CAST(s.labor_hours AS DECIMAL(8,2)), CAST(s.labor_cost AS DECIMAL(14,2)),
                   CAST(s.parts_cost AS DECIMAL(14,2)), CAST(s.downtime_hours AS DECIMAL(10,2)),
                   CAST(s.odometer_reading AS INT), CAST(s._is_deleted_in_source AS BIT)
            FROM lh_logistics_silver.dbo.silver_maintenance_records AS s) AS x),
        (SELECT COUNT(*) FROM (
            SELECT s.maintenance_id, CAST(s.labor_hours AS DECIMAL(8,2)), CAST(s.labor_cost AS DECIMAL(14,2)),
                   CAST(s.parts_cost AS DECIMAL(14,2)), CAST(s.downtime_hours AS DECIMAL(10,2)),
                   CAST(s.odometer_reading AS INT), CAST(s._is_deleted_in_source AS BIT)
            FROM lh_logistics_silver.dbo.silver_maintenance_records AS s
            EXCEPT
            SELECT f.maintenance_id, f.labor_hours, f.labor_cost, f.parts_cost, f.downtime_hours, f.odometer_reading, f.is_deleted_in_source
            FROM fact.fact_maintenance AS f) AS y)

    UNION ALL
    SELECT 'fact_safety_incident',
        (SELECT COUNT(*) FROM lh_logistics_silver.dbo.silver_safety_incidents),
        (SELECT COUNT(*) FROM fact.fact_safety_incident),
        (SELECT COUNT(DISTINCT f.incident_id) FROM fact.fact_safety_incident AS f),
        (SELECT COUNT(*) FROM lh_logistics_silver.dbo.silver_safety_incidents AS s WHERE s._is_deleted_in_source = 0),
        (SELECT COUNT(*) FROM fact.fact_safety_incident AS f WHERE f.is_deleted_in_source = 0),
        (SELECT COUNT(*) FROM (
            SELECT f.incident_id, f.trip_id, f.vehicle_damage_cost, f.cargo_damage_cost, f.is_deleted_in_source
            FROM fact.fact_safety_incident AS f
            EXCEPT
            SELECT s.incident_id, s.trip_id, CAST(s.vehicle_damage_cost AS DECIMAL(14,2)),
                   CAST(s.cargo_damage_cost AS DECIMAL(14,2)), CAST(s._is_deleted_in_source AS BIT)
            FROM lh_logistics_silver.dbo.silver_safety_incidents AS s) AS x),
        (SELECT COUNT(*) FROM (
            SELECT s.incident_id, s.trip_id, CAST(s.vehicle_damage_cost AS DECIMAL(14,2)),
                   CAST(s.cargo_damage_cost AS DECIMAL(14,2)), CAST(s._is_deleted_in_source AS BIT)
            FROM lh_logistics_silver.dbo.silver_safety_incidents AS s
            EXCEPT
            SELECT f.incident_id, f.trip_id, f.vehicle_damage_cost, f.cargo_damage_cost, f.is_deleted_in_source
            FROM fact.fact_safety_incident AS f) AS y)
)
SELECT c.*
FROM c
WHERE NOT (    c.gold_rows = c.silver_rows
           AND c.gold_keys = c.gold_rows
           AND c.gold_active = c.silver_active
           AND c.gold_not_in_silver = 0
           AND c.silver_not_in_gold = 0);
