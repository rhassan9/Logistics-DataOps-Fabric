-- ============================================================
-- Load the 12 source CSVs into the OLTP schema.
-- Run with psql from INSIDE the folder containing the CSV files:
--     psql "$NEON_URL" -f 02_load_data.sql
--
-- Order matters: reference tables first, then loads, then trips,
-- then everything that points at trips.s
-- \copy runs client-side, so the file paths are local to your Mac.
-- ============================================================

\echo '--- reference tables ---'

\copy customers   FROM 'customers.csv'   WITH (FORMAT csv, HEADER true, NULL '')
\copy facilities  FROM 'facilities.csv'  WITH (FORMAT csv, HEADER true, NULL '')
\copy drivers     FROM 'drivers.csv'     WITH (FORMAT csv, HEADER true, NULL '')
\copy trucks      FROM 'trucks.csv'      WITH (FORMAT csv, HEADER true, NULL '')
\copy trailers    FROM 'trailers.csv'    WITH (FORMAT csv, HEADER true, NULL '')
\copy routes      FROM 'routes.csv'      WITH (FORMAT csv, HEADER true, NULL '')

\echo '--- loads ---'
\copy loads FROM 'loads.csv' WITH (FORMAT csv, HEADER true, NULL '')

\echo '--- trips ---'
\copy trips FROM 'trips.csv' WITH (FORMAT csv, HEADER true, NULL '')

\echo '--- dependent transactional tables ---'
\copy maintenance_records FROM 'maintenance_records.csv' WITH (FORMAT csv, HEADER true, NULL '')
\copy safety_incidents    FROM 'safety_incidents.csv'    WITH (FORMAT csv, HEADER true, NULL '')
\copy delivery_events     FROM 'delivery_events.csv'     WITH (FORMAT csv, HEADER true, NULL '')
\copy fuel_purchases      FROM 'fuel_purchases.csv'      WITH (FORMAT csv, HEADER true, NULL '')

\echo '--- row counts ---'

SELECT 'customers'           AS table_name, count(*) FROM customers
UNION ALL SELECT 'facilities',           count(*) FROM facilities
UNION ALL SELECT 'drivers',              count(*) FROM drivers
UNION ALL SELECT 'trucks',               count(*) FROM trucks
UNION ALL SELECT 'trailers',             count(*) FROM trailers
UNION ALL SELECT 'routes',               count(*) FROM routes
UNION ALL SELECT 'loads',                count(*) FROM loads
UNION ALL SELECT 'trips',                count(*) FROM trips
UNION ALL SELECT 'maintenance_records',  count(*) FROM maintenance_records
UNION ALL SELECT 'safety_incidents',     count(*) FROM safety_incidents
UNION ALL SELECT 'delivery_events',      count(*) FROM delivery_events
UNION ALL SELECT 'fuel_purchases',       count(*) FROM fuel_purchases
ORDER BY table_name;

\echo '--- expected: drivers 150, trucks 120, trailers 180, customers 200,'
\echo '--- facilities 50, routes 60+, loads 57000+, trips 57000+,'
\echo '--- fuel_purchases 131000+, maintenance 6500+, events 114000+, incidents 114'

ANALYZE;
