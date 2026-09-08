-- ============================================================
-- Logistics Operations Database - OLTP source schema
-- Target: PostgreSQL (Neon)
-- 12 transactional/reference tables.
-- Aggregate tables (driver_monthly_metrics, truck_utilization_metrics)
-- are deliberately EXCLUDED: they are OLAP artifacts and will be
-- rebuilt in the gold layer from these transactional tables.
-- ============================================================

DROP TABLE IF EXISTS safety_incidents CASCADE;
DROP TABLE IF EXISTS delivery_events CASCADE;
DROP TABLE IF EXISTS maintenance_records CASCADE;
DROP TABLE IF EXISTS fuel_purchases CASCADE;
DROP TABLE IF EXISTS trips CASCADE;
DROP TABLE IF EXISTS loads CASCADE;
DROP TABLE IF EXISTS routes CASCADE;
DROP TABLE IF EXISTS trailers CASCADE;
DROP TABLE IF EXISTS trucks CASCADE;
DROP TABLE IF EXISTS drivers CASCADE;
DROP TABLE IF EXISTS facilities CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- ------------------------------------------------------------
-- REFERENCE TABLES (no dependencies)
-- ------------------------------------------------------------

CREATE TABLE customers (
    customer_id                VARCHAR(12) PRIMARY KEY,
    customer_name              TEXT        NOT NULL,
    customer_type              TEXT,
    credit_terms_days          INTEGER,
    primary_freight_type       TEXT,
    account_status             TEXT,
    contract_start_date        DATE,
    annual_revenue_potential   NUMERIC(14,2)
);

CREATE TABLE facilities (
    facility_id       VARCHAR(12) PRIMARY KEY,
    facility_name     TEXT NOT NULL,
    facility_type     TEXT,
    city              TEXT,
    state             VARCHAR(4),
    latitude          NUMERIC(10,6),
    longitude         NUMERIC(10,6),
    dock_doors        INTEGER,
    operating_hours   TEXT
);

CREATE TABLE drivers (
    driver_id           VARCHAR(12) PRIMARY KEY,
    first_name          TEXT,
    last_name           TEXT,
    hire_date           DATE,
    termination_date    DATE,          -- nullable: active drivers
    license_number      TEXT,
    license_state       VARCHAR(4),
    date_of_birth       DATE,
    home_terminal       TEXT,
    employment_status   TEXT,
    cdl_class           VARCHAR(4),
    years_experience    INTEGER
);

CREATE TABLE trucks (
    truck_id                VARCHAR(12) PRIMARY KEY,
    unit_number             TEXT,
    make                    TEXT,
    model_year              INTEGER,
    vin                     TEXT,
    acquisition_date        DATE,
    acquisition_mileage     INTEGER,
    fuel_type               TEXT,
    tank_capacity_gallons   INTEGER,
    status                  TEXT,
    home_terminal           TEXT
);

CREATE TABLE trailers (
    trailer_id         VARCHAR(12) PRIMARY KEY,
    trailer_number     TEXT,
    trailer_type       TEXT,
    length_feet        INTEGER,
    model_year         INTEGER,
    vin                TEXT,
    acquisition_date   DATE,
    status             TEXT,
    current_location   TEXT
);

CREATE TABLE routes (
    route_id                 VARCHAR(12) PRIMARY KEY,
    origin_city              TEXT,
    origin_state             VARCHAR(4),
    destination_city         TEXT,
    destination_state        VARCHAR(4),
    typical_distance_miles   INTEGER,
    base_rate_per_mile       NUMERIC(8,4),
    fuel_surcharge_rate      NUMERIC(8,4),
    typical_transit_days     INTEGER
);

-- ------------------------------------------------------------
-- TRANSACTIONAL TABLES
-- ------------------------------------------------------------

CREATE TABLE loads (
    load_id               VARCHAR(16) PRIMARY KEY,
    customer_id           VARCHAR(12) REFERENCES customers(customer_id),
    route_id              VARCHAR(12) REFERENCES routes(route_id),
    load_date             DATE,
    load_type             TEXT,
    weight_lbs            INTEGER,
    pieces                INTEGER,
    revenue               NUMERIC(14,2),
    fuel_surcharge        NUMERIC(14,2),
    accessorial_charges   NUMERIC(14,2),
    load_status           TEXT,
    booking_type          TEXT
);

-- driver_id / truck_id / trailer_id are nullable by design:
-- the source carries an intentional ~2% unassigned rate.
CREATE TABLE trips (
    trip_id                  VARCHAR(16) PRIMARY KEY,
    load_id                  VARCHAR(16) REFERENCES loads(load_id),
    driver_id                VARCHAR(12) REFERENCES drivers(driver_id),
    truck_id                 VARCHAR(12) REFERENCES trucks(truck_id),
    trailer_id               VARCHAR(12) REFERENCES trailers(trailer_id),
    dispatch_date            DATE,
    actual_distance_miles    INTEGER,
    actual_duration_hours    NUMERIC(10,2),
    fuel_gallons_used        NUMERIC(12,2),
    average_mpg              NUMERIC(8,2),
    idle_time_hours          NUMERIC(10,2),
    trip_status              TEXT
);

CREATE TABLE fuel_purchases (
    fuel_purchase_id    VARCHAR(16) PRIMARY KEY,
    trip_id             VARCHAR(16) REFERENCES trips(trip_id),
    truck_id            VARCHAR(12) REFERENCES trucks(truck_id),
    driver_id           VARCHAR(12) REFERENCES drivers(driver_id),
    purchase_date       TIMESTAMP,
    location_city       TEXT,
    location_state      VARCHAR(4),
    gallons             NUMERIC(10,2),
    price_per_gallon    NUMERIC(10,3),
    total_cost          NUMERIC(14,2),
    fuel_card_number    TEXT
);

CREATE TABLE maintenance_records (
    maintenance_id        VARCHAR(16) PRIMARY KEY,
    truck_id              VARCHAR(12) REFERENCES trucks(truck_id),
    maintenance_date      DATE,
    maintenance_type      TEXT,
    odometer_reading      INTEGER,
    labor_hours           NUMERIC(8,2),
    labor_cost            NUMERIC(14,2),
    parts_cost            NUMERIC(14,2),
    total_cost            NUMERIC(14,2),
    facility_location     TEXT,
    downtime_hours        NUMERIC(10,2),
    service_description   TEXT
);

CREATE TABLE delivery_events (
    event_id             VARCHAR(16) PRIMARY KEY,
    load_id              VARCHAR(16) REFERENCES loads(load_id),
    trip_id              VARCHAR(16) REFERENCES trips(trip_id),
    event_type           TEXT,
    facility_id          VARCHAR(12) REFERENCES facilities(facility_id),
    scheduled_datetime   TIMESTAMP,
    actual_datetime      TIMESTAMP,
    detention_minutes    INTEGER,
    on_time_flag         BOOLEAN,
    location_city        TEXT,
    location_state       VARCHAR(4)
);

CREATE TABLE safety_incidents (
    incident_id           VARCHAR(16) PRIMARY KEY,
    trip_id               VARCHAR(16) REFERENCES trips(trip_id),
    truck_id              VARCHAR(12) REFERENCES trucks(truck_id),
    driver_id             VARCHAR(12) REFERENCES drivers(driver_id),
    incident_date         TIMESTAMP,
    incident_type         TEXT,
    location_city         TEXT,
    location_state        VARCHAR(4),
    at_fault_flag         BOOLEAN,
    injury_flag           BOOLEAN,
    vehicle_damage_cost   NUMERIC(14,2),
    cargo_damage_cost     NUMERIC(14,2),
    claim_amount          NUMERIC(14,2),
    preventable_flag      BOOLEAN,
    description           TEXT
);

-- ------------------------------------------------------------
-- INDEXES on foreign keys and common filter columns.
-- Postgres indexes primary keys automatically but NOT foreign keys.
-- ------------------------------------------------------------

CREATE INDEX idx_loads_customer          ON loads(customer_id);
CREATE INDEX idx_loads_route             ON loads(route_id);
CREATE INDEX idx_loads_date              ON loads(load_date);

CREATE INDEX idx_trips_load              ON trips(load_id);
CREATE INDEX idx_trips_driver            ON trips(driver_id);
CREATE INDEX idx_trips_truck             ON trips(truck_id);
CREATE INDEX idx_trips_trailer           ON trips(trailer_id);
CREATE INDEX idx_trips_dispatch_date     ON trips(dispatch_date);

CREATE INDEX idx_fuel_trip               ON fuel_purchases(trip_id);
CREATE INDEX idx_fuel_truck              ON fuel_purchases(truck_id);
CREATE INDEX idx_fuel_driver             ON fuel_purchases(driver_id);
CREATE INDEX idx_fuel_date               ON fuel_purchases(purchase_date);

CREATE INDEX idx_maint_truck             ON maintenance_records(truck_id);
CREATE INDEX idx_maint_date              ON maintenance_records(maintenance_date);

CREATE INDEX idx_events_load             ON delivery_events(load_id);
CREATE INDEX idx_events_trip             ON delivery_events(trip_id);
CREATE INDEX idx_events_facility         ON delivery_events(facility_id);
CREATE INDEX idx_events_actual_dt        ON delivery_events(actual_datetime);

CREATE INDEX idx_incidents_trip          ON safety_incidents(trip_id);
CREATE INDEX idx_incidents_truck         ON safety_incidents(truck_id);
CREATE INDEX idx_incidents_driver        ON safety_incidents(driver_id);
