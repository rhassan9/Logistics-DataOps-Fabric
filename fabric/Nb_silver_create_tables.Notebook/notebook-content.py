# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e781f9bd-98b2-4482-9e9d-a2731909476f",
# META       "default_lakehouse_name": "lh_logistics_silver",
# META       "default_lakehouse_workspace_id": "a77071e4-bd2a-4979-8910-91ddb8cd2a09",
# META       "known_lakehouses": [
# META         {
# META           "id": "e781f9bd-98b2-4482-9e9d-a2731909476f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Silver — Table Definitions
#
# Destination : lh_logistics_silver
# Scope       : all 12 silver tables
# Cadence     : run once per environment, before any transformation
#
# ---------------------------------------------------------------------------
# WHY DDL IS SEPARATE FROM TRANSFORMATION
# ---------------------------------------------------------------------------
# Structure and content have different lifecycles. Table definitions are
# provisioned once at deployment; transformations run on every batch. Keeping
# them apart means the schema contract lives in one reviewable file, and the
# transformation notebooks stay free of setup logic.
#
# It also makes deployment explicit: this notebook is a setup step in the
# release pipeline, not something that happens as a side effect of the first
# data load.
#
# ---------------------------------------------------------------------------
# WHY THE TABLES ARE CREATED EMPTY FIRST
# ---------------------------------------------------------------------------
# Change data feed records only the changes made after it is enabled. It does
# not backfill. A table created by the first write and altered afterwards
# leaves that initial load outside the feed permanently, and gold's
# incremental loads would silently miss it.
#
# Creating the table empty with the property already set guarantees every row
# that ever enters silver appears in the feed.
#
# Explicit schemas also mean type enforcement rather than type inference. A
# transformation whose output does not match fails loudly instead of quietly
# widening a column.
#
# ---------------------------------------------------------------------------
# NAMING
# ---------------------------------------------------------------------------
# Column names stay aligned to the source. Silver's job includes traceability,
# and renaming here would break the line from silver back through bronze to
# the source system. Business-friendly naming happens in gold, and display
# names, descriptions and synonyms in the semantic model.
#
# _change_type, _commit_version and _commit_timestamp are avoided throughout:
# change data feed reserves those names, and a collision prevents CDF from
# being enabled at all.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# ===========================================================================
# CELL 1 — PARAMETERS  
# ===========================================================================
 
drop_existing = False   # True rebuilds every table. Destroys all silver data.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 2 — CONFIGURATION
# ===========================================================================
 
SILVER_LAKEHOUSE = "lh_logistics_silver"
SILVER_SCHEMA = "dbo"
 
# Properties applied to every silver table.
#
# enableChangeDataFeed       gold reads silver incrementally through the feed
# deletedFileRetentionDuration  seven days is the floor; shorter windows break
#                            time travel and can remove CDF history before a
#                            downstream consumer has read it
TABLE_PROPERTIES = """
TBLPROPERTIES (
    delta.enableChangeDataFeed = 'true',
    delta.deletedFileRetentionDuration = 'interval 7 days'
)
"""
 
# Audit columns carried by every silver table.
#
# _source_ingest_date  which bronze batch this row came from
# _silver_created_at   first time the row entered silver; never updated
# _silver_updated_at   last time MERGE touched it
# _silver_run_id       which transformation run last wrote it
# _dq_status           'valid' or 'flagged'; gold filters on this without
#                      needing to know which rules apply to which table
AUDIT_COLUMNS = """
    _source_ingest_date       STRING,
    _silver_created_at        TIMESTAMP,
    _silver_updated_at        TIMESTAMP,
    _silver_run_id            STRING,
    _dq_status                STRING
"""
 
print(f"target: {SILVER_LAKEHOUSE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 3 — REFERENCE TABLES
# ===========================================================================
# Written by Dataflow Gen2. Six tables, 758 rows in total.
#
# Six columns across these tables hold a single value in the current extract:
# drivers.cdl_class, trucks.fuel_type, trailers.status, trailers.length_feet.
# They are retained. A CDL class of B or C, a truck running on CNG, or a
# 48-foot trailer are all real, so these columns are uniform by coincidence
# rather than by definition. Dropping them would break on the first row
# carrying a different value and would remove attributes that gold's Type 2
# dimensions may later need to track.
#
# Derived columns use a fixed as-of date rather than the current date. Facts
# end on 2024-12-31, so an age measured against today would grow every run,
# breaking idempotency and describing a period in which nothing happened.
 
REFERENCE_TABLES = {
 
    "customers": f"""
        customer_id               STRING  NOT NULL,
        customer_name             STRING,
        customer_type             STRING,
        credit_terms_days         INT,
        primary_freight_type      STRING,
        account_status            STRING,
        contract_start_date       DATE,
        annual_revenue_potential  DECIMAL(14,2),
        {AUDIT_COLUMNS}
    """,
 
    "facilities": f"""
        facility_id               STRING  NOT NULL,
        facility_name             STRING,
        facility_type             STRING,
        city                      STRING,
        state                     STRING,
        latitude                  DECIMAL(10,6),
        longitude                 DECIMAL(10,6),
        dock_doors                INT,
        operating_hours           STRING,
        -- Coordinates are city centroids, not street addresses: facilities in
        -- the same city share identical values. Proximity and inter-facility
        -- distance are therefore meaningless, and this flag says so in the
        -- data rather than only in documentation.
        is_centroid_coordinate    BOOLEAN,
        {AUDIT_COLUMNS}
    """,
 
    "drivers": f"""
        driver_id                 STRING  NOT NULL,
        first_name                STRING,
        last_name                 STRING,
        hire_date                 DATE,
        termination_date          DATE,
        license_number            STRING,
        license_state             STRING,
        date_of_birth             DATE,
        home_terminal             STRING,
        employment_status         STRING,
        cdl_class                 STRING,
        years_experience          INT,
        -- Measured to the as-of date for active drivers, to the termination
        -- date otherwise.
        tenure_days               INT,
        age_at_hire               INT,
        {AUDIT_COLUMNS}
    """,
 
    "trucks": f"""
        truck_id                  STRING  NOT NULL,
        unit_number               STRING,
        make                      STRING,
        model_year                INT,
        vin                       STRING,
        acquisition_date          DATE,
        acquisition_mileage       INT,
        fuel_type                 STRING,
        tank_capacity_gallons     INT,
        status                    STRING,
        home_terminal             STRING,
        -- Age at the as-of date. Age at the time of each trip is a gold
        -- calculation joining to the date dimension.
        truck_age_years_as_of     INT,
        {AUDIT_COLUMNS}
    """,
 
    "trailers": f"""
        trailer_id                STRING  NOT NULL,
        trailer_number            STRING,
        trailer_type              STRING,
        length_feet               INT,
        model_year                INT,
        vin                       STRING,
        acquisition_date          DATE,
        status                    STRING,
        -- A snapshot as at the extract date, not a history of movements.
        current_location          STRING,
        trailer_age_years_as_of   INT,
        {AUDIT_COLUMNS}
    """,
 
    "routes": f"""
        route_id                  STRING  NOT NULL,
        origin_city               STRING,
        origin_state              STRING,
        destination_city          STRING,
        destination_state         STRING,
        typical_distance_miles    INT,
        -- Rates, not amounts. These must never be summed.
        base_rate_per_mile        DECIMAL(8,4),
        fuel_surcharge_rate       DECIMAL(8,4),
        typical_transit_days      INT,
        route_name                STRING,
        {AUDIT_COLUMNS}
    """,
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 4 — TRANSACTIONAL TABLES
# ===========================================================================
# Written by nb_silver_transactional. Six tables, 541,172 rows in total.
#
# Foreign key columns stay nullable. Seven of them carry nulls at around two
# percent, an intentional characteristic of the source. Gold resolves those to
# Unknown dimension members; silver must not invent values.
#
# Data quality flags record rule failures rather than removing rows. All three
# failing rules trace to the same cause: columns generated independently of
# the constraint relating them. Removing the rows would distort revenue, fuel
# and duration totals for defects that belong to the generator rather than to
# the business.
 
TRANSACTIONAL_TABLES = {
 
    "loads": f"""
        load_id                   STRING  NOT NULL,
        customer_id               STRING,
        route_id                  STRING,
        load_date                 DATE,
        load_type                 STRING,
        weight_lbs                INT,
        pieces                    INT,
        revenue                   DECIMAL(14,2),
        fuel_surcharge            DECIMAL(14,2),
        accessorial_charges       DECIMAL(14,2),
        load_status               STRING,
        booking_type              STRING,
        {AUDIT_COLUMNS}
    """,
 
    "trips": f"""
        trip_id                   STRING  NOT NULL,
        load_id                   STRING,
        driver_id                 STRING,
        truck_id                  STRING,
        trailer_id                STRING,
        dispatch_date             DATE,
        actual_distance_miles     INT,
        actual_duration_hours     DECIMAL(10,2),
        fuel_gallons_used         DECIMAL(12,2),
        average_mpg               DECIMAL(8,2),
        idle_time_hours           DECIMAL(10,2),
        trip_status               STRING,
        -- Idle time exceeding total duration. Fails on 100 percent of trips
        -- under three hours and 0 percent above twelve, which is the
        -- signature of two independent distributions. Idle time as a share of
        -- duration is therefore unusable; absolute idle hours remain valid.
        is_idle_implausible       BOOLEAN,
        {AUDIT_COLUMNS}
    """,
 
    "delivery_events": f"""
        event_id                  STRING  NOT NULL,
        load_id                   STRING,
        trip_id                   STRING,
        event_type                STRING,
        facility_id               STRING,
        scheduled_datetime        TIMESTAMP,
        actual_datetime           TIMESTAMP,
        detention_minutes         INT,
        on_time_flag              BOOLEAN,
        location_city             STRING,
        location_state            STRING,
        -- Actual minus scheduled, signed. Negative is early. Measures carrier
        -- punctuality, and is independent of detention (correlation 0.0255),
        -- which measures facility performance after arrival.
        arrival_variance_minutes  DECIMAL(10,1),
        -- Splits the source flag's single false value into Early and Late.
        -- Both are appointment failures but different operational problems:
        -- early means schedules are over-padded, late means they are missed.
        arrival_status            STRING,
        -- Detention beyond the two-hour free period, which is the industry
        -- convention and the point at which the shipper starts paying.
        billable_detention_minutes INT,
        -- Delivery timestamp preceding pickup on the same load. 486 loads, by
        -- up to 90 minutes: timestamp jitter rather than a corrupt record.
        is_timestamp_reversed     BOOLEAN,
        {AUDIT_COLUMNS}
    """,
 
    "fuel_purchases": f"""
        fuel_purchase_id          STRING  NOT NULL,
        trip_id                   STRING,
        truck_id                  STRING,
        driver_id                 STRING,
        purchase_date             TIMESTAMP,
        location_city             STRING,
        -- location_state is absent by design. Twenty-five cities map to more
        -- than one state in this table, so city and state were assigned
        -- independently. Authoritative geography comes from facilities.
        gallons                   DECIMAL(10,2),
        price_per_gallon          DECIMAL(10,3),
        total_cost                DECIMAL(14,2),
        fuel_card_number          STRING,
        -- Fill volume exceeding the truck's tank. 18,105 rows, all on
        -- 150-gallon trucks, because volume was drawn from one distribution
        -- regardless of tank size. Neither column can be identified as the
        -- wrong one, so both are kept and the row is flagged.
        is_capacity_exceeded      BOOLEAN,
        {AUDIT_COLUMNS}
    """,
 
    "maintenance_records": f"""
        maintenance_id            STRING  NOT NULL,
        truck_id                  STRING,
        maintenance_date          DATE,
        maintenance_type          STRING,
        odometer_reading          INT,
        labor_hours               DECIMAL(8,2),
        labor_cost                DECIMAL(14,2),
        parts_cost                DECIMAL(14,2),
        -- Equals labor_cost + parts_cost on every row. Retained with a
        -- validation rule rather than recomputed, so a future source
        -- contradiction surfaces instead of being silently overwritten.
        total_cost                DECIMAL(14,2),
        facility_location         STRING,
        downtime_hours            DECIMAL(10,2),
        -- service_description held 21 values: three urgency levels across
        -- seven components. The component half duplicated maintenance_type
        -- exactly, so only the urgency is new. The composite is dropped,
        -- since storing a value beside its own parts is redundancy.
        service_urgency           STRING,
        {AUDIT_COLUMNS}
    """,
 
    "safety_incidents": f"""
        incident_id               STRING  NOT NULL,
        trip_id                   STRING,
        truck_id                  STRING,
        driver_id                 STRING,
        incident_date             TIMESTAMP,
        incident_type             STRING,
        location_city             STRING,
        -- location_state dropped, as in fuel_purchases.
        -- Fault is legal liability. Preventability is a stricter safety
        -- standard: a driver rear-ended at a light is not at fault, but the
        -- collision may still have been preventable.
        at_fault_flag             BOOLEAN,
        injury_flag               BOOLEAN,
        vehicle_damage_cost       DECIMAL(14,2),
        cargo_damage_cost         DECIMAL(14,2),
        claim_amount              DECIMAL(14,2),
        preventable_flag          BOOLEAN,
        -- description held 12 values: three severities across four causes.
        -- Both halves are new information.
        incident_severity         STRING,
        incident_cause            STRING,
        -- incident_type mixes safety, regulatory, asset and service concerns,
        -- so a count of "safety incidents" would include customer complaints.
        incident_category         STRING,
        {AUDIT_COLUMNS}
    """,
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 5 — CREATE
# ===========================================================================
 
ALL_TABLES = {**REFERENCE_TABLES, **TRANSACTIONAL_TABLES}
SILVER_PREFIX = "silver_"
 
created, existing = [], []
 
for name, columns in ALL_TABLES.items():
    target = f"{SILVER_LAKEHOUSE}.{SILVER_SCHEMA}.{SILVER_PREFIX}{name}"
 
    if drop_existing:
        spark.sql(f"DROP TABLE IF EXISTS {target}")
 
    if spark.catalog.tableExists(target):
        existing.append(name)
        continue
 
    spark.sql(f"""
        CREATE TABLE {target} (
        {columns}
        )
        USING DELTA
        {TABLE_PROPERTIES}
    """)
    created.append(name)
 
print(f"created  : {len(created)}")
for n in created:
    print(f"  {n}")
print(f"unchanged: {len(existing)}")
for n in existing:
    print(f"  {n}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 6 — VERIFY
# ===========================================================================
# Change data feed must be active on every table before any row is written.
# A table missing the property has to be dropped and recreated: enabling it
# later leaves everything already loaded outside the feed.
 
from pyspark.sql import functions as F
 
print(f"{'table':<26}{'CDF':>8}{'retention':>22}{'columns':>9}")
print("-" * 65)
 
problems = []
 
for name in ALL_TABLES:
    target = f"{SILVER_LAKEHOUSE}.{SILVER_SCHEMA}.{SILVER_PREFIX}{name}"
    props  = {
        r["key"]: r["value"]
        for r in spark.sql(f"SHOW TBLPROPERTIES {target}").collect()
    }
    cdf       = props.get("delta.enableChangeDataFeed", "MISSING")
    retention = props.get("delta.deletedFileRetentionDuration", "default")
    n_cols    = len(spark.table(target).columns)
 
    if cdf != "true":
        problems.append(name)
 
    print(f"{name:<26}{cdf:>8}{retention:>22}{n_cols:>9}")
 
print("-" * 65)
print("All tables ready." if not problems
      else f"Change data feed missing on: {', '.join(problems)}. "
           f"Drop and recreate those tables before loading.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 7 — NOTES FOR THE DATAFLOW GEN2 AUTHOR
# ===========================================================================
# The six reference tables are written by Dataflow Gen2 against the schemas
# defined above.
#
#   1. Output column names and types must match exactly. The schema is fixed
#      now, so a mismatch fails the write rather than silently widening a
#      column.
#
#   2. Set the destination to append or update an EXISTING table. A replace
#      drops and recreates it, which discards the table properties and
#      silently disables change data feed.
#
#   3. Derived columns use a fixed as-of date of 2024-12-31, the last fact
#      date, rather than the current date:
#
#        tenure_days             datediff(coalesce(termination_date,
#                                                  as_of_date), hire_date)
#        age_at_hire             years between date_of_birth and hire_date
#        truck_age_years_as_of   year(as_of_date) - model_year
#        trailer_age_years_as_of year(as_of_date) - model_year
#        route_name              origin_city + ' to ' + destination_city
#        is_centroid_coordinate  true for every row
#
#   4. Audit columns: set _dq_status to 'valid', _source_ingest_date from the
#      bronze row, and both timestamps to the run time.
#
# After the first Dataflow run, re-run CELL 6. If change data feed has gone
# missing, the destination was set to replace.
 
print("See cell source for Dataflow Gen2 requirements.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
