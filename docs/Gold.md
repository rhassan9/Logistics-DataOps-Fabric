# Gold layer

Kimball star schema in `wh_logistics_gold`, a Fabric Data Warehouse built with T-SQL stored procedures and served through a Direct Lake semantic model. Gold reads silver through the silver SQL analytics endpoint.

**Collation: `Latin1_General_100_BIN2_UTF8`**, the Fabric default, case-sensitive, set at warehouse creation and not changeable afterwards.

Gold's ETL joins silver to gold on string business keys, and the silver SQL analytics endpoint exposes every string column as `varchar` with `Latin1_General_100_BIN2_UTF8`, as Microsoft documents for Delta string types and as `sys.columns` confirms on this lakehouse. Matching that collation keeps every cross-item comparison free of collation conversion. Microsoft documents that cross-warehouse queries between items of different collations can return errors or unexpected results, and that deployment pipelines and Git branching between warehouses of different collations are unsupported.

A case-insensitive collation would only have made ad hoc queries tolerant of case. Business keys follow a uniform uppercase convention (`TRK00045`, `CUST00183`, `DRV00097`) and no requirement depends on case-insensitive matching, so case carries no business meaning here.

**V-Order** is on, which is the warehouse default. Direct Lake reads these tables, and disabling V-Order on a warehouse is irreversible.

---

## Bus matrix

Six business processes, six facts, eight dimensions of which six are currently shared across more than one fact, plus four junk dimensions.

| Business process | Fact | Grain | date | customer | driver | truck | trailer | route | facility | location |
|---|---|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| Load lifecycle | `fact_load_lifecycle` | one load | 4 | x | F | F | F | x | | |
| Trip execution | `fact_trip` | one trip | 1 | | x | x | x | x | | |
| Delivery event | `fact_delivery_event` | one event | 2 | | | | | x | x | |
| Fuel purchase | `fact_fuel_purchase` | one purchase | 1 | | x | x | | x | | x |
| Maintenance | `fact_maintenance` | one service record | 1 | | | x | | | | x |
| Safety incident | `fact_safety_incident` | one incident | 1 | | x | x | | | | x |

Numbers count role-playing date keys. **F** marks fulfilling resource keys, defined below.

Date appears on all six facts, truck on five, route on four, driver on four, trailer and location on two or more. Customer and facility are single-fact today. The shared dimensions are what drill-across depends on, which is what the matrix exists to show.

### Junk dimensions

| Dimension | Attaches to | Attributes |
|---|---|---|
| `dim_load_type` | load lifecycle | booking_type, load_type |
| `dim_delivery_status` | delivery event | event_type, arrival_status |
| `dim_maintenance_class` | maintenance | service_urgency, maintenance_type |
| `dim_incident_class` | safety incident | incident_type, incident_category, severity, cause, preventable status, at fault status, injury status |

Each is built from the combinations that actually occur in silver plus the special members, not the full Cartesian product, because some attributes are dependent and a product would offer impossible combinations in slicers.

---

## Special dimension members

Three members on every dimension:

| Key | Member | Meaning | Produced by |
|---|---|---|---|
| 0 | Missing | the source recorded no value | null foreign keys on trips, fuel purchases and incidents |
| -1 | Unknown | a value exists but the lookup failed | the referential integrity check, since the warehouse does not enforce foreign keys |
| -2 | Not Applicable | the value cannot exist yet | lifecycle milestones not reached, resources not yet assigned |

The three have distinct causes and must not be conflated. A load awaiting dispatch has no truck because none has been assigned, which is Not Applicable. A trip whose `driver_id` is null in the source has a driver that was never recorded, which is Missing. A trip carrying a
`driver_id` that resolves to no dimension row is Unknown, and indicates a real integrity problem rather than an expected gap.

Microsoft's convention also includes -3 for Error. No ETL path in this model produces it, so it is not created.

---

## Dimensions

### Schemas and naming

Dimensions live in schema `dim` and facts in schema `fact`, keeping the prefixed table names used throughout this design (`dim.dim_driver`, `fact.fact_trip`). Microsoft's Fabric dimensional modelling guidance prefixes dimension and fact tables, and the names stay identical to the bus matrix; the semantic model gives tables business-facing names. Schema `ref` holds `ref.us_state`, the single source of full state names, which never enters the semantic model. Schema `log` holds the ETL run log.

Columns shared in meaning across dimensions carry their owning table's prefix (`driver_home_terminal`, `truck_home_terminal`, `truck_model_year`), so no field name repeats across the model.

### Keys and constraints

Every dimension uses a `BIGINT IDENTITY` surrogate key except `dim_date`, whose key is an `INT` in `YYYYMMDD` form. Microsoft names the date key as the one accepted exception to meaningless surrogate keys, and Kimball accepts readable date keys on the understanding that special rows force non-date values (0, -1, -2) that consumers must test for.

No table carries primary, unique or foreign key constraints. Fabric supports them only as `NOT ENFORCED`, added by `ALTER TABLE`, with documented Git integration limitations, and Microsoft notes that unenforced key columns are not necessarily good join candidates. Each load procedure asserts uniqueness itself and fails on a violation.

### Date dimension

`dim_date` is generated rather than sourced, covering whole calendar years from 2022-01-01 to 2025-12-31; the procedure rejects any other range shape. Labels and sort columns (`quarter_label`, `month_name`, `year_month_label` with their numeric sort keys) are materialised, because Direct Lake does not support calculated columns on SQL and supports them only in preview on OneLake.

`is_complete_month` is 1 when the whole month falls inside the loaded data window, supplied as a parameter (2024-12-31). Delivery events extend to 2025-01-03, so January 2025 is incomplete; filtering on the flag removes the false collapse from monthly trends and drops 2025 from year-level charts. Keys are deterministic, so regenerating the table never changes a key a fact depends on.

A fact's own date key must exist in `dim_date`. Facts dated beyond 2025-12-31 require the date range to be extended first; otherwise they resolve to the Unknown member.

### Slowly changing dimensions

All six reference dimensions carry `valid_from_date_key`, `valid_to_date_key`, `is_current` and `version_reason`, following Microsoft's SCD type 2 column pattern. Validity is half-open, `[valid_from, valid_to)`, so a fact resolves to the version where its date key is at least `valid_from_date_key` and below `valid_to_date_key`. A member's first version starts at the earliest date in `dim_date`.

`valid_to_date_key = 99991231` marks the open-ended current version. It is a comparison boundary, not a member of the reporting date dimension, and the validity columns are not related to `dim_date` in the semantic model.

Customer, trailer, route and facility use Type 1: changed attributes are overwritten and the validity columns stay at their initial values, because the source contains no master data change in its three years. Driver and truck use Type 2 for the attributes whose history changes how activity is attributed, and Type 1 for corrections:

| Dimension | Type 2: new version | Type 1: overwrite every version |
|---|---|---|
| `dim_driver` | `driver_home_terminal`, `employment_status`, `driver_termination_date` | `driver_name`, `driver_hire_date`, `years_experience` |
| `dim_truck` | `truck_status`, `truck_home_terminal` | `unit_number`, `truck_make`, `truck_model_year`, `truck_acquisition_date`, `acquisition_mileage`, `tank_capacity_gallons` |

`years_experience` is Type 1 despite growing over time: versioning it would add a row per driver per year. A Type 2 version starts at the ETL processing date passed to the procedure, which is a comparison boundary and may fall after the reporting window. A second change on the same date overwrites the current version rather than creating a zero-length one, and a processing date earlier than the current version's start is rejected. Both SCD2 dimensions carry a version label combining the member with its versioned attributes.

Dimension rows are never deleted, since facts reference their keys.

### Source mapping

Each dimension reads silver through one view (`dim.vw_src_customer` and so on) that holds the column mapping: renames, the state name lookup against `ref.us_state`, null text replaced by `Not Recorded` (distinct from the Unknown member), flags rendered as text, and display casing. The view stores no data; silver remains the staging layer. Change detection compares the view with the dimension using a null-safe `EXCEPT` comparison, so an unchanged member is never rewritten.

### Junk dimensions and location

Junk dimensions follow Kimball Design Tip #113: built from combinations that occur in silver, checked for new combinations on every load before the facts, and never versioned, since a changed value is simply another combination. The loads are insert-only.

| Dimension | Combinations |
|---|---:|
| `dim_load_type` | 6 of 6 possible |
| `dim_delivery_status` | 6 of 6 |
| `dim_maintenance_class` | 21 of 21 |
| `dim_incident_class` | 134 of 480 |

`incident_type` determines `incident_category` exactly, leaving 480 possible incident combinations. 170 incidents fill 134 of them, close to the 143 expected if the remaining attributes were generated independently, so the incident junk dimension provides little compression. At 137 rows it remains a single dimension.

`dim_location` holds one row per event city used by fuel purchases, safety incidents and maintenance. A city receives a state only when the trusted geography in facilities, routes and delivery events agrees on exactly one; `state_source` records which table supplied it. All 25 event cities resolve. A city that does not resolve receives no row, and its facts resolve to the Unknown member.

---

## Measures

Gold stores atomic measures and derives everything expressible from them. Ratios are non-additive and are computed from their underlying values rather than stored, and a total that equals the sum of columns already present is not persisted.

| Stored | Derived in the semantic model |
|---|---|
| `actual_distance_miles`, `fuel_gallons_used` | miles per gallon |
| `gallons`, `total_cost` | average price paid per gallon |
| `labor_cost`, `parts_cost` | maintenance cost |
| `vehicle_damage_cost`, `cargo_damage_cost` | total damage cost |
| `revenue`, `actual_distance_miles` | revenue per mile |
| event counts by `arrival_status` | on-time, early and late rates |

The source columns those derivations replace (`average_mpg`, `price_per_gallon`, `total_cost` on maintenance, `claim_amount`, `on_time_flag`) remain in silver, which preserves source truth. Gold holds the atomic values because a stored ratio cannot be re-aggregated correctly and a stored total can contradict its own components.

---

## Grain statements

Each fact declares its grain in a sentence before anything else. Everything below the grain statement has to be true at that grain.

### fact_load_lifecycle

> **One row per load, revisited as each milestone completes.**

An accumulating snapshot. The row is created when the load is booked and updated in place as the load is dispatched, picked up and delivered. It is the only fact spanning booking and execution, which is what makes revenue per asset a single-fact query.

| | |
|---|---|
| Dimension keys | `booked_date_key`, `dispatched_date_key`, `pickup_date_key`, `delivery_date_key` (role-playing), `customer_key`, `route_key`, `load_type_key`, `fulfilling_driver_key`, `fulfilling_truck_key`, `fulfilling_trailer_key` |
| Degenerate | `load_id` |
| Additive measures | `revenue`, `fuel_surcharge`, `accessorial_charges`, `weight_lbs`, `pieces` |
| Non-additive | `booking_to_dispatch_days`, `dispatch_to_pickup_hours`, `pickup_to_delivery_hours`, `booking_to_delivery_hours` |
| Flags | `is_timestamp_reversed` |

**Lag units are mixed by necessity.** `load_date` and `dispatch_date` are dates, while pickup and delivery are timestamps. Naming the columns with their units prevents a day figure being read as an hour figure.

**Reversed timestamps.** 486 loads record a delivery before their pickup, by up to 90 minutes. `pickup_to_delivery_hours` is null on those rows rather than negative, and the flag carries through so the exclusion is visible rather than silent.

**Fulfilling resources.** Load revenue attributes in full to the resource that delivered the load, with no allocation across execution attempts. Revenue is earned once, at the load. Costs already sit at trip grain, so a failed attempt carries its miles and fuel with zero revenue, which is what a re-tendered load does to the P&L. Any allocation rule would be a modelling artifact presented as a business fact.

**Definition.** `fulfilling_driver_key`, `fulfilling_truck_key` and `fulfilling_trailer_key` hold the resource that **completed** the load, not simply the one assigned at dispatch. While the load is in flight they hold the resource of the current attempt, and on delivery that attempt is by definition the fulfilling one. A re-tendered load therefore ends with the resource that delivered it, and the history of earlier attempts lives in `fact_trip`.

Before dispatch the keys hold the Not Applicable member, which makes "revenue booked but not yet assigned to an asset" a directly queryable number.

### fact_trip

> **One row per trip, which is one execution attempt of a load.**

| | |
|---|---|
| Dimension keys | `dispatch_date_key`, `driver_key`, `truck_key`, `trailer_key`, `route_key` |
| Degenerate | `trip_id`, `load_id` |
| Additive measures | `actual_distance_miles`, `actual_duration_hours`, `fuel_gallons_used`, `idle_time_hours` |
| Flags | `is_idle_implausible` |

`route_key` is resolved through the load during ETL rather than navigated at query time. Route is a primary axis for execution analysis, and the denormalisation is deliberate.

`is_idle_implausible` marks 7,450 trips whose idle time exceeds their total duration. Idle time as a share of duration is therefore unusable; absolute idle hours remain valid.

### fact_delivery_event

> **One row per pickup or delivery event.**

| | |
|---|---|
| Dimension keys | `scheduled_date_key`, `actual_date_key` (role-playing), `facility_key`, `route_key`, `delivery_status_key` |
| Degenerate | `event_id`, `load_id`, `trip_id` |
| Additive measures | `detention_minutes`, `billable_detention_minutes` |
| Non-additive | `arrival_variance_minutes` |
| Detail | `scheduled_datetime`, `actual_datetime` |

`route_key` is resolved through trip then load during ETL. DP-1 asks which city pairs deliver most reliably, and the origin and destination pair lives on `dim_route`, so the fact connects to the dimension directly rather than reaching it through two other facts.

Arrival variance and detention measure different things and are independent in this source (correlation 0.0255). Variance is carrier punctuality against the appointment; detention is facility performance after arrival, billable beyond the two-hour free period. Both are kept.

The two timestamps stay on the fact for detail. No time-of-day dimension is built: no requirement needs one, and the source assigns event times without pattern.

`arrival_variance_minutes` is signed, so negative means early. It must not be summed, since early and late arrivals would cancel. The semantic model exposes average variance and the Early, On Time and Late counts from `dim_delivery_status`.

The fact carries no city column. The source provides `location_city` on delivery events, but 164,935 of 170,820 values disagree with the facility's own city, so it is not suitable as event-level geography. `facility_key` is the authoritative location and the only geography
carried. Silver retains `location_city` for traceability.

The city-to-state mapping in `silver_facilities` is sound: every facility city resolves to exactly one state. That is a separate question from whether an event was assigned to the right city, which is why `dim_location` is still built from authoritative geography and used by the facts whose source provides city without a reliable facility relationship.

### fact_fuel_purchase

> **One row per fuel purchase.**

| | |
|---|---|
| Dimension keys | `purchase_date_key`, `truck_key`, `driver_key`, `route_key`, `location_key` |
| Degenerate | `fuel_purchase_id`, `trip_id` |
| Additive measures | `gallons`, `total_cost` |
| Flags | `is_capacity_exceeded` |

`route_key` is resolved through trip then load in ETL. It is carried rather than chained so the fact connects directly to the dimension, and it makes route contribution after fuel a drill-across against `fact_load_lifecycle` on `dim_route` rather than a three-table walk.

The fact carries no load reference. Route is the analytical axis FE-1 needs, and load-grain revenue already lives on the lifecycle fact.

`is_capacity_exceeded` marks 18,105 purchases larger than the truck's tank. They are excluded from tank utilisation measures and retained everywhere else, since removing 9 percent of fuel spend would distort every cost measure.

### fact_maintenance

> **One row per maintenance record.**

| | |
|---|---|
| Dimension keys | `maintenance_date_key`, `truck_key`, `location_key`, `maintenance_class_key` |
| Degenerate | `maintenance_id` |
| Additive measures | `labor_hours`, `labor_cost`, `parts_cost`, `downtime_hours` |
| Non-additive | `odometer_reading` |

`odometer_reading` is the truck's reading at service time. Summing it has no meaning, but the difference between consecutive readings for a truck gives the mileage between services.

`location_key` comes from `facility_location`, which is free text in the source rather than a facility identifier, so it resolves to `dim_location` at city grain rather than to `dim_facility`.

No measure may assume `service_urgency` differentiates anything. Four tested relationships returned flat: emergency share against service interval, cost and downtime against urgency, and cost against both odometer and model year. Urgency remains a valid slicer.

### fact_safety_incident

> **One row per safety incident.**

| | |
|---|---|
| Dimension keys | `incident_date_key`, `driver_key`, `truck_key`, `location_key`, `incident_class_key` |
| Degenerate | `incident_id`, `trip_id` |
| Additive measures | `vehicle_damage_cost`, `cargo_damage_cost` |

`claim_amount` is not carried. It equals `vehicle_damage_cost + cargo_damage_cost` on every row, so the semantic model computes it. The identity is asserted against silver, which holds
the source value.

At 170 incidents across 150 drivers over three years, any breakdown by experience band, category and severity produces single-digit cells. Every measure is reported with an explicit sample size caveat.

---

## Reconciliation test

The two pre-computed monthly aggregates shipped with the source are not gold facts. They are loaded into `lh_logistics_ops` as an expected baseline, and the comparison is implemented as a stored procedure that computes the monthly aggregation from the facts, compares it to the baseline, and logs pass or fail with counts.

Building persisted aggregates with no performance justification would be manufacturing a production object to satisfy a pattern. At 541,172 fact rows the warehouse does not need them.

Trip counts and total miles are verified as exactly reproducible across 4,464 driver-months and 3,312 truck-months. The remaining metrics have no documented formula and are reconciled after gold is built, with any that cannot be reproduced recorded as definition-unknown rather than reverse-engineered to force a match.