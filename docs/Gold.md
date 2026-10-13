# Gold layer

Kimball star schema in `wh_logistics_gold`, a Fabric Data Warehouse built with T-SQL stored procedures and served through a Direct Lake semantic model.

**In short**

- Six facts, eight dimensions and four junk dimensions, with three special members on every dimension: Missing, Unknown and Not Applicable.
- Facts load incrementally: a Spark notebook reads silver's change feed, and T-SQL procedures rebuild exactly the changed keys, inside one transaction per fact, with soft deletes and a mass-deletion guard. See [Loading the facts](#loading-the-facts).
- Every fact was tested through the real pipeline, and five defects were found and fixed. See [Testing](#testing).
- Ten metric comparisons reproduce the publisher's monthly baselines exactly and fail the run on any mismatch. See [Reconciliation](#reconciliation).

Gold reads silver two ways. The stored procedures read current state through the silver SQL analytics endpoint, with cross-database queries. A Spark notebook, `nb_gold_change_detection`, reads silver's Delta change data feed to decide which business keys each fact must rebuild, because the change feed is a Delta capability that T-SQL cannot read. The notebook hands the keys to the warehouse; the procedures do all the modelling.

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

The three have distinct causes and must not be conflated. A load awaiting dispatch has no truck because none has been assigned, which is Not Applicable. A trip whose `driver_id` is null in the source has a driver that was never recorded, which is Missing. A trip carrying a `driver_id` that resolves to no dimension row is Unknown, and indicates a real integrity problem rather than an expected gap.

Microsoft's convention also includes -3 for Error. No ETL path in this model produces it, so it is not created.

---

## Dimensions

### Schemas and naming

| Schema | Holds |
|---|---|
| `dim` | dimensions, their source-mapping views and load procedures |
| `fact` | facts and their load procedures |
| `stg` | one worklist table per fact: the business keys a run must rebuild |
| `log` | the run log, the change-feed ledger, checkpoint views and reconciliation results |
| `ref` | `ref.us_state`, the single source of full state names, which never enters the semantic model |

Tables keep the prefixed names used throughout this design (`dim.dim_driver`, `fact.fact_trip`). Microsoft's Fabric dimensional modelling guidance prefixes dimension and fact tables, and the names stay identical to the bus matrix; the semantic model gives tables business-facing names.

Columns shared in meaning across dimensions carry their owning table's prefix (`driver_home_terminal`, `truck_home_terminal`, `truck_model_year`), so no field name repeats across the model.

### Keys and constraints

Every dimension uses a `BIGINT IDENTITY` surrogate key except `dim_date`, whose key is an `INT` in `YYYYMMDD` form. Microsoft names the date key as the one accepted exception to meaningless surrogate keys, and Kimball accepts readable date keys on the understanding that special rows force non-date values (0, -1, -2) that consumers must test for.

No table carries primary, unique or foreign key constraints. Fabric supports them only as `NOT ENFORCED`, added by `ALTER TABLE`, with documented Git integration limitations, and Microsoft notes that unenforced key columns are not necessarily good join candidates. Each load procedure asserts uniqueness itself and fails on a violation.

### Date dimension

`dim_date` is generated rather than sourced, covering whole calendar years from 2022-01-01 to 2025-12-31; the procedure rejects any other range shape. Labels and sort columns (`quarter_label`, `month_name`, `year_month_label` with their numeric sort keys) are materialised, because Direct Lake does not support calculated columns on SQL and supports them only in preview on OneLake.

`is_complete_month` is 1 when the whole month falls inside the loaded data window, supplied as a parameter (2024-12-31). Delivery events extend to 2025-01-03, so January 2025 is incomplete; filtering on the flag removes the false collapse from monthly trends and drops 2025 from year-level charts. Keys are deterministic, so regenerating the table never changes a key a fact depends on.

A fact's own date key must exist in `dim_date`. Facts dated beyond 2025-12-31 require the date range to be extended first; otherwise they resolve to the Unknown member.

### Slowly changing dimensions

All six reference dimensions carry `valid_from_date_key`, `valid_to_date_key`, `is_current`, `is_deleted` and `version_reason`, following Microsoft's SCD type 2 column pattern. Validity is half-open, `[valid_from, valid_to)`, so a fact resolves to the version where its date key is at least `valid_from_date_key` and below `valid_to_date_key`. A member's first version starts at the earliest date in `dim_date`.

`valid_to_date_key = 99991231` marks the open-ended current version. It is a comparison boundary, not a member of the reporting date dimension, and the validity columns are not related to `dim_date` in the semantic model.

Customer, trailer, route and facility use Type 1: changed attributes are overwritten and the validity columns stay at their initial values, because the source contains no master data change in its three years. Driver and truck use Type 2 for the attributes whose history changes how activity is attributed, and Type 1 for corrections:

| Dimension | Type 2: new version | Type 1: overwrite every version |
|---|---|---|
| `dim_driver` | `driver_home_terminal`, `employment_status`, `driver_termination_date` | `driver_name`, `driver_hire_date`, `years_experience` |
| `dim_truck` | `truck_status`, `truck_home_terminal` | `unit_number`, `truck_make`, `truck_model_year`, `truck_acquisition_date`, `acquisition_mileage`, `tank_capacity_gallons` |

`years_experience` is Type 1 despite growing over time: versioning it would add a row per driver per year.

**A Type 2 version starts on the date the change took effect at the source**, taken from silver's `_source_changed_at`, not on the date the ETL happened to run. A late ETL run therefore still attributes every fact to the version that was true on the fact's own date. A second change on the same date overwrites the current version rather than creating a zero-length one, and a change dated before the current version's start is rejected rather than rewriting history. Both SCD2 dimensions carry a version label combining the member with its versioned attributes.

### Deletions

Dimension rows are never physically deleted, since facts reference their keys. When silver reports a member deleted at the source, `is_deleted` is set on every version of that member, so reports can exclude retired members without breaking any historical fact. A member that returns at the source is restored the same way.

A **mass-deletion guard** stops each load when more than 10 percent of active members would be marked deleted in one run. That is the signature of a broken upstream extract rather than a business event, and the procedure refuses until it is rerun with `@allow_mass_delete = 1`.

### Source mapping

Each dimension reads silver through one view (`dim.vw_src_customer` and so on) that holds the column mapping: renames, the state name lookup against `ref.us_state`, null text replaced by `Not Recorded` (distinct from the Unknown member), flags rendered as text, and display casing. The view stores no data; silver remains the staging layer. Change detection compares the view with the dimension using a null-safe `EXCEPT` comparison, so an unchanged member is never rewritten. At 758 rows the full comparison runs on every load, so the reference dimensions need no change feed.

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

Every fact also carries `is_deleted_in_source`. A row whose source record is deleted stays in the fact with its keys and measures intact and the flag set, so history is never lost and reports filter on the flag.

### fact_load_lifecycle

> **One row per load, revisited as each milestone completes.**

An accumulating snapshot. The row is rebuilt from silver whenever the load, any of its trips or any of its events changes, so it always shows how far the load has progressed. It is the only fact spanning booking and execution, which is what makes revenue per asset a single-fact query.

| | |
|---|---|
| Dimension keys | `booked_date_key`, `dispatched_date_key`, `pickup_date_key`, `delivery_date_key` (role-playing), `customer_key`, `route_key`, `load_type_key`, `fulfilling_driver_key`, `fulfilling_truck_key`, `fulfilling_trailer_key` |
| Degenerate | `load_id` |
| Additive measures | `revenue`, `fuel_surcharge`, `accessorial_charges`, `weight_lbs`, `pieces` |
| Non-additive | `booking_to_dispatch_days`, `dispatch_to_pickup_hours`, `pickup_to_delivery_hours`, `booking_to_delivery_hours` |
| Flags | `is_timestamp_reversed`, `is_deleted_in_source` |

**Fulfilling resources.** Load revenue attributes in full to the resource that delivered the load, with no allocation across execution attempts. Revenue is earned once, at the load. Costs already sit at trip grain, so a failed attempt carries its miles and fuel with zero revenue, which is what a re-tendered load does to the P&L. Any allocation rule would be a modelling artifact presented as a business fact.

`fulfilling_driver_key`, `fulfilling_truck_key` and `fulfilling_trailer_key` hold the resource that **completed** the load, not simply the one assigned at dispatch. The fulfilling trip is chosen in this order, counting only trips and events that still exist at the source:

1. the trip carrying the load's completed Delivery event, because it delivered the load;
2. otherwise the latest trip by dispatch date, then trip id, which is the current attempt;
3. otherwise none, and every execution milestone is Not Applicable.

Resources resolve to the dimension version valid on the fulfilling trip's dispatch date, the same version `fact_trip` uses for that trip, so the two facts always agree. Before dispatch the keys hold the Not Applicable member, which makes "revenue booked but not yet assigned to an asset" a directly queryable number.

**Milestones.**

| Key | Source | Not Applicable (-2) | Missing (0) | Unknown (-1) |
|---|---|---|---|---|
| `booked_date_key` | `load_date` | never | null | outside `dim_date` |
| `dispatched_date_key` | fulfilling trip's `dispatch_date` | no fulfilling trip | null | outside `dim_date` |
| `pickup_date_key` | fulfilling trip's Pickup event, actual time | no event, or no actual time yet | never | outside `dim_date` |
| `delivery_date_key` | fulfilling trip's Delivery event, actual time | no event, or no actual time yet | never | outside `dim_date` |

An event without an actual time has been scheduled but has not happened, which is a milestone not yet reached. If a trip or event is deleted at the source, the milestones it supplied return to Not Applicable; the load row itself is deleted only when the load is.

Customer and route resolve at the booked date, the date the row starts from. A trip or event with more than one live Pickup or Delivery, or a load with more than one completed delivery, stops the load with an error rather than being resolved by a guess, since either would make a milestone ambiguous.

**Lags.** `load_date` and `dispatch_date` are dates, while pickup and delivery are timestamps, so the columns carry their units in their names and a day figure cannot be read as an hour figure. Hour lags count from midnight of a date-only end, are measured in microseconds and rounded to two decimals; measuring in whole seconds is the truncation that produced the second silver `arrival_status` defect. Every load books and dispatches on the same date, so `booking_to_dispatch_days` is 0 throughout this extract; it is constant here, not by definition, and is kept for the same reason silver keeps constant columns. Dispatch to pickup runs from 4.0 to 22.0 hours.

**Reversed timestamps.** 486 loads record a delivery before their pickup, by up to 90 minutes. `pickup_to_delivery_hours` is null on those rows rather than negative, and `is_timestamp_reversed` carries through so the exclusion is visible rather than silent. Gold computes the flag from the fulfilling trip's own events rather than reading silver's per-event flag, so the fact never depends on which events a silver batch happened to contain.

### fact_trip

> **One row per trip, which is one execution attempt of a load.**

| | |
|---|---|
| Dimension keys | `dispatch_date_key`, `driver_key`, `truck_key`, `trailer_key`, `route_key` |
| Degenerate | `trip_id`, `load_id` |
| Additive measures | `actual_distance_miles`, `actual_duration_hours`, `fuel_gallons_used`, `idle_time_hours` |
| Flags | `is_idle_implausible`, `is_deleted_in_source` |

`route_key` is resolved through the load during ETL rather than navigated at query time. Route is a primary axis for execution analysis, and the denormalisation is deliberate. A trip with no load is Missing, a load that fails its lookup is Unknown, and a load with no route is Missing; each is a different fault. A load deleted at the source still supplies its route, since the route was true when the trip ran.

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
| Flags | `is_deleted_in_source` |

`route_key` is resolved through the event's own load during ETL; every event's load matches its trip's load. DP-1 asks which city pairs deliver most reliably, and the origin and destination pair lives on `dim_route`, so the fact connects to the dimension directly rather than reaching it through two other facts. Facility and route resolve at the actual event date, or the scheduled date when no actual time is recorded.

Arrival variance and detention measure different things and are independent in this source (correlation 0.0255). Variance is carrier punctuality against the appointment; detention is facility performance after arrival, billable beyond the two-hour free period. Both are kept.

The two timestamps stay on the fact for detail. No time-of-day dimension is built: no requirement needs one, and the source assigns event times without pattern.

`arrival_variance_minutes` is signed, so negative means early. It must not be summed, since early and late arrivals would cancel. The semantic model exposes average variance and the Early, On Time and Late counts from `dim_delivery_status`.

The fact carries no city column. The source provides `location_city` on delivery events, but 164,935 of 170,820 values disagree with the facility's own city, so it is not suitable as event-level geography. `facility_key` is the authoritative location and the only geography carried. Silver retains `location_city` for traceability.

The city-to-state mapping in `silver_facilities` is sound: every facility city resolves to exactly one state. That is a separate question from whether an event was assigned to the right city, which is why `dim_location` is still built from authoritative geography and used by the facts whose source provides city without a reliable facility relationship.

### fact_fuel_purchase

> **One row per fuel purchase.**

| | |
|---|---|
| Dimension keys | `purchase_date_key`, `truck_key`, `driver_key`, `route_key`, `location_key` |
| Degenerate | `fuel_purchase_id`, `trip_id` |
| Additive measures | `gallons`, `total_cost` |
| Flags | `is_capacity_exceeded`, `is_deleted_in_source` |

`route_key` is resolved through trip then load in ETL. It is carried rather than chained so the fact connects directly to the dimension, and it makes route contribution after fuel a drill-across against `fact_load_lifecycle` on `dim_route` rather than a three-table walk. Truck and driver resolve to the version valid on the purchase date. 3,880 purchases record no truck and 3,988 no driver; both resolve to Missing.

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
| Flags | `is_deleted_in_source` |

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
| Flags | `is_deleted_in_source` |

`claim_amount` is not carried. It equals `vehicle_damage_cost + cargo_damage_cost` on every row, so the semantic model computes it. The identity is asserted against silver, which holds the source value.

At 170 incidents across 150 drivers over three years, any breakdown by experience band, category and severity produces single-digit cells. Every measure is reported with an explicit sample size caveat.

---

## Loading the facts

Every fact loads in two halves sharing one `run_id`: the change detection notebook decides which business keys changed, and the fact's procedure rebuilds exactly those keys.

### Change detection

`nb_gold_change_detection` runs once per pipeline run, after silver and before the fact procedures.

**It refuses a reused `run_id`.** A run_id names one detection and the gold load that consumes it. Before reading anything, the notebook checks that its run_id appears in neither the run log nor the change-feed ledger.

**It checks that the warehouse sees current silver.** The procedures read silver through the SQL analytics endpoint, whose metadata Microsoft documents as syncing in the background, while the notebook reads Delta directly. The notebook forces a metadata sync through Semantic Link (`refresh_sql_endpoint_metadata`), then compares a fingerprint of all 12 silver tables as Spark and the endpoint each see them: row count, deleted count and the latest `_source_changed_at` in microseconds. Any difference stops the run before a key is staged.

**It reads each source table's change feed from that fact's own checkpoint.** Each fact declares the silver tables it reads, and each (fact, table) pair has a checkpoint: the last Delta version a successful run of that fact consumed. The notebook reads the window after the checkpoint up to the current version and derives the affected keys:

| Fact | Keys come from changes to |
|---|---|
| `fact_trip` | the trip, or its load |
| `fact_delivery_event` | the event, or its load |
| `fact_fuel_purchase` | the purchase, its trip, or the trip's load |
| `fact_maintenance` | the record |
| `fact_safety_incident` | the incident |
| `fact_load_lifecycle` | the load, any of its trips, or any of its events |

The feed is read including the pre-update image of each row. For a fact keyed on a parent, such as the lifecycle keyed on the load, the post-update image of a trip that moved to another load names only the load it joined; the pre-update image names the load it left, which would otherwise keep stale milestones.

**Rows needing repair are added even when silver has not changed.** Three kinds of gold row are re-queued on every run:

- a key resolved to Unknown, because the missing dimension row may have arrived since;
- a Type 2 key that is no longer the version valid on the fact's date, because a version created after the fact was loaded can start on or before that date;
- a key with no dimension row at all, left behind when a dimension is rebuilt.

**A full reconciliation replaces the window when it cannot be trusted.** With no checkpoint, when the change feed for the window is no longer readable, or when a step is listed in `full_scan_steps`, the worklist becomes every silver key plus every key already in gold, so keys that left silver still reach the procedure.

The keys go to the fact's worklist in `stg`, and each source table's version window goes to the ledger `log.cdf_batch`.

### Fact procedures

Each procedure follows the same sequence:

1. **Dependency gate.** The ledger must hold a window for every silver table the fact reads, for this run_id; otherwise the procedure refuses to run.
2. **Stage.** Read current silver state for the worklist keys, including rows flagged deleted, and resolve every key at the fact's own date: the version valid on that date for Type 2 dimensions, Missing for a null reference, Unknown for a failed lookup.
3. **Assert.** Exactly one staged row per business key, or the load stops.
4. **Guard.** If more than 10 percent of the fact's active rows would be soft-deleted, stop unless `@allow_mass_delete = 1`.
5. **Publish in one transaction.** Replace the rows for the staged keys, soft-delete keys that silver no longer holds, and delete this run's worklist rows. The work and the evidence that it was done commit together or not at all.
6. **Log success after commit.** Only then does `log.etl_run` record `succeeded`.

### Checkpoints

`log.vw_cdf_checkpoint` gives each (fact, source table) pair the window of its **most recent** successful run, counting a window only when a `succeeded` event of the same fact and run_id followed its detection. Detection alone never moves a checkpoint, so a failed or abandoned run is re-read in full by the next one, and a window recorded under a run_id that had already succeeded is never counted as processed. Taking the most recent window rather than the highest version keeps the checkpoint correct after a silver table is recreated and its versions restart.

### Operations

| Object | Purpose |
|---|---|
| `log.etl_run` | append-only run log: started, succeeded or failed per step, with row counts |
| `log.cdf_batch` | the version window each run read, per fact and source table |
| `log.vw_unfinished_runs` | detections no successful load has followed |
| `stg.usp_purge_orphan_keys` | removes worklist rows at least 24 hours old that no success of their step followed |

Every procedure raises its own numbered errors, so a failure in the run log identifies its source directly:

| Range | Object |
|---|---|
| 500xx | `ref.us_state` |
| 501xx | `dim_date` |
| 502xx, 504xx | special members |
| 503xx | reference dimensions, one decade each (customer 5030x to truck 5035x) |
| 505xx | run log writer |
| 506xx | facts, one decade each (trip 5061x to lifecycle 5066x) |
| 507xx | worklist purge |
| 508xx | baseline reconciliation |

The change feed must stay readable until gold has consumed it, so silver's `VACUUM` runs only after every fact's checkpoint has passed the versions it would remove. When it has not, the next run falls back to full reconciliation rather than failing.

---

## Testing

Every fact was tested through the real pipeline with a fixed sequence of runs, each with its own run_id:

| Run | Change | Proves |
|---|---|---|
| First load | none, no checkpoint | full reconciliation loads every row |
| No change | none | an idle run stages nothing |
| Value change | a genuine change to a source value, including on a parent table | the right keys, and only those, are rebuilt |
| Deletion | a row flagged deleted in silver | the row stays in gold with the flag set |
| Restore | a full silver run from unchanged bronze | the same row returns to live |
| Forced full | `full_scan_steps` set | a full rebuild changes nothing |

Each run is checked in both directions against silver: row and key counts, every column compared with `EXCEPT` both ways, a key oracle that translates gold's surrogate keys back to business ids, worklist leftovers and checkpoint positions. For `fact_load_lifecycle` an independent oracle rebuilds every row from silver, and a cross-fact check confirms that every live trip's resources and dispatch date agree with the lifecycle row of its load.

Review and testing found five defects, each fixed:

- **Reused run_ids skipped changes.** The checkpoint view first matched a window to a success by run_id alone, so a detection repeated under a run_id that had already succeeded advanced the checkpoint without any procedure processing its keys. The cross-fact check exposed it as one lifecycle row disagreeing with `fact_trip`. The view now requires the success to follow the detection, and the notebook rejects reused run_ids.
- **A trip or event moving between loads left the old load stale.** The change feed was read from post-update images only. The pre-update image is now included.
- **A recreated silver table could skip changes.** The checkpoint was the highest version any successful run had consumed. A recreated table restarts its versions at 0, so the old, higher checkpoint stayed the maximum: every run repeated a full scan until the new table passed the old number, and the next window then started there, skipping the versions in between. The checkpoint is now the most recent successful window.
- **Route Missing and Unknown were conflated.** A trip whose load had no route resolved to Unknown, which signals a broken reference, instead of Missing.
- **Rebuilt dimensions orphaned fact keys.** Rebuilding a dimension reassigns its surrogate keys, and facts loaded earlier pointed at keys that no longer existed. Repair now re-queues any fact row whose key has no dimension row.

---

## Reconciliation

The two pre-computed monthly aggregates shipped with the source, `driver_monthly_metrics` and `truck_utilization_metrics`, are not gold facts. They are loaded into `lh_logistics_ops` as an expected baseline, an answer key produced by the dataset's publisher independently of this pipeline. Internal tests compare gold with silver through this project's own understanding of the data; the baseline tests that understanding itself.

`log.usp_reconcile_baselines` rebuilds both aggregates from the facts, compares them with the baselines metric by metric, and appends one row per metric to `log.reconciliation_result`. The row set is every resource-month with a live trip, by dispatch month; both baselines match it exactly, with no month on only one side.

**Verified metrics** fail the run on any mismatch, after every result has been recorded. Each reproduced its baseline exactly on every row, all 4,464 driver-months or all 3,312 truck-months, with its definition written before any result was seen. Trips, miles and revenue are compared on both baselines, fuel gallons on drivers, and the three maintenance metrics on trucks: ten comparisons in all.

| Metric | Definition |
|---|---|
| `trips_completed` | count of live trips |
| `total_miles` | sum of `actual_distance_miles` |
| `total_revenue` | lifecycle revenue by fulfilling driver or truck, in the dispatch month |
| `total_fuel_gallons` | sum of trip `fuel_gallons_used` |
| `maintenance_events` | count of maintenance records, by maintenance month |
| `maintenance_cost` | sum of `labor_cost + parts_cost` |
| `downtime_hours` | sum of `downtime_hours` |

The revenue match confirms the fulfilling-resource rule from outside the project: revenue lands on the same driver and truck, in the same month, as the publisher's own calculation, on every row.

**Unverified metrics** are reported on every run and never fail it. Each keeps the one definition stated in advance; none is varied until it matches, which would be fitting the answer rather than reproducing it.

| Metric | Definition | Status |
|---|---|---|
| `average_idle_hours` | mean trip idle hours | matches 4,408 of 4,464 driver-months at the one decimal the baseline stores; every other month differs by exactly one unit in that decimal |
| `on_time_delivery_rate` | On Time share of Delivery events on the driver's trips | matches 4,383 of 4,464 at the three decimals the baseline stores; every other month differs by exactly one unit in the last decimal |
| `average_mpg` | total miles over total gallons | definition-unknown: the ratio does not reproduce it, consistent with the source's trip-level `average_mpg` being generated independently of miles and gallons |
| `utilization_rate` | none derivable from the data | definition-unknown, not compared |

The two near-misses keep their status under the exact-match rule; a one-unit difference in the last stored decimal is consistent with two tools rounding the same value differently, and changing the rounding until they agree would be fitting the answer.

The baseline's stored precision was measured rather than assumed. The first comparison used the precision declared when the baselines were loaded, and every idle-hours and on-time mismatch was at most half of the baseline's last decimal place, the signature of comparing at the wrong precision rather than of a different definition.

The procedure's failure path is tested by adding one mile to one trip: both `total_miles` metrics fail on exactly one row with a difference of 1.0000, `trips_completed` still passes, and the run stops with error 50802 naming the failing metrics.

Building persisted aggregates with no performance justification would be manufacturing a production object to satisfy a pattern. At 541,172 fact rows the warehouse does not need them.