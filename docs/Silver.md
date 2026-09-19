# Silver layer

Silver holds one validated, non-aggregated row per business entity. It deduplicates,
derives business columns, and flags records that fail a data quality rule. Aggregation and
dimensional modelling belong in gold.

Writes are MERGE on the business key, so loads are idempotent and silver holds current
state (SCD Type 1). Gold applies Type 2 where a dimension needs history.

---

## Artifacts

| Artifact | Scope | Cadence |
|---|---|---|
| `nb_silver_create_tables` | DDL for all 12 tables | once per environment |
| `nb_silver_transactional` | 6 transactional tables | every batch |
| 6 Dataflow Gen2 flows | 6 reference tables | every batch |

DDL is separate from transformation because structure and content have different
lifecycles. The split also forces change data feed to be enabled at creation rather than
after the first write, which matters because CDF captures nothing retrospectively: a table
created by its first load would leave that load permanently outside the feed.

The transformation notebook raises an error rather than creating a missing table, so a
fresh environment fails loudly instead of silently producing tables without CDF.

---

## Design decisions

### Source-aligned naming

Column names are unchanged from bronze. Silver's responsibilities include traceability, and
renaming here would break the line from `silver_trips.trip_id` back through
`bronze_trips.trip_id` to `public.trips.trip_id`.

Business-friendly naming happens in gold, which is the layer analysts consume. Display
names, descriptions and synonyms belong in the semantic model, which is where Microsoft's
guidance on naming for AI and Copilot actually applies.

### Constant columns retained

Six source columns hold a single value each: `drivers.cdl_class`, `trucks.fuel_type`,
`trailers.status`, `trailers.length_feet`, `loads.load_status`, `trips.trip_status`.

They are constant in this extract, not by definition. CDL classes B and C exist, trucks run
on fuel types other than diesel, loads can be cancelled. Dropping them would break on the
first row carrying a different value and would remove attributes that gold's Type 2
dimensions may later need to track. They are excluded from gold dimensions instead, where
the decision is cheap to reverse.

### Fixed as-of date for derived ages

`truck_age_years_as_of`, `trailer_age_years_as_of` and `tenure_days` are calculated against
**2024-12-31**, the last fact date, not the current date.

A current-date calculation changes on every run, which breaks idempotency. It is also
wrong: a truck's age relative to today is meaningless when every trip it made happened
before 2025. Age at the time of each event is a gold calculation joining to the date
dimension.

### Audit columns

| Column | Set by | Meaning |
|---|---|---|
| `_source_ingest_date` | transformation | which bronze batch the row came from |
| `_silver_created_at` | MERGE, insert only | first time the row entered silver |
| `_silver_updated_at` | MERGE, insert and update | last time the row was touched |
| `_silver_run_id` | transformation | which run last wrote it |
| `_dq_status` | transformation | `valid` or `flagged` |

`_change_type`, `_commit_version` and `_commit_timestamp` are deliberately avoided. Change
data feed reserves those names, and a collision prevents CDF from being enabled at all.

### Table properties

| Property | Value | Reason |
|---|---|---|
| `delta.enableChangeDataFeed` | `true` | gold reads silver incrementally through the feed |
| `delta.deletedFileRetentionDuration` | 7 days | shorter windows break time travel and can remove CDF history before gold consumes it |
| V-Order | off | silver is read by Spark and by gold's T-SQL, not by Direct Lake. V-Order optimises for the VertiPaq engine at a cost of 15 to 33 percent on writes, so it is applied in gold |

---

## Transformations

### Data quality: flag, do not remove

Three rules fail on this source. All three trace to the same cause: columns generated
independently of the constraint that relates them.

| Rule | Rows | Column |
|---|---:|---|
| Fuel purchase exceeds tank capacity | 18,105 | `is_capacity_exceeded` |
| Idle time exceeds trip duration | 7,450 | `is_idle_implausible` |
| Delivery timestamp precedes pickup | 972 events, 486 loads | `is_timestamp_reversed` |

None are quarantined. Removing 26,000 rows would distort revenue, fuel and duration totals
for defects belonging to the data generator rather than to the business. The reversal maxes
at 90 minutes, which is timestamp jitter rather than a corrupt record.

Gold excludes flagged rows from the affected measures only. `_dq_status` lets it do that
without knowing which rules apply to which table.

### Derived columns

**`delivery_events`** carries three measures that are easily confused:

| Measure | What it describes |
|---|---|
| `arrival_variance_minutes` | actual minus scheduled, signed. Carrier punctuality |
| `detention_minutes` (source) | waiting time at the facility after arrival. Facility performance |
| `billable_detention_minutes` | detention beyond the two-hour free period |

Arrival variance and detention are independent in this source, correlation 0.0255, so both
are kept.

`arrival_status` splits the source flag's single false value into Early and Late. Both are
appointment failures but different operational problems: consistently early means schedules
are over-padded, late means they are not being met. The threshold matches the source flag's
exclusive 120-minute window in both directions.

**`maintenance_records`** and **`safety_incidents`** have their templated description
columns flattened. `service_description` held 21 values (three urgency levels across seven
components) and `description` held 12 (three severities across four causes). Neither is free
text, so the planned NLP step was dropped in favour of a string split.

`service_description` is removed after splitting, because its component half duplicates
`maintenance_type` exactly and storing a value beside its own parts is redundancy. Bronze
retains the original verbatim.

`incident_category` groups `incident_type` into Safety, Regulatory, Asset and Service. The
source column mixes four unrelated concerns, so an unqualified count of safety incidents
would include customer complaints.

### Columns removed

`location_state` is dropped from `fuel_purchases` and `safety_incidents`. Twenty-five cities
map to more than one state in each, so city and state were assigned independently.
`location_city` is internally consistent and is kept. Authoritative geography comes from
`silver_facilities`, where the pairs are correct.

### Nulls preserved

Seven foreign key columns carry nulls at around two percent. This is an intentional
characteristic of the source, not an error, so silver preserves them. Gold resolves them to
Unknown dimension members, since Fabric Warehouse does not enforce foreign keys and an
unmatched key would silently disappear from an inner join.

---

## Reference tables

The six reference tables are built with Dataflow Gen2 rather than a notebook, to exercise
the low-code path alongside the code path.

**Dataflow Gen2 has no merge or upsert update method.** Only append and replace exist. The
tables therefore use **Replace with Fixed schema**, which replaces rows while keeping the
table object and its properties. Dynamic schema would drop and recreate the table,
destroying the change data feed setting.

Two costs follow from that:

- `_silver_created_at` is meaningless on these tables, because replace reinserts every row.
  The transactional tables keep true first-appearance semantics; the reference tables cannot.
- The change feed emits a delete and an insert for all 758 rows on every refresh, whether or
  not anything changed. Harmless at this size, but it means CDF is not useful for reference
  data and gold full-reloads these dimensions instead.

**Conclusion for tool selection:** Dataflow Gen2 suits small tables where only current state
matters and a full rebuild is cheap. It is a poor fit for any layer needing upsert semantics
or preserved history, including bronze, where replace would destroy the audit record that
layer exists to hold.

---

## Validation

Silver preserves its source's row count. Deduplication is the only operation permitted to
reduce it, so a shortfall means rows were lost.

| Table | Rows | Flagged |
|---|---:|---:|
| loads | 85,410 | 0 |
| trips | 85,410 | 7,450 |
| delivery_events | 170,820 | 972 |
| maintenance_records | 2,920 | 0 |
| safety_incidents | 170 | 0 |
| fuel_purchases | 196,442 | 18,105 |

Derived columns reconcile independently against the source:

- `arrival_status` matches `on_time_flag` exactly: 95,095 On Time, 75,725 Early plus Late
- `billable_detention_minutes` sums to 2,987,151, the figure measured during profiling

Reconciliation found one bug. `arrival_status` used an inclusive boundary while the source
flag uses an exclusive one, misclassifying the 20 rows sitting at exactly ±120 minutes. That
is the same class of error as the inclusive watermark fix in bronze, and both were found by
comparing against a known number rather than by reading the code.

---

## Known limitations

**Schema evolution is manual.** `align_to_target` fails on any mismatch between the
transformation output and the table definition. A new source column stops the pipeline
rather than being absorbed. This is deliberate: silent schema widening is harder to detect
than a failed run.

**Late-arriving data is handled implicitly.** The inclusive watermark in bronze combined
with MERGE on the business key means a late row is picked up and merged rather than
duplicated. There is no explicit late-arrival logic.

**SCD Type 2 is not implemented in silver.** The source contains no dimension changes across
its three years, so history has nothing to capture. Type 2 is applied in gold, where the
mechanism can be demonstrated against generated data.

---

## Maintenance

`OPTIMIZE` and `VACUUM` are Spark SQL commands and are not supported in the SQL analytics
endpoint or the warehouse editor. Run them from a notebook.

```python
LARGE_TABLES = ["loads", "trips", "delivery_events", "fuel_purchases"]

for t in LARGE_TABLES:
    spark.sql(f"OPTIMIZE lh_logistics_silver.dbo.silver_{t}")
    spark.sql(f"VACUUM   lh_logistics_silver.dbo.silver_{t} RETAIN 168 HOURS")
```

Once gold consumes the change feed, run `VACUUM` only after gold has caught up. Files outside
the retention window are removed, and the change data they hold goes with them.