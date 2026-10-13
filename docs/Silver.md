# Silver layer

Silver holds one validated, non-aggregated row for every business key the source has ever contained, carrying that key's current state. It deduplicates, derives business columns, flags records that fail a data quality rule, and records whether each key still exists at the source. Aggregation and dimensional modelling belong in gold.

Writes are MERGE on the business key, so loads are idempotent and silver holds current state (SCD Type 1). Gold applies Type 2 where a dimension needs history.

**In short**

- One row per business key ever seen, current values, built by MERGE; transactional tables by a Spark notebook, reference tables by Dataflow Gen2.
- Rows are never deleted: `_is_deleted_in_source` and `_source_changed_at` form a contract with gold that survives a change of capture method. See [The contract with gold](#the-contract-with-gold).
- Data quality problems are flagged, not removed; two boundary defects in `arrival_status` were found by reconciling against the source flag. See [Validation](#validation).

---

## Artifacts

| Artifact | Scope | Cadence |
|---|---|---|
| `nb_silver_create_tables` | DDL for all 12 tables | once per environment |
| `nb_silver_transactional` | 6 transactional tables | every batch |
| `df_lh_logistics_bronze_to_silver`, one Dataflow Gen2 with six queries | 6 reference tables | every batch |

DDL is separate from transformation because structure and content have different lifecycles. The split also forces change data feed to be enabled at creation rather than after the first write, which matters because CDF captures nothing retrospectively: a table created by its first load would leave that load permanently outside the feed.

The transformation notebook raises an error rather than creating a missing table, so a fresh environment fails loudly instead of silently producing tables without CDF.

---

## The contract with gold

Every silver table carries two columns that define what gold may rely on, whatever produces silver upstream:

| Column | Meaning |
|---|---|
| `_is_deleted_in_source` | `true` when the key no longer exists at the source. The row is kept with its last known values; silver never physically deletes a row |
| `_source_changed_at` | when the current state, current values or the deletion, took effect at the source, as precisely as ingestion can observe it |

A key is live when `_is_deleted_in_source` is false. Gold carries the flag onto its facts and dimensions as a soft delete, and uses `_source_changed_at` to date new Type 2 dimension versions.

The contract is deliberately independent of how silver learns about changes. Today both columns are derived from bronze snapshots, so `_source_changed_at` is accurate to the bronze ingestion cadence. A change-data-capture source would supply source commit times and delete events instead, and only the derivation below would change; the columns, their meaning and every gold object would not.

### Deriving current state

Bronze is append-only, so a key can appear in many batches. `current_state` reduces bronze to one row per key:

- **The latest version is current.** Taking the earliest would pin silver to stale values after the first source update.
- **`_source_changed_at` dates the version, not the batch.** It is the first batch of the unbroken run of identical versions ending at the latest one, so a value that changes and changes back is dated by its return.
- **Deletion comes from complete snapshots.** Every bronze write stamps one `_ingested_at`, so each full-mode batch is a complete snapshot of the source table. A key whose last appearance precedes the latest snapshot no longer exists; its last version is kept, flagged, and dated by the first snapshot that no longer contained it.
- **A key that returns is live again, dated by its return.** A snapshot between two appearances breaks the run of identical versions, so a key that disappears and comes back unchanged is not dated by its first appearance.

Incremental runs read only recent batches. They can show what changed but not what disappeared, so deletion is detected on full runs. An incremental run revives a key silver holds as deleted only with a bronze row newer than the deletion; older rows in its window cannot undo it.

The reference queries in the Dataflow apply the same rules through a shared Power Query function, `fnCurrentState`.

---

## Design decisions

### Source-aligned naming

Column names are unchanged from bronze. Silver's responsibilities include traceability, and renaming here would break the line from `silver_trips.trip_id` back through `bronze_trips.trip_id` to `public.trips.trip_id`.

Business-friendly naming happens in gold, which is the layer analysts consume. Display names, descriptions and synonyms belong in the semantic model, which is where Microsoft's guidance on naming for AI and Copilot actually applies.

### Constant columns retained

Six source columns hold a single value each: `drivers.cdl_class`, `trucks.fuel_type`, `trailers.status`, `trailers.length_feet`, `loads.load_status`, `trips.trip_status`.

They are constant in this extract, not by definition. CDL classes B and C exist, trucks run on fuel types other than diesel, loads can be cancelled. Dropping them would break on the first row carrying a different value and would remove attributes that gold's Type 2 dimensions may later need to track. They are excluded from gold dimensions instead, where the decision is cheap to reverse.

### Fixed as-of date for derived ages

`truck_age_years_as_of`, `trailer_age_years_as_of` and `tenure_days` are calculated against **2024-12-31**, the last load/trip date used for these silver as-of calculations, not the current date.

A current-date calculation changes on every run, which breaks idempotency. It is also wrong: a truck's age relative to today is meaningless when every trip it made happened before 2025. Age at the time of each event is a gold calculation joining to the date dimension.

### Audit columns

| Column | Set by | Meaning |
|---|---|---|
| `_source_ingest_date` | transformation | which bronze batch the row came from |
| `_silver_created_at` | MERGE, insert only | first time the row entered silver |
| `_silver_updated_at` | MERGE, insert and changed update | last time silver's copy changed |
| `_silver_run_id` | transformation | which run last wrote it |
| `_dq_status` | transformation | `valid` or `flagged` |
| `_is_deleted_in_source` | transformation | the key no longer exists at the source |
| `_source_changed_at` | transformation | when the current state took effect at the source |

`_change_type`, `_commit_version` and `_commit_timestamp` are deliberately avoided. Change data feed reserves those names, and a collision prevents CDF from being enabled at all.

The MERGE updates a matched row only when a value differs, using null-safe comparison. An unconditional update would restamp every row and write a change-feed record for it on every full run, and gold rebuilds whatever the feed reports, so every run would rebuild every fact.

`_source_changed_at` is excluded from that comparison. It describes a version and must not create one: an incremental run sees fewer batches and can compute a later date for unchanged values. `_is_deleted_in_source` is compared, because a key becoming deleted, or returning, is a real change that gold must hear about.

### Table properties

| Property | Value | Reason |
|---|---|---|
| `delta.enableChangeDataFeed` | `true` on transactional tables, off on reference tables | gold reads the transactional feeds; see below |
| `delta.deletedFileRetentionDuration` | 7 days | shorter windows break time travel and remove change data before a consumer can read it |
| V-Order | off on transactional tables, on for reference tables | the transactional tables are MERGE-heavy, write cost matters more than read speed there, and each row is read roughly once per batch, so they keep the new-workspace Spark default of off. The reference tables are written by Dataflow Gen2, whose Lakehouse destination applies V-Order by default (`EnableVorder = true` on all six destinations); at 758 rows the write cost is immaterial. V-Order does improve Spark and SQL reads, which is why gold, a warehouse, keeps its default of on |
| Delta protocol | reader 1, writer 2, deletion vectors off, on reference tables only | see Reference tables |

**Gold consumes the change data feed of the six transactional tables.** Gold is a Fabric Warehouse, and T-SQL reads only the current snapshot through the SQL analytics endpoint. A Spark notebook in the gold layer therefore reads each table's feed from the version that gold last consumed and hands the affected business keys to the warehouse procedures. The reference tables are small enough for gold to compare in full on every load, so they need no feed.

---

## Transformations

### Data quality: flag, do not remove

Three rules fail on this source. All three trace to the same cause: columns generated independently of the constraint that relates them.

| Rule | Rows | Column |
|---|---:|---|
| Fuel purchase exceeds tank capacity | 18,105 | `is_capacity_exceeded` |
| Idle time exceeds trip duration | 7,450 | `is_idle_implausible` |
| Delivery timestamp precedes pickup | 972 events, 486 loads | `is_timestamp_reversed` |

None are quarantined. Removing 26,000 rows would distort revenue, fuel and duration totals for defects belonging to the data generator rather than to the business. The reversal maxes at 90 minutes, which is timestamp jitter rather than a corrupt record.

Gold excludes flagged rows from the affected measures only. `_dq_status` lets it do that without knowing which rules apply to which table.

Deleted rows never decide a flag for live ones: the reversal check reads only events that still exist at the source.

### Derived columns

**`delivery_events`** carries three measures that are easily confused:

| Measure | What it describes |
|---|---|
| `arrival_variance_minutes` | actual minus scheduled, signed. Carrier punctuality |
| `detention_minutes` (source) | waiting time at the facility after arrival. Facility performance |
| `billable_detention_minutes` | detention beyond the two-hour free period |

Arrival variance and detention are independent in this source, correlation 0.0255, so both are kept.

`arrival_status` splits the source flag's single false value into Early and Late. Both are appointment failures but different operational problems: consistently early means schedules are over-padded, late means they are not being met. The threshold matches the source flag's exclusive 120-minute window in both directions, applied to the variance at full timestamp precision. `arrival_variance_minutes` stores the same variance rounded to a tenth of a minute; classification never uses the rounded value.

**`maintenance_records`** and **`safety_incidents`** have their templated description columns flattened. `service_description` held 21 values (three urgency levels across seven components) and `description` held 12 (three severities across four causes). Neither is free text, so the planned NLP step was dropped in favour of a string split.

`service_description` is removed after splitting, because its component half duplicates `maintenance_type` exactly and storing a value beside its own parts is redundancy. Bronze retains the original verbatim.

`incident_category` groups `incident_type` into Safety, Regulatory, Asset and Service. The source column mixes four unrelated concerns, so an unqualified count of safety incidents would include customer complaints.

### Columns removed

`location_state` is dropped from `fuel_purchases` and `safety_incidents`. Twenty-five cities map to more than one state in each, so city and state were assigned independently. `location_city` is internally consistent and is kept. Authoritative geography comes from `silver_facilities`, where the pairs are correct.

### Nulls preserved

Seven foreign key columns carry nulls at around two percent. This is an intentional characteristic of the source, not an error, so silver preserves them. Gold resolves them to the Missing dimension member (key 0), which is distinct from Unknown (key -1), the member for a value that exists but fails its lookup. Fabric Warehouse does not enforce foreign keys, so an unresolved key would otherwise silently disappear from an inner join.

---

## Reference tables

The six reference tables are built with Dataflow Gen2 rather than a notebook, to exercise the low-code path alongside the code path.

Each query reads its bronze table through `fnCurrentState`, which reduces every bronze batch to one current row per key with the same deletion and dating rules as the notebook. Without it, the second bronze batch would have given every reference table duplicate keys; gold's dimension loads would have stopped on their duplicate guard rather than load wrong data, but the defect belonged in silver and is fixed there.

**Dataflow Gen2 has no merge or upsert update method.** Only append and replace exist. The tables therefore use **Replace with Fixed schema**, which replaces rows while keeping the table object and its properties. Dynamic schema would drop and recreate the table, discarding its properties. With Fixed schema, a query whose columns do not match the table fails to publish, so a new column is added to the table first and mapped second.

`_silver_created_at` is meaningless on these tables, because replace reinserts every row. The transactional tables keep true first-appearance semantics; the reference tables cannot.

**Delta protocol.** Dataflow Gen2 writes Delta at reader version 1 and writer version 2. Tables created by Spark on Fabric Runtime 2.0 start at reader 3 and writer 7 with deletion vectors enabled, and the Dataflow refresh fails against them. The DDL notebook therefore creates the reference tables with the protocol pinned to reader 1 and writer 2 and deletion vectors off, a level both writers support. The transactional tables, written only by Spark, keep the runtime default.

**Conclusion for tool selection:** Dataflow Gen2 suits small tables where only current state matters and a full rebuild is cheap. It is a poor fit for any layer needing upsert semantics or preserved history, including bronze, where replace would destroy the audit record that layer exists to hold. It also constrains the Delta features its target tables may use, which has to be planned for when Spark and Dataflow share a lakehouse.

---

## Validation

Silver keeps one row per business key ever seen. Deduplication is the only operation permitted to reduce the row count, so a shortfall means rows were lost. Live rows must match the source count.

| Table | Rows | Flagged |
|---|---:|---:|
| loads | 85,410 | 0 |
| trips | 85,410 | 7,450 |
| delivery_events | 170,820 | 972 |
| maintenance_records | 2,920 | 0 |
| safety_incidents | 170 | 0 |
| fuel_purchases | 196,442 | 18,105 |

Every notebook run checks the contract on its tables: one row per key, and no row without `_is_deleted_in_source` or `_source_changed_at`. The run writes its log to `lh_logistics_ops` and then fails if any table failed or broke the contract, so a failure is always recorded before it stops the pipeline. Gold's dimension loads enforce the same contract on the reference tables: a duplicate key or a missing contract column stops the load.

Derived columns reconcile independently against the source:

- `arrival_status` matches `on_time_flag` exactly: 95,095 On Time against `true`; 9,382 Early and 66,343 Late against `false`
- `billable_detention_minutes` sums to 2,987,151, the figure measured during profiling

Reconciliation found two defects in `arrival_status`, both at the 120-minute boundary.

The first was an inclusive boundary where the source flag is exclusive, misclassifying 20 rows sitting at exactly ±120 minutes.

The second surfaced when gold listed the delivery status values: 59 events carried `on_time_flag = true` but were classified Early or Late. Two causes combined. Classification used the rounded `arrival_variance_minutes`, so 119.97 minutes rounded to 120.0 and became Late; and `unix_timestamp` truncated both timestamps to whole seconds, so an arrival 7,199.6 seconds late read as 7,200. A millisecond check settled the source rule: every `true` row in the boundary band sits below 7,200,000 ms and every `false` row at or above it. The variance is now computed at full timestamp precision and classified directly, and all 170,820 events reconcile.

Both defects, like the inclusive watermark fix in bronze, were found by comparing against a known number rather than by reading the code. Since `arrival_status` carries the same information as `on_time_flag`, gold keeps only `arrival_status`; the source flag remains in silver.

The deletion rules are tested end to end: a row flagged deleted in silver reaches gold as a soft delete, and a full silver run from unchanged bronze returns it to live with a single updated row.

---

## Known limitations

**Deletions are detected on full runs only.** An incremental batch holds only rows past the watermark, so a key missing from it proves nothing.

**`_source_changed_at` is accurate to the bronze cadence.** It is the ingestion time of the first batch that carried the state, an upper bound on the true source change time.

**The reversal flag is computed within one run's rows.** It compares a load's pickup and delivery events, and an incremental run that receives only one of them cannot see the pair. Both events arrive together in this source. Gold computes the flag on `fact_load_lifecycle` from its own data and does not depend on this column.

**Schema evolution is manual.** `align_to_target` fails on any mismatch between the transformation output and the table definition. A new source column stops the pipeline rather than being absorbed. This is deliberate: silent schema widening is harder to detect than a failed run.

**Late-arriving data is handled implicitly.** The inclusive watermark in bronze combined with MERGE on the business key means a late row is picked up and merged rather than duplicated. There is no explicit late-arrival logic.

**SCD Type 2 is not implemented in silver.** The source contains no dimension changes across its three years, so history has nothing to capture. Type 2 is applied in gold, where the mechanism can be demonstrated against generated data.

---

## Maintenance

`OPTIMIZE` and `VACUUM` are Spark SQL commands and are not supported in the SQL analytics endpoint or the warehouse editor. The transactional notebook runs them only when its `run_maintenance` parameter is true, so routine batch runs skip them and a scheduled maintenance run enables them. The equivalent standalone commands:

```python
LARGE_TABLES = ["loads", "trips", "delivery_events", "fuel_purchases"]

for t in LARGE_TABLES:
    spark.sql(f"OPTIMIZE lh_logistics_silver.dbo.silver_{t}")
    spark.sql(f"VACUUM   lh_logistics_silver.dbo.silver_{t} RETAIN 168 HOURS")
```

`VACUUM` removes files outside the retention window, including change data. Run it only after gold's change detection has consumed every version it would remove; otherwise gold's next run falls back to a full reconciliation.