# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "344f3ccc-7a2a-43a7-b62d-572ac6168344",
# META       "default_lakehouse_name": "lh_logistics_bronze",
# META       "default_lakehouse_workspace_id": "a77071e4-bd2a-4979-8910-91ddb8cd2a09",
# META       "known_lakehouses": [
# META         {
# META           "id": "344f3ccc-7a2a-43a7-b62d-572ac6168344"
# META         },
# META         {
# META           "id": "f34bc028-a89a-46c4-a3ef-6b69a368f9c6"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Bronze Profiling
#
# Profiles all 12 bronze tables before any silver cleaning logic is written.
# The output of this notebook is the evidence base for the silver design:
# every transformation rule should trace back to something measured here.
#
# Source      : lh_logistics_bronze
# Produces    : a profile report, and one Delta table of findings for the repo
#
# WHY THIS RUNS FIRST
# Cleaning rules written from a schema are guesses. Cleaning rules written from
# a profile are decisions. This notebook answers, per table and per column:
# how many rows, how many distinct keys, where are the nulls, what are the
# ranges, and which referential links do not resolve.
#
# Data Wrangler complements this. This notebook gives breadth across 12 tables;
# Data Wrangler gives depth on one. Use this to find where to look, then launch
# Data Wrangler (notebook ribbon, Home tab) on the specific table that needs it.
#
# SETUP
#   Attach lh_logistics_bronze as the default lakehouse.
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BRONZE_PREFIX = "bronze_"
df  = spark.table(f"{BRONZE_PREFIX}loads")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 1 - SCOPE
# ===========================================================================
 
from pyspark.sql import functions as F
from datetime import datetime
 
BRONZE_PREFIX = "bronze_"
 
# business_key drives duplicate detection.
# date_columns drive range and gap checks.
TABLES = {
    "customers":           {"business_key": "customer_id",
                            "date_columns": ["contract_start_date"]},
    "facilities":          {"business_key": "facility_id",
                            "date_columns": []},
    "drivers":             {"business_key": "driver_id",
                            "date_columns": ["hire_date", "termination_date",
                                             "date_of_birth"]},
    "trucks":              {"business_key": "truck_id",
                            "date_columns": ["acquisition_date"]},
    "trailers":            {"business_key": "trailer_id",
                            "date_columns": ["acquisition_date"]},
    "routes":              {"business_key": "route_id",
                            "date_columns": []},
    "loads":               {"business_key": "load_id",
                            "date_columns": ["load_date"]},
    "trips":               {"business_key": "trip_id",
                            "date_columns": ["dispatch_date"]},
    "maintenance_records": {"business_key": "maintenance_id",
                            "date_columns": ["maintenance_date"]},
    "safety_incidents":    {"business_key": "incident_id",
                            "date_columns": ["incident_date"]},
    "delivery_events":     {"business_key": "event_id",
                            "date_columns": ["scheduled_datetime",
                                             "actual_datetime"]},
    "fuel_purchases":      {"business_key": "fuel_purchase_id",
                            "date_columns": ["purchase_date"]},
}
 
# Ingestion metadata added by bronze. Excluded from column profiling so the
# report describes source data rather than pipeline bookkeeping.
LINEAGE_COLUMNS = {"_ingest_date", "_ingested_at", "_source_system", "_load_mode"}
 
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
print(f"Profiling {len(TABLES)} tables  ·  run {RUN_ID}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 2 - TABLE LEVEL
# ===========================================================================
# Row counts, business key uniqueness, and how many ingestion batches each
# table has accumulated.
#
# duplicate_keys is the number that decides silver's dedup strategy. Bronze
# re-reads rows at the watermark boundary by design, so duplicates here are
# expected rather than a defect.
 
table_profile = []
 
for name, cfg in TABLES.items():
    df  = spark.table(f"{BRONZE_PREFIX}{name}")
    key = cfg["business_key"]
 
    rows          = df.count()
    distinct_keys = df.select(key).distinct().count()
    null_keys     = df.filter(F.col(key).isNull()).count()
    batches       = df.select("_ingest_date").distinct().count()
 
    table_profile.append({
        "table":          name,
        "rows":           rows,
        "distinct_keys":  distinct_keys,
        "duplicate_keys": rows - distinct_keys,
        "null_keys":      null_keys,
        "columns":        len(df.columns) - len(LINEAGE_COLUMNS),
        "batches":        batches,
    })
 
profile_df = spark.createDataFrame(table_profile)
display(profile_df.orderBy(F.desc("rows")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

for name, cfg in TABLES.items():
    df  = spark.table(f"{BRONZE_PREFIX}{name}")
    key = cfg["business_key"]

    business_cols = [
        c for c in df.columns
        if c != key and c not in LINEAGE_COLUMNS
    ]

    rows     = df.count()
    distinct = df.select(business_cols).distinct().count()

    if rows != distinct:
        print(f"{name}: {rows - distinct:,} rows duplicate another row's content")
    else:
        print(f"No duplicate data in {name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 3 - COLUMN LEVEL
# ===========================================================================
# Null rate and cardinality per column.
#
# What to look for:
#   null_pct 100      column is empty; drop it in silver
#   null_pct 0        candidate for a NOT NULL constraint
#   distinct 1        constant; carries no information
#   distinct == rows  a key or a free-text field
#   low distinct      categorical; check its values in cell 4
 
column_profile = []
 
for name, cfg in TABLES.items():
    df   = spark.table(f"{BRONZE_PREFIX}{name}")
    rows = df.count()
    cols = [c for c in df.columns if c not in LINEAGE_COLUMNS]
 
    # One pass per table rather than one per column: 12 jobs, not 130.
    aggs = []
    for c in cols:
        aggs.append(F.sum(F.col(c).isNull().cast("int")).alias(f"{c}__nulls"))
        aggs.append(F.countDistinct(F.col(c)).alias(f"{c}__distinct"))
 
    result = df.agg(*aggs).collect()[0].asDict()
 
    for c in cols:
        nulls    = result[f"{c}__nulls"]
        distinct = result[f"{c}__distinct"]
        column_profile.append({
            "table":     name,
            "column":    c,
            "data_type": dict(df.dtypes)[c],
            "nulls":     nulls,
            "null_pct":  round(100.0 * nulls / rows, 2) if rows else 0.0,
            "distinct":  distinct,
        })
 
column_df = spark.createDataFrame(column_profile)
 
print("Columns with nulls:")
display(column_df.filter(F.col("nulls") > 0).orderBy(F.desc("null_pct")))
 
print("Low-cardinality columns (categorical candidates):")
display(
    column_df
    .filter((F.col("distinct") <= 20) & (F.col("distinct") > 0))
    .orderBy("table", "column")
)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 4 - CATEGORICAL VALUES
# ===========================================================================
# Actual values in the low-cardinality columns, with frequencies.
#
# This is what exposes inconsistent categories: "Active" alongside "ACTIVE",
# stray whitespace, or a status value the schema never documented. Every
# standardisation rule in silver should point at a row of this output.
 
# CELL 4 - derive categoricals from the cell 3 profile
EXCLUDE = {"first_name", "last_name", "customer_name", "facility_name",
           "vin", "license_number", "fuel_card_number", "unit_number",
           "trailer_number", "description", "service_description"}

categoricals = [
    (r["table"], r["column"])
    for r in column_df.filter(
        (F.col("distinct") <= 25) &
        (F.col("distinct") > 1) &
        (F.col("data_type").isin("string", "boolean"))
    ).collect()
    if r["column"] not in EXCLUDE
]

for table, col in categoricals:
    print(f"\n{table}.{col}")
    display(
        spark.table(f"{BRONZE_PREFIX}{table}")
        .groupBy(col).count().orderBy(F.desc("count")).limit(25)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Cross-tabulation. If each customer_type maps to exactly one booking_type,
# MAGIC -- the two fields carry the same information and one is redundant.
# MAGIC SELECT
# MAGIC     c.customer_type,
# MAGIC     l.booking_type,
# MAGIC     count(*)                                              AS loads,
# MAGIC     round(100.0 * count(*) / sum(count(*)) OVER
# MAGIC           (PARTITION BY c.customer_type), 1)              AS pct_of_type
# MAGIC FROM bronze_loads l
# MAGIC JOIN bronze_customers c ON l.customer_id = c.customer_id
# MAGIC GROUP BY c.customer_type, l.booking_type
# MAGIC ORDER BY c.customer_type, l.booking_type

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Per-customer view. A customer using more than one booking_type means the
# MAGIC -- fields are genuinely independent; all customers at exactly 1 means the
# MAGIC -- generator derived one from the other.
# MAGIC SELECT
# MAGIC     booking_types_used,
# MAGIC     count(*) AS customers
# MAGIC FROM (
# MAGIC     SELECT c.customer_id,
# MAGIC            count(DISTINCT l.booking_type) AS booking_types_used
# MAGIC     FROM bronze_loads l
# MAGIC     JOIN bronze_customers c ON l.customer_id = c.customer_id
# MAGIC     GROUP BY c.customer_id
# MAGIC )
# MAGIC GROUP BY booking_types_used
# MAGIC ORDER BY booking_types_used

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 5 - NUMERIC RANGES
# ===========================================================================
# Min, max, mean, stddev, negatives and zeros for every numeric column.
#
# Columns are derived from the schema rather than listed by hand, so the
# profile stays correct if the source changes. Only two judgement filters are
# applied: lineage columns are pipeline metadata, and identifier columns are
# numeric by type but categorical by meaning, so neither is a measure.
#
# What to look for: negatives where only positives are possible, zeros that
# should be nulls, and maxima far enough from the mean to be data errors
# rather than genuine business events. Cell 5B quantifies the last of those.

NUMERIC_TYPES = ("int", "bigint", "smallint", "double", "float", "decimal")
ID_SUFFIXES   = ("_id", "_number")


def numeric_columns(table_name):
    """Numeric measure columns for one bronze table."""
    df = spark.table(f"{BRONZE_PREFIX}{table_name}")
    return [
        c for c, t in df.dtypes
        if t.startswith(NUMERIC_TYPES)
        and c not in LINEAGE_COLUMNS
        and not c.endswith(ID_SUFFIXES)
    ]


numeric_profile = []

for name in TABLES:
    df   = spark.table(f"{BRONZE_PREFIX}{name}")
    cols = numeric_columns(name)
    if not cols:
        continue

    # One aggregation per table rather than one per column.
    aggs = []
    for c in cols:
        aggs += [
            F.min(c).alias(f"{c}__min"),
            F.max(c).alias(f"{c}__max"),
            F.avg(c).alias(f"{c}__avg"),
            F.stddev(c).alias(f"{c}__std"),
            F.sum((F.col(c) < 0).cast("int")).alias(f"{c}__neg"),
            F.sum((F.col(c) == 0).cast("int")).alias(f"{c}__zero"),
            F.sum(F.col(c).isNull().cast("int")).alias(f"{c}__null"),
        ]

    r = df.agg(*aggs).collect()[0].asDict()

    for c in cols:
        lo, hi = r[f"{c}__min"], r[f"{c}__max"]
        mean, std = r[f"{c}__avg"], r[f"{c}__std"]
        numeric_profile.append({
            "table":     name,
            "column":    c,
            "min":       float(lo)   if lo   is not None else None,
            "max":       float(hi)   if hi   is not None else None,
            "mean":      round(float(mean), 2) if mean is not None else None,
            "stddev":    round(float(std),  2) if std  is not None else None,
            "negatives": r[f"{c}__neg"],
            "zeros":     r[f"{c}__zero"],
            "nulls":     r[f"{c}__null"],
        })

numeric_df = spark.createDataFrame(numeric_profile)

print(f"{numeric_df.count()} numeric columns profiled")
display(numeric_df.orderBy("table", "column"))

print("Columns containing negative values:")
display(numeric_df.filter(F.col("negatives") > 0))

print("Columns where more than 5% of rows are zero:")
display(
    numeric_df
    .join(profile_df.select("table", "rows"), on="table")
    .withColumn("zero_pct", F.round(100.0 * F.col("zeros") / F.col("rows"), 2))
    .filter(F.col("zero_pct") > 5)
    .select("table", "column", "zeros", "zero_pct", "min", "max")
    .orderBy(F.desc("zero_pct"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT count(*) AS impossible_fills
# MAGIC FROM bronze_fuel_purchases f
# MAGIC JOIN bronze_trucks t ON f.truck_id = t.truck_id
# MAGIC WHERE f.gallons > t.tank_capacity_gallons

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC     count(*)                                                  AS events,
# MAGIC     sum(CASE WHEN detention_minutes =
# MAGIC              (unix_timestamp(actual_datetime)
# MAGIC               - unix_timestamp(scheduled_datetime)) / 60
# MAGIC              THEN 1 ELSE 0 END)                               AS exact_match,
# MAGIC     round(avg(abs(detention_minutes
# MAGIC               - (unix_timestamp(actual_datetime)
# MAGIC                  - unix_timestamp(scheduled_datetime)) / 60)), 1) AS avg_gap
# MAGIC FROM bronze_delivery_events

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 1. Are they statistically independent, or did the generator link them?
# MAGIC SELECT round(corr(detention_minutes,
# MAGIC         (unix_timestamp(actual_datetime)
# MAGIC          - unix_timestamp(scheduled_datetime)) / 60), 4) AS correlation
# MAGIC FROM bronze_delivery_events

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 2. What does the existing on_time_flag actually track?
# MAGIC SELECT
# MAGIC     on_time_flag,
# MAGIC     count(*)                                                       AS events,
# MAGIC     round(avg((unix_timestamp(actual_datetime)
# MAGIC                - unix_timestamp(scheduled_datetime)) / 60), 1)     AS avg_variance,
# MAGIC     round(avg(detention_minutes), 1)                               AS avg_detention
# MAGIC FROM bronze_delivery_events
# MAGIC GROUP BY on_time_flag

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC     on_time_flag,
# MAGIC     round(min((unix_timestamp(actual_datetime)
# MAGIC                - unix_timestamp(scheduled_datetime)) / 60), 1)  AS min_variance,
# MAGIC     round(max((unix_timestamp(actual_datetime)
# MAGIC                - unix_timestamp(scheduled_datetime)) / 60), 1)  AS max_variance,
# MAGIC     count(*)                                                    AS events
# MAGIC FROM bronze_delivery_events
# MAGIC GROUP BY on_time_flag

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 3. How much detention is actually billable?
# MAGIC SELECT
# MAGIC     count(*)                                                        AS events,
# MAGIC     sum(CASE WHEN detention_minutes > 120 THEN 1 ELSE 0 END)        AS over_free_time,
# MAGIC     sum(greatest(detention_minutes - 120, 0))                       AS billable_minutes
# MAGIC FROM bronze_delivery_events

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 5B - OUTLIERS AND DISTRIBUTION SHAPE
# ===========================================================================
# Cell 5 reports range and spread. This cell identifies which rows sit outside
# the plausible range, and how skewed each distribution is.
#
# Two methods, because they answer different questions:
#
#   IQR (Tukey)   Q1 - 1.5*IQR to Q3 + 1.5*IQR. Distribution-free, so it holds
#                 for skewed data such as costs and revenue. This is the
#                 primary method here.
#   Z-score       Number of standard deviations from the mean. Assumes roughly
#                 normal data, and the mean and stddev are themselves pulled
#                 by the outliers. Reported for comparison only.
#
# An outlier is not automatically an error. A single 500-gallon fuel purchase
# may be legitimate. The purpose is to decide, per column, whether extreme
# values are genuine business events (keep), data entry errors (quarantine),
# or impossible values (fail the run).
 
from pyspark.sql import functions as F
from decimal import Decimal
 
# approxQuantile with relativeError 0.01 is accurate enough for profiling and
# far cheaper than an exact quantile calculation on six-figure tables.
QUANTILE_ERROR = 0.01

NUMERIC_TYPES = ("int", "bigint", "smallint", "double", "float", "decimal")
ID_SUFFIXES   = ("_id", "_number")


def numeric_columns(table_name):
    """Numeric measure columns for one bronze table."""
    df = spark.table(f"{BRONZE_PREFIX}{table_name}")
    return [
        c for c, t in df.dtypes
        if t.startswith(NUMERIC_TYPES)
        and c not in LINEAGE_COLUMNS
        and not c.endswith(ID_SUFFIXES)
    ]




outlier_profile = []
 
for name in TABLES:
    df   = spark.table(f"{BRONZE_PREFIX}{name}")
    cols = numeric_columns(name)
    if not cols:
        continue
 
    rows = df.count()
 
    for c in cols:
        q1, median, q3 = df.approxQuantile(c, [0.25, 0.5, 0.75], QUANTILE_ERROR)
        if q1 is None:
            continue
 
        iqr         = q3 - q1
        lower_fence = q1 - 1.5 * iqr
        upper_fence = q3 + 1.5 * iqr
 
        stats = df.agg(
            F.avg(c).alias("mean"),
            F.stddev(c).alias("std"),
            F.sum((F.col(c) < lower_fence).cast("int")).alias("below"),
            F.sum((F.col(c) > upper_fence).cast("int")).alias("above"),
        ).collect()[0]
 
        mean, std = stats["mean"], stats["std"]
 
        # Rows beyond three standard deviations, for comparison with IQR.
        if std and std > 0:
            z_outliers = df.filter(
                F.abs((F.col(c) - F.lit(mean)) / F.lit(std)) > 3
            ).count()
        else:
            z_outliers = 0
 
        iqr_outliers = stats["below"] + stats["above"]
 
        outlier_profile.append({
            "table":         name,
            "column":        c,
            "q1":            round(q1, 2),
            "median":        round(median, 2),
            "q3":            round(q3, 2),
            "lower_fence":   round(lower_fence, 2),
            "upper_fence":   round(upper_fence, 2),
            "iqr_outliers":  iqr_outliers,
            "iqr_pct":       round(100.0 * iqr_outliers / rows, 2) if rows else 0.0,
            "z3_outliers":   z_outliers,
            # (mean - median) / stddev. Positive means a long right tail,
            # which is normal for cost and revenue columns.
            "skew_indicator": round((float(mean) - median) / std, 2) if std and std > 0 else None,
        })
 
outlier_df = spark.createDataFrame(outlier_profile)
 
print("Columns ranked by proportion of IQR outliers:")
display(outlier_df.orderBy(F.desc("iqr_pct")))
 
# A large gap between the two methods is informative in itself: it means the
# distribution is skewed enough that the mean and stddev are unreliable.
print("Columns where the two methods disagree most:")
display(
    outlier_df
    .withColumn("method_gap", F.abs(F.col("iqr_outliers") - F.col("z3_outliers")))
    .orderBy(F.desc("method_gap"))
    .limit(15)
)
 
# Inspect the actual extreme rows for the worst offenders. Aggregate counts
# say a column has outliers; only the rows themselves say whether they are
# errors or genuine events.
WORST = (
    outlier_df
    .filter(F.col("iqr_pct") > 1)
    .orderBy(F.desc("iqr_pct"))
    .limit(5)
    .collect()
)
 
for row in WORST:
    print(f"\nExtreme values: {row['table']}.{row['column']} "
          f"(fence {row['lower_fence']} to {row['upper_fence']})")
    display(
        spark.table(f"{BRONZE_PREFIX}{row['table']}")
        .filter(
            (F.col(row["column"]) < row["lower_fence"]) |
            (F.col(row["column"]) > row["upper_fence"])
        )
        .orderBy(F.desc(row["column"]))
        .limit(10)
    )
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 6 - DATE RANGES AND COVERAGE
# ===========================================================================
# Range, distinct dates, and future-dated rows.
#
# Two things this catches: dates outside the expected 2022-2024 window, and
# how many rows share each date. The second matters because it quantifies the
# watermark boundary condition that shaped the bronze design.
 
EXPECTED_START = "2022-01-01"
EXPECTED_END   = "2024-12-31"
 
date_profile = []
 
for name, cfg in TABLES.items():
    df = spark.table(f"{BRONZE_PREFIX}{name}")
    for c in cfg["date_columns"]:
        if c not in df.columns:
            continue
        r = df.agg(
            F.min(c).alias("min_v"),
            F.max(c).alias("max_v"),
            F.countDistinct(c).alias("distinct_v"),
            F.sum((F.col(c) > F.current_date()).cast("int")).alias("future_v"),
            F.sum(F.col(c).isNull().cast("int")).alias("null_v"),
        ).collect()[0]
 
        rows_total = df.count()
        date_profile.append({
            "table":            name,
            "column":           c,
            "min":              str(r["min_v"]),
            "max":              str(r["max_v"]),
            "distinct_dates":   r["distinct_v"],
            "rows_per_date":    round(rows_total / r["distinct_v"], 1) if r["distinct_v"] else 0,
            "future_dated":     r["future_v"],
            "nulls":            r["null_v"],
        })
 
display(spark.createDataFrame(date_profile).orderBy("table", "column"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 6B - TEMPORAL CONTINUITY
# ===========================================================================
# Cell 6 reports the date range. This cell checks whether the range is
# continuous, and whether volume per period is stable.
#
# Gaps matter for two reasons. A missing date could mean the source genuinely
# had no activity, or that a batch failed and was never reprocessed. Bronze
# cannot distinguish the two, so the gap has to be surfaced and explained
# rather than silently accepted. Volume spikes and collapses matter for the
# same reason: a month at ten percent of the usual count is either a real
# business event or a partial load.
 
CONTINUITY_CHECKS = [
    ("loads",               "load_date"),
    ("trips",               "dispatch_date"),
    ("delivery_events",     "actual_datetime"),
    ("fuel_purchases",      "purchase_date"),
    ("maintenance_records", "maintenance_date"),
]
 
for table, column in CONTINUITY_CHECKS:
    print(f"\n{table}.{column}")
 
    daily = (
        spark.table(f"{BRONZE_PREFIX}{table}")
        .filter(F.col(column).isNotNull())
        .withColumn("d", F.to_date(F.col(column)))
        .groupBy("d")
        .count()
    )
 
    bounds = daily.agg(
        F.min("d").alias("first_day"),
        F.max("d").alias("last_day"),
        F.count("*").alias("days_present"),
        F.avg("count").alias("avg_per_day"),
        F.min("count").alias("min_per_day"),
        F.max("count").alias("max_per_day"),
    ).collect()[0]
 
    span_days = (bounds["last_day"] - bounds["first_day"]).days + 1
 
    print(f"  {bounds['first_day']} to {bounds['last_day']}  "
          f"({span_days} calendar days, {bounds['days_present']} with data, "
          f"{span_days - bounds['days_present']} missing)")
    print(f"  per day: min {bounds['min_per_day']}, "
          f"avg {bounds['avg_per_day']:.1f}, max {bounds['max_per_day']}")
 
    # Monthly volume. A collapse or spike here is more visible than in the
    # daily series and is the level a reviewer would actually look at.
    display(
        spark.table(f"{BRONZE_PREFIX}{table}")
        .filter(F.col(column).isNotNull())
        .withColumn("month", F.date_format(F.col(column), "yyyy-MM"))
        .groupBy("month")
        .count()
        .orderBy("month")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 7 - REFERENTIAL INTEGRITY
# ===========================================================================
# Orphan rate per foreign key: rows whose key has no match in the parent table.
#
# The source database enforces these constraints, so orphans should be zero.
# The Fabric warehouse will NOT enforce them, so silver has to be the place
# this is verified. Any orphan found here would silently disappear from an
# inner join in gold.
#
# Nulls are counted separately from orphans. Nulls are a known, intentional
# condition (~2% of trips are unassigned) and resolve to the Unknown dimension
# member in gold. An orphan is a genuine integrity failure.
 
RELATIONSHIPS = [
    ("loads",               "customer_id",  "customers",   "customer_id"),
    ("loads",               "route_id",     "routes",      "route_id"),
    ("trips",               "load_id",      "loads",       "load_id"),
    ("trips",               "driver_id",    "drivers",     "driver_id"),
    ("trips",               "truck_id",     "trucks",      "truck_id"),
    ("trips",               "trailer_id",   "trailers",    "trailer_id"),
    ("fuel_purchases",      "trip_id",      "trips",       "trip_id"),
    ("fuel_purchases",      "truck_id",     "trucks",      "truck_id"),
    ("fuel_purchases",      "driver_id",    "drivers",     "driver_id"),
    ("maintenance_records", "truck_id",     "trucks",      "truck_id"),
    ("delivery_events",     "load_id",      "loads",       "load_id"),
    ("delivery_events",     "trip_id",      "trips",       "trip_id"),
    ("delivery_events",     "facility_id",  "facilities",  "facility_id"),
    ("safety_incidents",    "trip_id",      "trips",       "trip_id"),
    ("safety_incidents",    "truck_id",     "trucks",      "truck_id"),
    ("safety_incidents",    "driver_id",    "drivers",     "driver_id"),
]
 
integrity = []
 
for child, child_col, parent, parent_col in RELATIONSHIPS:
    child_df  = spark.table(f"{BRONZE_PREFIX}{child}")
    parent_df = spark.table(f"{BRONZE_PREFIX}{parent}").select(
        F.col(parent_col).alias("_pk")
    ).distinct()
 
    total  = child_df.count()
    nulls  = child_df.filter(F.col(child_col).isNull()).count()
 
    orphans = (
        child_df.filter(F.col(child_col).isNotNull())
        .join(parent_df, F.col(child_col) == F.col("_pk"), "left_anti")
        .count()
    )
 
    integrity.append({
        "child":        child,
        "column":       child_col,
        "parent":       parent,
        "rows":         total,
        "nulls":        nulls,
        "null_pct":     round(100.0 * nulls / total, 2) if total else 0.0,
        "orphans":      orphans,
        "orphan_pct":   round(100.0 * orphans / total, 2) if total else 0.0,
    })
 
integrity_df = spark.createDataFrame(integrity)
display(integrity_df.orderBy(F.desc("orphan_pct"), F.desc("null_pct")))
 
orphan_total = integrity_df.agg(F.sum("orphans")).collect()[0][0]
print("Referential integrity clean." if orphan_total == 0
      else f"{orphan_total:,} orphaned rows found. Investigate before silver.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 'safety_incidents' AS tbl, location_city, count(DISTINCT location_state) AS states
# MAGIC FROM bronze_safety_incidents GROUP BY location_city HAVING count(DISTINCT location_state) > 1
# MAGIC UNION ALL
# MAGIC SELECT 'fuel_purchases', location_city, count(DISTINCT location_state)
# MAGIC FROM bronze_fuel_purchases GROUP BY location_city HAVING count(DISTINCT location_state) > 1
# MAGIC UNION ALL
# MAGIC SELECT 'delivery_events', location_city, count(DISTINCT location_state)
# MAGIC FROM bronze_delivery_events GROUP BY location_city HAVING count(DISTINCT location_state) > 1

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 8 - BUSINESS RULE VALIDATION
# ===========================================================================
# Domain-specific checks that generic profiling cannot reach. Each result
# becomes either a silver validation rule, a quarantine rule, or a documented
# characteristic of the source.
#
# Removed from the original draft, now answered elsewhere:
#   - detention vs scheduled variance: they measure different things
#     (correlation 0.0255); on_time_flag has a symmetric 120-minute window
#   - referential integrity: cell 7, zero orphans
#   - date ranges and coverage: cells 6 and 6B

from pyspark.sql import functions as F

results = []


def check(name, sql, severity):
    """Run one validation query and record the failure count."""
    n = spark.sql(sql).collect()[0][0]
    results.append({"check": name, "failures": n, "severity": severity})
    print(f"{'PASS' if n == 0 else 'FAIL'}  {name:<46}{n:>10,}")
    return n


print(f"{'':<52}{'failures':>10}")
print("-" * 62)

# ---------------------------------------------------------------------------
# Derived column identities
# ---------------------------------------------------------------------------
# Three columns store a value computed from others in the same row. Profiling
# showed two of them match exactly at the mean, which for a sum proves the
# identity holds per row. Multiplication does not carry that guarantee, so
# fuel cost needs a genuine row-level test.
#
# A failure means the source contains two contradictory truths for one fact.

check("claim = vehicle + cargo damage", """
    SELECT count(*) FROM bronze_safety_incidents
    WHERE abs(claim_amount - (vehicle_damage_cost + cargo_damage_cost)) > 0.01
""", "quarantine")

check("maintenance total = labour + parts", """
    SELECT count(*) FROM bronze_maintenance_records
    WHERE abs(total_cost - (labor_cost + parts_cost)) > 0.01
""", "quarantine")

check("fuel cost = gallons x price", """
    SELECT count(*) FROM bronze_fuel_purchases
    WHERE abs(total_cost - gallons * price_per_gallon) > 0.05
""", "quarantine")

# ---------------------------------------------------------------------------
# Physical impossibilities
# ---------------------------------------------------------------------------
# Cross-table constraints the source database could not enforce.
#
# The tank capacity check already returned 18,105 failures on 196,442 rows.
# A purchase larger than the truck's tank is physically impossible, so those
# rows are wrong regardless of which column is at fault.

check("fuel purchase exceeds tank capacity", """
    SELECT count(*)
    FROM bronze_fuel_purchases f
    JOIN bronze_trucks t ON f.truck_id = t.truck_id
    WHERE f.gallons > t.tank_capacity_gallons
""", "quarantine")

check("service odometer below acquisition mileage", """
    SELECT count(*)
    FROM bronze_maintenance_records m
    JOIN bronze_trucks t ON m.truck_id = t.truck_id
    WHERE m.odometer_reading < t.acquisition_mileage
""", "quarantine")

check("idle time exceeds trip duration", """
    SELECT count(*) FROM bronze_trips
    WHERE idle_time_hours > actual_duration_hours
""", "quarantine")

check("reported mpg disagrees with distance / gallons", """
    SELECT count(*) FROM bronze_trips
    WHERE fuel_gallons_used > 0
      AND abs(average_mpg - actual_distance_miles / fuel_gallons_used) > 0.5
""", "quarantine")

# ---------------------------------------------------------------------------
# Temporal logic
# ---------------------------------------------------------------------------
# Events must occur in a possible order. A trip cannot be dispatched before
# its load is booked; a truck cannot be serviced before it was bought.

check("trip dispatched before load booked", """
    SELECT count(*)
    FROM bronze_trips t
    JOIN bronze_loads l ON t.load_id = l.load_id
    WHERE t.dispatch_date < l.load_date
""", "quarantine")

check("maintenance before truck acquired", """
    SELECT count(*)
    FROM bronze_maintenance_records m
    JOIN bronze_trucks t ON m.truck_id = t.truck_id
    WHERE m.maintenance_date < t.acquisition_date
""", "quarantine")

check("incident outside driver employment period", """
    SELECT count(*)
    FROM bronze_safety_incidents s
    JOIN bronze_drivers d ON s.driver_id = d.driver_id
    WHERE date(s.incident_date) < d.hire_date
       OR (d.termination_date IS NOT NULL
           AND date(s.incident_date) > d.termination_date)
""", "quarantine")

check("driver terminated before hired", """
    SELECT count(*) FROM bronze_drivers
    WHERE termination_date IS NOT NULL
      AND termination_date < hire_date
""", "fail")

# ---------------------------------------------------------------------------
# Categorical consistency
# ---------------------------------------------------------------------------
# A driver marked Active should have no termination date, and vice versa.
# Geography should be internally consistent: one city belongs to one state.

check("active driver has a termination date", """
    SELECT count(*) FROM bronze_drivers
    WHERE employment_status = 'Active' AND termination_date IS NOT NULL
""", "quarantine")

check("inactive driver has no termination date", """
    SELECT count(*) FROM bronze_drivers
    WHERE employment_status <> 'Active' AND termination_date IS NULL
""", "quarantine")

for tbl in ["safety_incidents", "fuel_purchases", "delivery_events"]:
    check(f"{tbl}: city mapped to multiple states", f"""
        SELECT count(*) FROM (
            SELECT location_city
            FROM bronze_{tbl}
            GROUP BY location_city
            HAVING count(DISTINCT location_state) > 1
        )
    """, "document")

# ---------------------------------------------------------------------------
# Structural expectations
# ---------------------------------------------------------------------------
# Relationships the schema implies but does not enforce. A violation here
# would change the grain of the model, so these fail the run rather than
# quarantining rows.

check("load without exactly one trip", """
    SELECT count(*) FROM (
        SELECT l.load_id
        FROM bronze_loads l
        LEFT JOIN bronze_trips t ON l.load_id = t.load_id
        GROUP BY l.load_id
        HAVING count(t.trip_id) <> 1
    )
""", "fail")

check("load without exactly one pickup and one delivery", """
    SELECT count(*) FROM (
        SELECT load_id
        FROM bronze_delivery_events
        GROUP BY load_id
        HAVING count(DISTINCT event_type) <> 2 OR count(*) <> 2
    )
""", "fail")

check("delivery recorded before its pickup", """
    SELECT count(*) FROM (
        SELECT load_id,
               max(CASE WHEN event_type = 'Pickup'   THEN actual_datetime END) AS picked,
               max(CASE WHEN event_type = 'Delivery' THEN actual_datetime END) AS delivered
        FROM bronze_delivery_events
        GROUP BY load_id
    ) WHERE delivered < picked
""", "fail")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

summary = spark.createDataFrame(results)

print("\nFailures by severity:")
display(
    summary.filter(F.col("failures") > 0)
           .orderBy(F.desc("failures"))
)

blocking = summary.filter(
    (F.col("failures") > 0) & (F.col("severity") == "fail")
).count()

print(f"\n{blocking} blocking issue(s). "
      f"{summary.filter((F.col('failures') > 0) & (F.col('severity') == 'quarantine')).count()} "
      f"quarantine rule(s) needed.")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC     round(actual_duration_hours)                     AS duration_hrs,
# MAGIC     count(*)                                         AS trips,
# MAGIC     sum(CASE WHEN idle_time_hours > actual_duration_hours
# MAGIC              THEN 1 ELSE 0 END)                      AS impossible
# MAGIC FROM bronze_trips
# MAGIC GROUP BY round(actual_duration_hours)
# MAGIC ORDER BY duration_hrs
# MAGIC LIMIT 25

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT count(*) AS plausible_swap
# MAGIC FROM (
# MAGIC     SELECT load_id,
# MAGIC            max(CASE WHEN event_type = 'Pickup'   THEN actual_datetime END) AS picked,
# MAGIC            max(CASE WHEN event_type = 'Delivery' THEN actual_datetime END) AS delivered
# MAGIC     FROM bronze_delivery_events GROUP BY load_id
# MAGIC ) e
# MAGIC JOIN bronze_trips t USING (load_id)
# MAGIC WHERE e.delivered < e.picked
# MAGIC   AND abs(unix_timestamp(e.picked) - unix_timestamp(e.delivered)) / 3600
# MAGIC       BETWEEN t.actual_duration_hours * 0.8 AND t.actual_duration_hours * 1.2

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC     count(*)                                             AS affected_loads,
# MAGIC     round(avg((unix_timestamp(picked)
# MAGIC                - unix_timestamp(delivered)) / 3600), 1)  AS avg_reversal_hrs,
# MAGIC     max((unix_timestamp(picked)
# MAGIC          - unix_timestamp(delivered)) / 3600)            AS worst_reversal_hrs
# MAGIC FROM (
# MAGIC     SELECT load_id,
# MAGIC            max(CASE WHEN event_type = 'Pickup'   THEN actual_datetime END) AS picked,
# MAGIC            max(CASE WHEN event_type = 'Delivery' THEN actual_datetime END) AS delivered
# MAGIC     FROM bronze_delivery_events GROUP BY load_id
# MAGIC ) WHERE delivered < picked


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 8B - INSPECT THE FAILURES
# ===========================================================================
# Counts say a rule is broken. Only the rows say how. The tank capacity
# failure is the largest known issue, so it gets examined first: the question
# is whether gallons is wrong, tank_capacity is wrong, or the truck
# assignment is wrong.

print("Tank capacity failures, by truck size:")
display(spark.sql("""
    SELECT
        t.tank_capacity_gallons,
        count(*)                                  AS purchases,
        sum(CASE WHEN f.gallons > t.tank_capacity_gallons
                 THEN 1 ELSE 0 END)               AS impossible,
        round(100.0 * sum(CASE WHEN f.gallons > t.tank_capacity_gallons
                               THEN 1 ELSE 0 END) / count(*), 1) AS pct,
        max(f.gallons)                            AS largest_fill
    FROM bronze_fuel_purchases f
    JOIN bronze_trucks t ON f.truck_id = t.truck_id
    GROUP BY t.tank_capacity_gallons
    ORDER BY t.tank_capacity_gallons
"""))

# If gallons is uniformly distributed regardless of tank size, the generator
# ignored the constraint entirely and this is a source characteristic rather
# than a correctable error.
print("Fill volume distribution by tank size:")
display(spark.sql("""
    SELECT
        t.tank_capacity_gallons,
        round(min(f.gallons), 1)  AS min_fill,
        round(avg(f.gallons), 1)  AS avg_fill,
        round(max(f.gallons), 1)  AS max_fill
    FROM bronze_fuel_purchases f
    JOIN bronze_trucks t ON f.truck_id = t.truck_id
    GROUP BY t.tank_capacity_gallons
    ORDER BY t.tank_capacity_gallons
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 9 - FREE TEXT
# ===========================================================================
# The two columns the spaCy categorisation will consume. Length distribution
# and sample values determine whether rule-based matching is viable.
 
for table, column in [("maintenance_records", "service_description"),
                      ("safety_incidents",    "description"),
                      ("maintenance_records", "maintenance_type"),
                      ("safety_incidents",    "incident_type")]:
    print(f"\n{table}.{column}")
    df = spark.table(f"{BRONZE_PREFIX}{table}")
 
    display(df.select(
        F.count(column).alias("non_null"),
        F.countDistinct(column).alias("distinct"),
        F.min(F.length(column)).alias("min_len"),
        F.avg(F.length(column)).alias("avg_len"),
        F.max(F.length(column)).alias("max_len"),
    ))
 
    display(
        df.groupBy(column).count()
          .orderBy(F.desc("count"))
          .limit(20)
    )
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 10 - PERSIST THE PROFILE
# ===========================================================================
# Written to Delta so the silver design can cite measured numbers, and so a
# later run can be compared against this baseline to detect drift.
#
# Each frame is stamped with the run id and appended, building a history of
# profiles rather than overwriting the previous one.

date_df    = spark.createDataFrame(date_profile)
summary_df = spark.createDataFrame(results)      # cell 8 rule results

PROFILE_OUTPUTS = [
    (profile_df,   "profile_tables"),
    (column_df,    "profile_columns"),
    (numeric_df,   "profile_numeric"),
    (outlier_df,   "profile_outliers"),
    (date_df,      "profile_dates"),
    (integrity_df, "profile_integrity"),
    (summary_df,   "profile_rules"),
]

for df_out, table_name in PROFILE_OUTPUTS:
    (
        df_out
        .withColumn("_profile_run", F.lit(RUN_ID))
        .write.format("delta")
        .mode("append")
        .saveAsTable(f"lh_logistics_ops.dbo.{table_name}")
    )
    print(f"saved {table_name:<22}{df_out.count():>6} rows")

print(f"\nProfile {RUN_ID} complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
