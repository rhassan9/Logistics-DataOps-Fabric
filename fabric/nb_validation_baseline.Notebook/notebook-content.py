# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f34bc028-a89a-46c4-a3ef-6b69a368f9c6",
# META       "default_lakehouse_name": "lh_logistics_ops",
# META       "default_lakehouse_workspace_id": "a77071e4-bd2a-4979-8910-91ddb8cd2a09",
# META       "known_lakehouses": [
# META         {
# META           "id": "f34bc028-a89a-46c4-a3ef-6b69a368f9c6"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Validation Baseline — Kaggle Aggregate Tables
#
# Source      : Files/validation/*.csv in lh_logistics_ops
# Destination : lh_logistics_ops
# Purpose     : expected baseline for reconciling the gold aggregates
#
# These two tables ship with the Kaggle export as pre-computed monthly
# rollups. They are OLAP artifacts, so they were never loaded into the source
# database and never enter the medallion layers. They are loaded here, outside
# the pipeline, purely as the expected answer that gold's rebuilt aggregates
# are compared against.
#
# Attach lh_logistics_ops as the default lakehouse before running.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 1 — LOAD
# ===========================================================================
 
from pyspark.sql import functions as F, types as T
from datetime import datetime
 
OPS_LAKEHOUSE = "lh_logistics_ops"
OPS_SCHEMA    = "dbo"
 
LOADED_AT = datetime.now()
 
# Explicit schemas rather than inference, so a re-run cannot silently change a
# type and shift a comparison.
BASELINES = {
    "driver_monthly_metrics": T.StructType([
        T.StructField("driver_id",              T.StringType()),
        T.StructField("month",                  T.DateType()),
        T.StructField("trips_completed",        T.IntegerType()),
        T.StructField("total_miles",            T.IntegerType()),
        T.StructField("total_revenue",          T.DecimalType(14, 2)),
        T.StructField("average_mpg",            T.DecimalType(8, 2)),
        T.StructField("total_fuel_gallons",     T.DecimalType(12, 2)),
        T.StructField("on_time_delivery_rate",  T.DecimalType(6, 4)),
        T.StructField("average_idle_hours",     T.DecimalType(8, 2)),
    ]),
    "truck_utilization_metrics": T.StructType([
        T.StructField("truck_id",            T.StringType()),
        T.StructField("month",               T.DateType()),
        T.StructField("trips_completed",     T.IntegerType()),
        T.StructField("total_miles",         T.IntegerType()),
        T.StructField("total_revenue",       T.DecimalType(14, 2)),
        T.StructField("average_mpg",         T.DecimalType(8, 2)),
        T.StructField("maintenance_events",  T.IntegerType()),
        T.StructField("maintenance_cost",    T.DecimalType(14, 2)),
        T.StructField("downtime_hours",      T.DecimalType(10, 2)),
        T.StructField("utilization_rate",    T.DecimalType(6, 4)),
    ]),
}
 
for name, schema in BASELINES.items():
    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(f"Files/validation/{name}.csv")
        .withColumn("_loaded_at", F.lit(LOADED_AT))
        .withColumn("_source",    F.lit("kaggle_expected_baseline"))
    )
 
    target = f"{OPS_LAKEHOUSE}.{OPS_SCHEMA}.validation_{name}"
 
    # Overwrite, not append: this is a fixed baseline, not a growing log.
    df.write.format("delta").mode("overwrite").saveAsTable(target)
    print(f"{target:<60}{df.count():>8,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 2 — IS THE BASELINE REPRODUCIBLE?
# ===========================================================================
# Every constraint violation found during profiling came from columns
# generated without reference to each other. If these aggregates were produced
# the same way, they cannot be reproduced from the transactional tables, and
# their value as a reconciliation baseline is nil.
#
# Two unambiguous metrics are tested. trips_completed is a count and
# total_miles is a sum, so neither depends on a definition we have to guess.
#
# Trips with no driver are excluded from the silver side: the baseline is keyed
# on driver, so an unassigned trip cannot appear in it.
 
silver_driver_monthly = (
    spark.table("lh_logistics_silver.dbo.silver_trips")
    .filter(F.col("driver_id").isNotNull())
    .withColumn("month", F.trunc("dispatch_date", "month"))
    .groupBy("driver_id", "month")
    .agg(
        F.count("*").alias("silver_trips"),
        F.sum("actual_distance_miles").alias("silver_miles"),
    )
)
 
baseline = (
    spark.table(f"{OPS_LAKEHOUSE}.{OPS_SCHEMA}.validation_driver_monthly_metrics")
    .select(
        "driver_id",
        "month",
        F.col("trips_completed").alias("baseline_trips"),
        F.col("total_miles").alias("baseline_miles"),
    )
)
 
comparison = (
    baseline.join(silver_driver_monthly, ["driver_id", "month"], "full_outer")
    .withColumn("trips_diff", F.col("silver_trips") - F.col("baseline_trips"))
    .withColumn("miles_diff", F.col("silver_miles") - F.col("baseline_miles"))
)
 
summary = comparison.agg(
    F.count("*").alias("driver_months"),
    F.sum(F.when(F.col("silver_trips").isNull(), 1).otherwise(0)).alias("baseline_only"),
    F.sum(F.when(F.col("baseline_trips").isNull(), 1).otherwise(0)).alias("silver_only"),
    F.sum(F.when(F.col("trips_diff") == 0, 1).otherwise(0)).alias("trips_match"),
    F.sum(F.when(F.col("miles_diff") == 0, 1).otherwise(0)).alias("miles_match"),
)
 
display(summary)
 
print("Largest discrepancies:")
display(
    comparison
    .filter((F.col("trips_diff") != 0) | (F.col("miles_diff") != 0))
    .orderBy(F.abs(F.col("miles_diff")).desc())
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================
# CELL 3 — TRUCK BASELINE REPRODUCIBILITY
# ===========================================================================
# Test whether the Kaggle truck_utilization_metrics baseline can be
# reproduced from the transactional Silver tables.
#
# Two unambiguous metrics are tested:
#   - trips_completed = COUNT(*) from silver_trips
#   - total_miles     = SUM(actual_distance_miles)
#
# The comparison is at the truck × calendar month grain.
#
# Trips with no truck_id are excluded because the baseline is keyed on truck.
# This mirrors the driver test above.

silver_truck_monthly = (
    spark.table("lh_logistics_silver.dbo.silver_trips")
    .filter(F.col("truck_id").isNotNull())
    .withColumn("month", F.trunc("dispatch_date", "month"))
    .groupBy("truck_id", "month")
    .agg(
        F.count("*").alias("silver_trips"),
        F.sum("actual_distance_miles").alias("silver_miles"),
    )
)

baseline_truck = (
    spark.table(
        f"{OPS_LAKEHOUSE}.{OPS_SCHEMA}.validation_truck_utilization_metrics"
    )
    .select(
        "truck_id",
        "month",
        F.col("trips_completed").alias("baseline_trips"),
        F.col("total_miles").alias("baseline_miles"),
    )
)

truck_comparison = (
    baseline_truck
    .join(
        silver_truck_monthly,
        ["truck_id", "month"],
        "full_outer"
    )
    .withColumn(
        "trips_diff",
        F.col("silver_trips") - F.col("baseline_trips")
    )
    .withColumn(
        "miles_diff",
        F.col("silver_miles") - F.col("baseline_miles")
    )
)

truck_summary = truck_comparison.agg(
    F.count("*").alias("truck_months"),

    F.sum(
        F.when(F.col("silver_trips").isNull(), 1).otherwise(0)
    ).alias("baseline_only"),

    F.sum(
        F.when(F.col("baseline_trips").isNull(), 1).otherwise(0)
    ).alias("silver_only"),

    F.sum(
        F.when(F.col("trips_diff") == 0, 1).otherwise(0)
    ).alias("trips_match"),

    F.sum(
        F.when(F.col("miles_diff") == 0, 1).otherwise(0)
    ).alias("miles_match"),
)

display(truck_summary)

print("Largest discrepancies:")
display(
    truck_comparison
    .filter(
        (F.col("trips_diff") != 0) |
        (F.col("miles_diff") != 0)
    )
    .orderBy(
        F.abs(F.col("miles_diff")).desc()
    )
    .limit(20)
)

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
