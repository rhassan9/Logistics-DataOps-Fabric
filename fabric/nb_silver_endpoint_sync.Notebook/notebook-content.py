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

# ---------------------------------------------------------------------------
# CELL 1: SETUP
# ---------------------------------------------------------------------------
import sempy
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants
 
try:
    from sempy.fabric.sql_endpoint import refresh_sql_endpoint_metadata
except ImportError as exc:
    raise RuntimeError(
        f"semantic-link-sempy {sempy.__version__} has no sql_endpoint module; 0.13.0 or later is required."
    ) from exc
 
WAREHOUSE        = "wh_logistics_gold"
SILVER_LAKEHOUSE = "lh_logistics_silver"
SILVER           = "lh_logistics_silver.dbo"
SILVER_TABLES = [
    "silver_customers", "silver_drivers", "silver_trucks", "silver_trailers",
    "silver_routes", "silver_facilities", "silver_loads", "silver_trips",
    "silver_delivery_events", "silver_fuel_purchases",
    "silver_maintenance_records", "silver_safety_incidents",
]
 
 
def endpoint_query(sql):
    """Run T-SQL through the warehouse, which reads silver via its SQL endpoint."""
    return spark.read.option(Constants.DatabaseName, WAREHOUSE).synapsesql(sql)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# CELL 2: SYNC AND PROVE
# ---------------------------------------------------------------------------
# Spark reads silver straight from Delta. The gold procedures read it through
# the lakehouse SQL analytics endpoint, whose metadata syncs in the background
# and can lag. Force the sync, then prove both engines see the same silver
# before any dimension or fact is loaded from it.
sync = refresh_sql_endpoint_metadata(
    warehouse=SILVER_LAKEHOUSE, warehouse_type="Lakehouse"
)
display(sync)
if (sync["Status"] == "Failure").any():
    raise RuntimeError("Silver SQL endpoint sync reported a failed table; gold not loaded.")
 
# Same fingerprint on both sides: rows, deleted rows, latest change in epoch
# microseconds (timezone-neutral on both engines).
spark_fp = {
    t: tuple(
        spark.table(f"{SILVER}.{t}")
        .selectExpr(
            "COUNT(*)",
            "SUM(CAST(_is_deleted_in_source AS BIGINT))",
            "unix_micros(MAX(_source_changed_at))",
        )
        .first()
    )
    for t in SILVER_TABLES
}
 
endpoint_sql = "\nUNION ALL\n".join(
    f"""SELECT '{t}' AS table_name,
       COUNT_BIG(*) AS row_count,
       CAST(SUM(CASE WHEN _is_deleted_in_source = 1 THEN 1 ELSE 0 END) AS BIGINT) AS deleted_count,
       DATEDIFF_BIG(microsecond, '1970-01-01', MAX(_source_changed_at)) AS max_changed_us
FROM {SILVER_LAKEHOUSE}.dbo.{t}"""
    for t in SILVER_TABLES
)
endpoint_fp = {
    r["table_name"]: (r["row_count"], r["deleted_count"], r["max_changed_us"])
    for r in endpoint_query(endpoint_sql).collect()
}
 
stale = [
    f"{t}: spark={spark_fp[t]} endpoint={endpoint_fp.get(t)}"
    for t in SILVER_TABLES
    if spark_fp[t] != endpoint_fp.get(t)
]
if stale:
    raise RuntimeError(
        "SQL endpoint does not match silver Delta (rows, deleted, max_changed_us):\n"
        + "\n".join(stale)
    )
print(f"Silver SQL endpoint in sync for {len(SILVER_TABLES)} tables.")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
