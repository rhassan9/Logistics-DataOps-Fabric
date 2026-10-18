# ---------------------------------------------------------------------------
# CELL 1: PARAMETERS
# ---------------------------------------------------------------------------
mode                 = "touch"         # touch | delete | restore
table                = "silver_trips"
key                  = "trip_id"
n                    = 100             # touch and delete: how many active keys
offset               = 200               # skip this many keys first, to pick different ones
keys_csv             = ""              # restore: the keys printed by the delete run
restore_from_version = -1              # restore: the version printed by the delete run


# ---------------------------------------------------------------------------
# CELL 2: APPLY
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F
 
full = f"lh_logistics_silver.dbo.{table}"
if mode not in ("touch", "delete", "restore"):
    raise ValueError("mode must be touch, delete or restore")
 
version_before = spark.sql(f"DESCRIBE HISTORY {full} LIMIT 1").first()["version"]
 
if mode == "restore":
    keys = [k.strip() for k in keys_csv.split(",") if k.strip()]
    if not keys or restore_from_version < 0:
        raise ValueError("restore needs keys_csv and restore_from_version from the delete run")
else:
    active = spark.table(full).where(~F.col("_is_deleted_in_source"))
    keys = [r[key] for r in active.select(key).orderBy(key).limit(offset + n).collect()][offset:]
    if len(keys) < n:
        raise ValueError(f"only {len(keys)} active keys available")
 
in_list = ", ".join("'" + str(k).replace("'", "''") + "'" for k in keys)
 
if mode == "touch":
    spark.sql(f"UPDATE {full} SET {key} = {key} WHERE {key} IN ({in_list})")
elif mode == "delete":
    spark.sql(f"""
        UPDATE {full}
        SET _is_deleted_in_source = true, _source_changed_at = current_timestamp()
        WHERE {key} IN ({in_list})
    """)
else:
    spark.sql(f"""
        MERGE INTO {full} AS t
        USING (SELECT {key}, _is_deleted_in_source, _source_changed_at
               FROM {full} VERSION AS OF {restore_from_version}
               WHERE {key} IN ({in_list})) AS s
        ON t.{key} = s.{key}
        WHEN MATCHED THEN UPDATE SET
            t._is_deleted_in_source = s._is_deleted_in_source,
            t._source_changed_at    = s._source_changed_at
    """)
 
version_after = spark.sql(f"DESCRIBE HISTORY {full} LIMIT 1").first()["version"]
print(f"{mode}: {len(keys)} keys in {table}; version {version_before} -> {version_after}")
print("keys_csv =", ",".join(str(k) for k in keys))
if mode == "delete":
    print("restore_from_version =", version_before)