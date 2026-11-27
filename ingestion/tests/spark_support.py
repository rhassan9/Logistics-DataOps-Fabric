"""Local Spark session and silver schemas for the silver routine tests.

The session mirrors the settings the routine relies on in Fabric Runtime 2.0:
UTC session time zone and ANSI mode. The silver schemas are read from
nb_silver_create_tables, the notebook that creates every silver table, so the
tests conform to the same columns and types the tables hold.
"""

from __future__ import annotations

import os
import re
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from ingestion import landing_contract as lc

REPO = Path(__file__).resolve().parents[2]
SILVER_DDL = REPO / "fabric" / "nb_silver_create_tables.Notebook" / "notebook-content.py"

# PySpark turns collected timestamps into naive datetimes in the process's own
# zone; UTC makes them read as the instants Spark holds.
os.environ["TZ"] = "UTC"
time.tzset()

_session = None


def spark():
    """One local session for the whole test run."""
    global _session
    if _session is None:
        _session = (
            SparkSession.builder.master("local[1]")
            .appName("silver-routine-tests")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", "true")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate()
        )
        _session.sparkContext.setLogLevel("ERROR")
    return _session


def spark_type(text):
    """Spark type of a type string such as 'decimal(14,2)' or 'STRING'."""
    text = text.strip().lower()
    simple = {"string": T.StringType(), "int": T.IntegerType(), "bigint": T.LongType(),
              "date": T.DateType(), "timestamp": T.TimestampType(), "boolean": T.BooleanType()}
    if text in simple:
        return simple[text]
    match = re.fullmatch(r"decimal\((\d+),\s*(\d+)\)", text)
    if match:
        return T.DecimalType(int(match[1]), int(match[2]))
    raise ValueError(f"unmapped type {text}")


def _ddl_columns(ddl):
    columns = []
    for line in ddl.splitlines():
        match = re.match(r"\s*([A-Za-z_]\w*)\s+([A-Z]+(?:\(\d+,\s*\d+\))?)", line)
        if match:
            columns.append((match[1], match[2]))
    return columns


def silver_schemas():
    """{table: StructType} of the 12 silver tables, from nb_silver_create_tables."""
    text = SILVER_DDL.read_text(encoding="utf-8")
    audit = _ddl_columns(re.search(r'AUDIT_COLUMNS\s*=\s*"""(.*?)"""', text, re.S)[1])
    schemas = {}
    for block in ("REFERENCE_TABLES", "TRANSACTIONAL_TABLES"):
        body = re.search(rf"{block}\s*=\s*\{{(.*?)\n\}}", text, re.S)[1]
        for name, ddl in re.findall(r'"(\w+)"\s*:\s*f?"""(.*?)"""', body, re.S):
            columns = _ddl_columns(ddl)
            if "{AUDIT_COLUMNS}" in ddl:
                columns += audit
            schemas[name] = T.StructType([T.StructField(c, spark_type(t), True) for c, t in columns])
    return schemas


def contract_schema(table, mode=lc.MODE_SNAPSHOT):
    return T.StructType([T.StructField(c, spark_type(t), True) for c, t in lc.file_columns(table, mode)])


def frame(table, rows, mode=lc.MODE_SNAPSHOT):
    """Batch rows of one table as a DataFrame with the contract's schema."""
    schema = contract_schema(table, mode)
    names = schema.names
    return spark().createDataFrame([tuple(r.get(c) for c in names) for r in rows], schema)


def empty(schema):
    return spark().createDataFrame([], schema)
