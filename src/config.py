"""Project-wide configuration: schema, paths and Spark settings."""
from pathlib import Path

from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_BATCHES_DIR = PROJECT_ROOT / "data" / "batches"
WORK_DIR = PROJECT_ROOT / "work"  # streaming input + checkpoints (git-ignored)

# Explicit schema: streaming file sources REQUIRE a schema (no inference by default),
# and an explicit schema avoids an extra job to scan the data.
TRANSACTION_SCHEMA = StructType([
    StructField("txn_id", LongType(), True),
    StructField("account_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("txn_type", StringType(), True),
    StructField("event_time", StringType(), True),
])

VALID_TXN_TYPES = ["DEPOSIT", "WITHDRAWAL"]
LARGE_TXN_THRESHOLD = 500.0

# The state store is split into spark.sql.shuffle.partitions partitions.
# That number is FIXED for the life of a checkpoint, so choose it deliberately.
SHUFFLE_PARTITIONS = 2
