"""Stateless transformations.

Every function here is *stateless*: each output row depends only on the input
row (or only on the rows of the current micro-batch). Nothing is remembered
between micro-batches, so these work on both streaming and static DataFrames.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config import LARGE_TXN_THRESHOLD, VALID_TXN_TYPES


def clean_transactions(df: DataFrame) -> DataFrame:
    """Stateless, narrow transformations (no shuffle):

    * withColumn  - normalise txn_type, parse event_time, flag large transactions
    * filter      - drop records without an account, with non-positive amounts,
                    or with an unknown transaction type

    Narrow transformations run inside each existing partition, so Spark does
    not move data across the network for them.
    """
    return (
        df.withColumn("txn_type", F.upper(F.trim(F.col("txn_type"))))
          .withColumn("event_ts", F.to_timestamp("event_time"))
          .withColumn("is_large", F.col("amount") >= F.lit(LARGE_TXN_THRESHOLD))
          .filter(F.col("account_id").isNotNull())
          .filter(F.col("amount") > 0)
          .filter(F.col("txn_type").isin(VALID_TXN_TYPES))
          .select("txn_id", "account_id", "amount", "txn_type", "is_large", "event_ts")
    )


def batch_counts(df: DataFrame) -> DataFrame:
    """Per-account count over ONE micro-batch (stateless).

    groupBy is a WIDE transformation: it triggers a shuffle. Because it runs on
    the (static) batch DataFrame handed to foreachBatch, the result forgets
    everything once the batch is finished.
    """
    return df.groupBy("account_id").count().withColumnRenamed("count", "batch_count")


def running_counts(df: DataFrame) -> DataFrame:
    """Running per-account count over ALL batches (stateful).

    The code looks almost identical to batch_counts, but applied to a
    *streaming* DataFrame Spark plans a StateStoreSave/Restore operator: the
    partial counts per account are kept in the state store (backed by the
    checkpoint) and merged with each new micro-batch.
    """
    return df.groupBy("account_id").agg(F.count(F.lit(1)).alias("running_count"))
