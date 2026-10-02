"""Two streaming queries over the same input: one stateless, one stateful.

Actions vs transformations in a streaming app
---------------------------------------------
* Transformations (filter, withColumn, groupBy...) only build a logical plan.
* Starting the query (writeStream...start()) is what makes Spark run it, one
  micro-batch at a time. Inside foreachBatch, collect()/show()/count() are
  ordinary batch ACTIONS that trigger a job per micro-batch.
"""
from pathlib import Path
from typing import Dict

from pyspark.sql import DataFrame, SparkSession

from src.config import TRANSACTION_SCHEMA
from src.transformations import (batch_counts, clean_transactions,
                                 running_counts)

Counts = Dict[int, Dict[str, int]]


def _read_stream(spark: SparkSession, input_dir: Path) -> DataFrame:
    """File source. maxFilesPerTrigger=1 -> one input file per micro-batch."""
    return (spark.readStream
            .schema(TRANSACTION_SCHEMA)
            .option("maxFilesPerTrigger", 1)
            .json(str(input_dir)))


def run_stateless(spark: SparkSession, input_dir: Path, checkpoint_dir: Path) -> Counts:
    """STATELESS: every micro-batch is processed in isolation.

    Returns {batch_id: {account_id: count_in_that_batch}}.
    """
    collected: Counts = {}

    def handle_batch(batch_df: DataFrame, batch_id: int) -> None:
        # batch_df is a normal, static DataFrame holding ONLY this batch's rows.
        print(f"\n[STATELESS] micro-batch {batch_id} "
              f"(partitions={batch_df.rdd.getNumPartitions()})")
        batch_df.orderBy("txn_id").show(truncate=False)           # action
        rows = batch_counts(batch_df).collect()                   # action + shuffle
        collected[batch_id] = {r["account_id"]: r["batch_count"] for r in rows}
        print(f"[STATELESS] counts this batch only: {dict(sorted(collected[batch_id].items()))}")

    cleaned = clean_transactions(_read_stream(spark, input_dir))
    query = (cleaned.writeStream
             .foreachBatch(handle_batch)
             .option("checkpointLocation", str(checkpoint_dir))
             .trigger(availableNow=True)   # process everything available, then stop
             .start())
    query.awaitTermination()
    return collected


def run_stateful(spark: SparkSession, input_dir: Path, checkpoint_dir: Path) -> Counts:
    """STATEFUL: a streaming aggregation keeps per-account counts in the state store.

    outputMode("complete") re-emits the whole result table after every batch,
    so each batch shows the ACCUMULATED count per account.
    Returns {batch_id: {account_id: running_count_after_that_batch}}.
    """
    collected: Counts = {}

    def handle_batch(result_df: DataFrame, batch_id: int) -> None:
        rows = result_df.collect()                                # action
        collected[batch_id] = {r["account_id"]: r["running_count"] for r in rows}
        print(f"\n[STATEFUL]  micro-batch {batch_id}: "
              f"running counts = {dict(sorted(collected[batch_id].items()))}")

    aggregated = running_counts(clean_transactions(_read_stream(spark, input_dir)))

    print("\n[STATEFUL] physical plan (look for Exchange = shuffle, "
          "StateStoreRestore/StateStoreSave = state):")
    aggregated.explain()

    query = (aggregated.writeStream
             .outputMode("complete")
             .foreachBatch(handle_batch)
             .option("checkpointLocation", str(checkpoint_dir))
             .trigger(availableNow=True)
             .start())
    query.awaitTermination()
    return collected
