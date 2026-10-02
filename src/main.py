"""Entry point:  python -m src.main

Runs the stateless and the stateful query on the same sample data and prints a
side-by-side comparison of current-batch counts vs accumulated running counts.
"""
import shutil
from pathlib import Path
from typing import List, Optional, Tuple

from pyspark.sql import SparkSession

from src.comparison import (build_comparison, format_table,
                            verify_running_matches_cumulative)
from src.config import SAMPLE_BATCHES_DIR, SHUFFLE_PARTITIONS, WORK_DIR
from src.stream_simulator import simulate_arrivals
from src.streaming_jobs import run_stateful, run_stateless


def create_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("Day24-Stateless-vs-Stateful-Streaming")
            .master("local[2]")
            .config("spark.hadoop.fs.defaultFS", "file:///")
            .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate())


def run_demo(spark: SparkSession, work_dir: Path = WORK_DIR,
             source_dir: Path = SAMPLE_BATCHES_DIR) -> Tuple[List[dict], bool]:
    work_dir = Path(work_dir)
    if work_dir.exists():
        shutil.rmtree(work_dir)  # fresh checkpoints => fresh (empty) state
    input_dir = work_dir / "input"
    n = simulate_arrivals(source_dir, input_dir)
    print(f"Simulated arrival of {n} batch files into {input_dir}")

    batch = run_stateless(spark, input_dir, work_dir / "checkpoint_stateless")
    running = run_stateful(spark, input_dir, work_dir / "checkpoint_stateful")
    return (build_comparison(batch, running),
            verify_running_matches_cumulative(batch, running))


def main(argv: Optional[list] = None) -> None:
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        rows, consistent = run_demo(spark)
        print("\n" + "=" * 46)
        print("CURRENT-BATCH vs RUNNING COUNTS (valid txns only)")
        print("=" * 46)
        print(format_table(rows))
        print("\nConsistency check (running == cumulative sum of batch counts):",
              "PASSED" if consistent else "FAILED")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
