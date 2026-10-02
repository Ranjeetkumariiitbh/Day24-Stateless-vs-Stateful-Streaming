# Day 24 – Stateless vs Stateful Streaming (PySpark)

A small Spark Structured Streaming project that processes bank transactions two ways on the same input:

| | Stateless | Stateful |
|---|---|---|
| Question answered | "How many transactions per account **in this micro-batch**?" | "How many transactions per account **so far**?" |
| Remembers earlier batches? | No | Yes – in the **state store** (persisted via the checkpoint) |
| Code | `batch_counts()` inside `foreachBatch` | `running_counts()` = streaming `groupBy().agg(count)` in `complete` mode |
| Cost | No memory between batches | State grows with the number of keys (accounts); needs a checkpoint |

## Project layout

```
Day24-Stateless-vs-Stateful-Streaming/
├── data/batches/batch_1..3.json   # sample "stream": one file = one micro-batch
├── src/
│   ├── config.py                  # schema, thresholds, shuffle partitions
│   ├── transformations.py         # clean_transactions, batch_counts, running_counts
│   ├── streaming_jobs.py          # run_stateless / run_stateful queries
│   ├── comparison.py              # pure-Python comparison table + consistency check
│   ├── stream_simulator.py        # delivers sample files one by one
│   └── main.py                    # entry point
├── tests/                         # test_comparison.py (no Spark), test_streaming.py (Spark)
├── requirements.txt  pytest.ini  .gitignore
```

## Setup and run

Requirements: Python 3.9+, Java 11/17/21, and `pip install -r requirements.txt` (PySpark 3.5+; `trigger(availableNow=True)` needs Spark ≥ 3.3).

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m src.main        # run the demo from the project root
pytest -q                 # run the tests
```

## Sample data and expected result

Each file is one micro-batch (`maxFilesPerTrigger=1`). The data deliberately includes invalid records
(a negative amount in batch 1, a `null` account in batch 2) and a lower-case `withdrawal`, to show stateless cleaning.

Valid transactions per account (expected, computed independently from the raw files):

| batch | account | batch_count (stateless) | running_count (stateful) |
|---|---|---|---|
| 0 | ACC001 | 2 | 2 |
| 0 | ACC002 | 1 | 1 |
| 0 | ACC003 | 1 | 1 |
| 1 | ACC001 | 1 | 3 |
| 1 | ACC002 | 2 | 3 |
| 1 | ACC003 | 0 | 1 |
| 1 | ACC004 | 1 | 1 |
| 2 | ACC001 | 2 | 5 |
| 2 | ACC002 | 0 | 3 |
| 2 | ACC003 | 1 | 2 |
| 2 | ACC004 | 0 | 1 |
| 2 | ACC005 | 1 | 1 |

Notice ACC002 in batch 2: `batch_count = 0`, yet `running_count = 3`. That gap is exactly what state provides.
`main.py` prints this table and a self-check that every running count equals the sum of the stateless batch counts so far.

## Stateful vs stateless processing

**Stateless** operations (`filter`, `withColumn`, `select`, `map`) look at one row at a time. They need no memory
between batches, restart trivially, and scale linearly.

**Stateful** operations (streaming `groupBy().count()`, deduplication, joins, windows, `mapGroupsWithState`) must
remember data from earlier batches. Spark stores it in a versioned **state store** (in executor memory, snapshotted to the
checkpoint directory) so the query can recover after a failure.

Production considerations: set a **watermark** for windowed/dedup state so old state can be dropped (account IDs here are a bounded key set, so
this demo keeps state forever); monitor state size; and never change the stateful query's logic or
`spark.sql.shuffle.partitions` against an existing checkpoint.

## Spark concepts shown in the code

* **Transformations** – lazy, they only build the plan. *Narrow* (`filter`, `withColumn`, `select`): no data movement.
  *Wide* (`groupBy`): require a **shuffle**.
* **Actions** – start execution. In a streaming app the query is launched by `writeStream...start()`;
  inside `foreachBatch`, `show()` and `collect()` are ordinary batch actions, one job per micro-batch.
* **Partitions** – each batch DataFrame has its own partitions (printed in the stateless output). The stateful query's state
  is split into `spark.sql.shuffle.partitions` (= 2 here) state-store partitions, one per shuffle partition.
* **Shuffle** – `groupBy` on `account_id` repartitions rows by key so all rows of an account land in the same partition.
  In the stateful query this `Exchange` sits right before `StateStoreRestore/StateStoreSave`, so the shuffle happens on
  every micro-batch. The default of 200 shuffle partitions is far too many for a tiny local demo, and the state store inherits that number.
  Reduce the shuffle by pre-aggregating, filtering early (done in `clean_transactions`) and picking a sensible partition count.
* **Output modes** – `complete` re-emits the whole result table each batch (fine for a handful of accounts; use `update` for large key sets).
* **Triggers** – `availableNow=True` processes all currently available files in micro-batches and then stops, which makes the demo deterministic.

## Testing status

`tests/test_comparison.py` needs only Python. `tests/test_streaming.py` runs both queries end-to-end on the sample data and
asserts the table above; it is skipped automatically when PySpark is not installed.

## Publish to GitHub

```bash
git init && git add . && git commit -m "Day 24: Stateless vs Stateful Streaming (PySpark)"
git branch -M main
git remote add origin https://github.com/<your-user>/Day24-Stateless-vs-Stateful-Streaming.git
git push -u origin main
```
