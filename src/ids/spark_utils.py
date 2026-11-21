"""SparkSession factory and small DataFrame helpers."""
from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from pathlib import Path


def get_spark(app: str = "ids2018", master: str | None = None, driver_memory: str | None = None,
              shuffle_partitions: int | None = None, extra: dict | None = None):
    from pyspark.sql import SparkSession

    master = master or os.environ.get("SPARK_MASTER")
    builder = SparkSession.builder.appName(app)
    # Under spark-submit the JVM already knows --master; only default to local[*] for plain python.
    if master:
        builder = builder.master(master)
    elif not os.environ.get("SPARK_ENV_LOADED"):
        builder = builder.master("local[*]")
    builder = (builder
               # Spark 4 turns ANSI mode on by default; we want invalid strings -> NULL on cast.
               .config("spark.sql.ansi.enabled", "false")
               .config("spark.sql.adaptive.enabled", "true")
               # keep partition column `day` as the string '2018-02-14' (not a DateType)
               .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
               .config("spark.sql.parquet.compression.codec", "snappy")
               .config("spark.ui.showConsoleProgress", "false"))
    mem = driver_memory or os.environ.get("SPARK_DRIVER_MEMORY")
    if mem:
        builder = builder.config("spark.driver.memory", mem)
    if shuffle_partitions or os.environ.get("SPARK_SHUFFLE_PARTITIONS"):
        builder = builder.config("spark.sql.shuffle.partitions",
                                 str(shuffle_partitions or os.environ["SPARK_SHUFFLE_PARTITIONS"]))
    for k, v in (extra or {}).items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark


@contextmanager
def timer(store: dict, key: str):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        store[key] = round(time.perf_counter() - t0, 3)


def write_json(obj, path: str | Path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False, default=_json_default)
    print(f"[ids] wrote {path}")


def read_json(path: str | Path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _json_default(o):
    try:
        import numpy as np
        if isinstance(o, np.integer):
            return int(o)
        if isinstance(o, np.floating):
            return float(o)
        if isinstance(o, np.ndarray):
            return o.tolist()
    except ImportError:
        pass
    raise TypeError(f"not JSON serialisable: {type(o)}")


def log(msg: str):
    print(f"[ids {time.strftime('%H:%M:%S')}] {msg}", flush=True)
