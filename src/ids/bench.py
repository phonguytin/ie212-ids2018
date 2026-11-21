"""Scalability benchmark: Spark (local[k]) vs pandas / scikit-learn on *real* data subsets.

    python -m ids.bench prepare --input data --parquet work/parquet --out work/bench
    python -m ids.bench all --fracs 0.25,0.5,1.0 --cores 1,2,4,8 --timeout 3600
    python -m ids.bench run --engine spark --task etl --frac 0.5 --cores 4     # single run

Data sizes are real row samples (no synthetic duplication):
  * ETL input  : CSV files with a nested 25% / 50% line sample of every day (100% = raw files)
  * train input: nested 25% / 50% / 100% hash samples of the de-duplicated Parquet
Tasks
  etl   : read CSV -> cast -> drop header/NaN/Inf rows and id columns -> dedup -> Parquet
  train : Random Forest (20 trees, depth 10) fit on 80%, predict 20%
Every run is a separate subprocess (fresh JVM / interpreter) with wall-clock time and peak RSS
of the whole process tree; a run killed by the OS is reported as OOM, a slow one as TIMEOUT.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import random
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

from . import schema as S
from .spark_utils import log, read_json, write_json

RF_TREES, RF_DEPTH = 20, 10


# --------------------------------------------------------------------------------------
# prepare
# --------------------------------------------------------------------------------------

def prepare(input_dir: str, parquet_dir: str, out: str, fracs=(0.25, 0.5), seed: int = 7):
    out_p = Path(out)
    subs = [f for f in fracs if f < 1.0]
    for f in subs:
        (out_p / f"csv_{int(f * 100)}").mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    for path in sorted(glob.glob(os.path.join(input_dir, "*.csv"))):
        name = os.path.basename(path)
        log(f"sampling lines of {name}")
        outs = {f: open(out_p / f"csv_{int(f * 100)}" / name, "w", newline="") for f in subs}
        with open(path, newline="") as src:
            header = src.readline()
            for fh in outs.values():
                fh.write(header)
            for line in src:
                u = rng.random()
                for f, fh in outs.items():
                    if u < f:          # nested: the 25% sample is a subset of the 50% sample
                        fh.write(line)
        for fh in outs.values():
            fh.close()

    from pyspark.sql import functions as F
    from .spark_utils import get_spark
    spark = get_spark("ids-bench-prepare")
    meta = read_json(Path(parquet_dir) / "meta.json")
    df = spark.read.parquet(str(Path(parquet_dir) / "dedup"))
    u = (F.abs(F.xxhash64(F.col("rid"), F.lit(seed))) % 1000003) / 1000003.0
    for f in list(fracs) + ([1.0] if 1.0 not in fracs else []):
        log(f"train subset {f}")
        (df.filter(u < f).select(*meta["features"], "y")
           .coalesce(16).write.mode("overwrite").parquet(str(out_p / f"train_{int(f * 100)}")))
    write_json({"features": meta["features"], "fracs": list(fracs)}, out_p / "bench_meta.json")


def csv_dir(bench: str, data: str, frac: float) -> str:
    return data if frac >= 1.0 else str(Path(bench) / f"csv_{int(frac * 100)}")


# --------------------------------------------------------------------------------------
# single runs (executed in a child process)
# --------------------------------------------------------------------------------------

def spark_etl(src: str, dst: str, cores: int):
    from pyspark.sql import functions as F
    from .etl import parse, read_raw
    from .spark_utils import get_spark

    spark = get_spark("bench-spark-etl", f"local[{cores}]",
                      shuffle_partitions=max(8, cores * 4))
    raw, _ = read_raw(spark, src)
    df = parse(raw).filter(~F.col("is_header") & ~F.col("has_invalid"))
    df = df.select("day", "label", *S.CANDIDATE_FEATURES).dropDuplicates(S.CANDIDATE_FEATURES + ["label"])
    df.write.mode("overwrite").parquet(dst)
    n = spark.read.parquet(dst).count()
    spark.stop()
    return {"rows_out": n}


def pandas_etl(src: str, dst: str, cores: int):
    import numpy as np
    import pandas as pd

    frames = []
    for p in sorted(glob.glob(os.path.join(src, "*.csv"))):
        df = pd.read_csv(p, low_memory=False)
        df.columns = [S.canon(c) for c in df.columns]
        df = df[df["label"].astype(str).str.strip() != "Label"]
        for c in S.CANDIDATE_FEATURES:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df[["label"] + S.CANDIDATE_FEATURES]
        df = df.replace([np.inf, -np.inf], np.nan).dropna()
        df["day"] = S.day_from_filename(p)
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    del frames
    df = df.drop_duplicates(subset=S.CANDIDATE_FEATURES + ["label"])
    shutil.rmtree(dst, ignore_errors=True)
    os.makedirs(dst, exist_ok=True)
    df.to_parquet(os.path.join(dst, "part-0.parquet"), index=False)
    return {"rows_out": int(len(df))}


def spark_train(src: str, feats: list[str], cores: int):
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import RandomForestClassifier
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.ml.feature import VectorAssembler
    from pyspark.sql import functions as F
    from .spark_utils import get_spark

    spark = get_spark("bench-spark-train", f"local[{cores}]",
                      shuffle_partitions=max(8, cores * 4))
    df = spark.read.parquet(src).withColumn("_u", F.rand(1))
    train, test = df.filter("_u < 0.8").cache(), df.filter("_u >= 0.8").cache()
    n_train, n_test = train.count(), test.count()          # load outside the timed part
    t0 = time.perf_counter()
    model = Pipeline(stages=[
        VectorAssembler(inputCols=feats, outputCol="v"),
        RandomForestClassifier(featuresCol="v", labelCol="y", numTrees=RF_TREES,
                               maxDepth=RF_DEPTH, featureSubsetStrategy="sqrt", seed=1)]).fit(train)
    fit_s = time.perf_counter() - t0
    t0 = time.perf_counter()
    auc = BinaryClassificationEvaluator(labelCol="y", metricName="areaUnderPR") \
        .evaluate(model.transform(test))
    pred_s = time.perf_counter() - t0
    spark.stop()
    return {"rows_train": n_train, "rows_test": n_test, "fit_s": fit_s, "predict_s": pred_s,
            "pr_auc": auc}


def sklearn_train(src: str, feats: list[str], cores: int):
    import numpy as np
    import pandas as pd
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import average_precision_score

    df = pd.read_parquet(src)
    X = df[feats].to_numpy(dtype=np.float32)
    y = df["y"].to_numpy()
    del df
    m = np.random.default_rng(1).random(len(y)) < 0.8
    t0 = time.perf_counter()
    clf = RandomForestClassifier(n_estimators=RF_TREES, max_depth=RF_DEPTH, max_features="sqrt",
                                 n_jobs=cores, random_state=1).fit(X[m], y[m])
    fit_s = time.perf_counter() - t0
    t0 = time.perf_counter()
    p = clf.predict_proba(X[~m])[:, 1]
    pred_s = time.perf_counter() - t0
    return {"rows_train": int(m.sum()), "rows_test": int((~m).sum()), "fit_s": fit_s,
            "predict_s": pred_s, "pr_auc": float(average_precision_score(y[~m], p))}


def run_one(a) -> dict:
    t0 = time.perf_counter()
    if a.task == "etl":
        src = csv_dir(a.bench, a.data, a.frac)
        dst = str(Path(a.bench) / f"out_{a.engine}_{int(a.frac * 100)}")
        info = (spark_etl if a.engine == "spark" else pandas_etl)(src, dst, a.cores)
        info["input_bytes"] = sum(os.path.getsize(p) for p in glob.glob(os.path.join(src, "*.csv")))
    else:
        feats = read_json(Path(a.bench) / "bench_meta.json")["features"]
        src = str(Path(a.bench) / f"train_{int(a.frac * 100)}")
        info = (spark_train if a.engine == "spark" else sklearn_train)(src, feats, a.cores)
    info["wall_s"] = time.perf_counter() - t0
    return info


# --------------------------------------------------------------------------------------
# orchestration
# --------------------------------------------------------------------------------------

def _peak_rss(proc, stop: threading.Event, out: dict):
    import psutil
    try:
        p = psutil.Process(proc.pid)
    except psutil.Error:
        return
    peak = 0
    while not stop.is_set():
        try:
            rss = p.memory_info().rss + sum(c.memory_info().rss for c in p.children(recursive=True))
            peak = max(peak, rss)
        except psutil.Error:
            pass
        time.sleep(0.25)
    out["peak_rss_mb"] = round(peak / 2**20, 1)


def run_all(a):
    out_path = Path(a.out)
    res = read_json(out_path) if out_path.exists() and a.resume else {"runs": []}
    done = {(r["engine"], r["task"], r["frac"], r["cores"]) for r in res["runs"]}
    res["config"] = {"rf_trees": RF_TREES, "rf_depth": RF_DEPTH, "cpu_count": os.cpu_count(),
                     "timeout_s": a.timeout, "mem_limit_note": a.note}
    for task in a.tasks:
        for engine in a.engines:
            engine_name = engine if task == "etl" or engine == "spark" else "sklearn"
            cores_list = a.cores if (engine == "spark" or task == "train") else [1]  # pandas ETL is single-threaded
            for frac in a.fracs:
                for cores in cores_list:
                    key = (engine_name, task, frac, cores)
                    if key in done:
                        continue
                    log(f"bench {engine_name:7s} {task:5s} frac={frac:<4} cores={cores}")
                    cmd = [sys.executable, "-m", "ids.bench", "run", "--engine", engine,
                           "--task", task, "--frac", str(frac), "--cores", str(cores),
                           "--bench", a.bench, "--data", a.data]
                    t0 = time.perf_counter()
                    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                            text=True, env={**os.environ,
                                                            "SPARK_DRIVER_MEMORY": a.spark_mem})
                    stop, mem = threading.Event(), {}
                    th = threading.Thread(target=_peak_rss, args=(proc, stop, mem), daemon=True)
                    th.start()
                    status = "ok"
                    try:
                        so, se = proc.communicate(timeout=a.timeout)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        so, se = proc.communicate()
                        status = "timeout"
                    stop.set(); th.join()
                    row = {"engine": engine_name, "task": task, "frac": frac, "cores": cores,
                           "elapsed_s": round(time.perf_counter() - t0, 2), **mem}
                    if status == "ok" and proc.returncode == 0:
                        line = [l for l in so.strip().splitlines() if l.startswith("{")][-1]
                        row.update(json.loads(line))
                        row["status"] = "ok"
                    elif status == "timeout":
                        row["status"] = "timeout"
                    else:
                        oom = proc.returncode in (-9, 137) or "MemoryError" in se \
                            or "OutOfMemory" in se
                        row["status"] = "oom" if oom else "error"
                        row["error"] = se.strip().splitlines()[-3:] if se else proc.returncode
                    log(f"   -> {row['status']} {row.get('wall_s', row['elapsed_s']):.1f}s "
                        f"peak={row.get('peak_rss_mb')}MB")
                    res["runs"].append(row)
                    write_json(res, out_path)
    return res


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("cmd", choices=["prepare", "run", "all"])
    ap.add_argument("--data", default="data")
    ap.add_argument("--parquet", default="work/parquet")
    ap.add_argument("--bench", default="work/bench")
    ap.add_argument("--out", default="results/scalability.json")
    ap.add_argument("--engine", choices=["spark", "pandas"])
    ap.add_argument("--engines", default="spark,pandas")
    ap.add_argument("--task", choices=["etl", "train"])
    ap.add_argument("--tasks", default="etl,train")
    ap.add_argument("--frac", type=float)
    ap.add_argument("--fracs", default="0.25,0.5,1.0")
    ap.add_argument("--cores", default="4")
    ap.add_argument("--timeout", type=int, default=3600)
    ap.add_argument("--spark-mem", default=os.environ.get("SPARK_DRIVER_MEMORY", "8g"))
    ap.add_argument("--note", default="", help="free text, e.g. container memory limit")
    ap.add_argument("--resume", action="store_true")
    a = ap.parse_args()
    if a.cmd == "prepare":
        prepare(a.data, a.parquet, a.bench, tuple(float(x) for x in a.fracs.split(",")))
    elif a.cmd == "run":
        a.cores = int(a.cores)
        print(json.dumps(run_one(a)), flush=True)
    else:
        a.fracs = [float(x) for x in a.fracs.split(",")]
        a.cores = [int(x) for x in a.cores.split(",")]
        a.engines = a.engines.split(",")
        a.tasks = a.tasks.split(",")
        run_all(a)


if __name__ == "__main__":
    main()
