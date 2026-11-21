"""Speed layer: Kafka -> Spark Structured Streaming -> MongoDB.

    spark-submit --master spark://spark-master:7077 /opt/ids/src/ids/stream_scorer.py

Message on topic `flows` (JSON, produced by the web app's replayer / upload endpoint):
    {"uid": "...", "src": "replay"|"upload", "job": "<upload job id>"|null, "row": 17,
     "day": "2018-02-14", "ts": <epoch ms when sent>, "label": "SSH-Bruteforce"|null,
     "f": {"flow_duration": "1234", "flow_byts_s": "Infinity", ...}}   # canonical names

Every micro-batch:
  1. reads the live config (threshold, c_FP, c_FN) from MongoDB -> threshold changes from the
     web page apply on the next batch, no restart
  2. scores with the saved binary PipelineModel (P(attack)) and the multiclass RF (attack type)
  3. writes alerts (p >= threshold)               -> ids.alerts          (from the executors)
     writes every scored row of uploaded files    -> ids.upload_results  (from the executors)
     upserts 10-second window counters per source -> ids.metrics  (n, alerts, TP/FP/FN/TN, ...)
"""
from __future__ import annotations

import os
import time
from pathlib import Path

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "ids")
WINDOW = os.environ.get("METRIC_WINDOW", "10 seconds")


def mongo_db():
    from pymongo import MongoClient
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)[MONGO_DB]


DEFAULT_CONFIG = {"_id": "runtime", "threshold": 0.5, "c_fp": 1.0, "c_fn": 50.0}


def ensure_indexes(db):
    from pymongo import ASCENDING, DESCENDING
    db.alerts.create_index([("scored_at", DESCENDING)])
    db.alerts.create_index([("attack_type", ASCENDING), ("scored_at", DESCENDING)])
    db.alerts.create_index([("src", ASCENDING), ("job", ASCENDING)])
    db.alerts.create_index("uid")
    db.metrics.create_index([("window_start", ASCENDING), ("src", ASCENDING)], unique=True)
    db.upload_results.create_index([("job", ASCENDING), ("row", ASCENDING)])


# --- executor-side writers ------------------------------------------------------------

def _write_alerts(rows):
    batch = []
    db = None
    for r in rows:
        d = r.asDict(recursive=True)
        d["features"] = d.pop("f") or {}
        batch.append(d)
        if len(batch) >= 1000:
            db = mongo_db() if db is None else db
            db.alerts.insert_many(batch, ordered=False)
            batch = []
    if batch:
        db = mongo_db() if db is None else db
        db.alerts.insert_many(batch, ordered=False)


def _write_upload_results(rows):
    batch, db = [], None
    for r in rows:
        batch.append(r.asDict())
        if len(batch) >= 2000:
            db = mongo_db() if db is None else db
            db.upload_results.insert_many(batch, ordered=False)
            batch = []
    if batch:
        db = mongo_db() if db is None else db
        db.upload_results.insert_many(batch, ordered=False)


# --- driver ---------------------------------------------------------------------------

def build(spark, models_dir: str):
    from pyspark.ml import PipelineModel
    from .spark_utils import read_json

    deploy = read_json(Path(models_dir) / "deploy.json")
    bin_model = PipelineModel.load(str(Path(models_dir) / deploy.get("binary_model", "binary")))
    mc_model = None
    if deploy.get("multiclass_model") and (Path(models_dir) / deploy["multiclass_model"]).exists():
        mc_model = PipelineModel.load(str(Path(models_dir) / deploy["multiclass_model"]))
    return deploy, bin_model, mc_model


def make_processor(deploy, bin_model, mc_model):
    from pyspark.ml.functions import vector_to_array
    from pyspark.sql import functions as F

    families = deploy.get("families")
    fam_arr = F.array(*[F.lit(f) for f in families])
    K = len(families)

    def process(batch_df, batch_id):
        t_start = time.time()
        db = mongo_db()
        cfg = db.config.find_one({"_id": "runtime"}) or DEFAULT_CONFIG
        thr = float(cfg.get("threshold", 0.5))

        scored = bin_model.transform(batch_df)
        p = vector_to_array(F.col("bin_prob"))[1]
        if mc_model is not None:
            scored = mc_model.transform(scored)
            # attack type = most probable *attack* family (ignore the Benign column)
            sl = F.slice(vector_to_array(F.col("mc_prob")), 2, K - 1)
            atype = F.element_at(fam_arr, F.array_position(sl, F.array_max(sl)).cast("int") + 1)
        else:
            atype = F.lit("Attack")
        lab = F.lower(F.trim(F.col("label")))
        truth = F.when(F.col("label").isNull() | (lab == ""), F.lit(None)) \
                 .otherwise(lab != "benign")
        now_ms = int(time.time() * 1000)
        out = (scored.select(
                    "uid", "src", "job", "row", "day", "ts", "label", "kafka_ts", "f",
                    p.alias("p_attack"), atype.alias("pred_type"))
               .withColumn("alert", F.col("p_attack") >= F.lit(thr))
               .withColumn("y_true", truth)
               .withColumn("threshold", F.lit(thr))
               .withColumn("scored_at", F.lit(now_ms))
               .persist())

        # 1) alerts
        (out.filter("alert")
            .select("uid", "src", "job", "row", "day", "ts", "label", "p_attack", "threshold",
                    "scored_at", F.col("pred_type").alias("attack_type"), "y_true", "f")
            .foreachPartition(_write_alerts))

        # 2) every row of uploaded files (so the upload page can show full results)
        (out.filter(F.col("src") == "upload")
            .select("job", "row", "uid", "p_attack", "alert", "label",
                    F.when(F.col("alert"), F.col("pred_type")).alias("attack_type"), "scored_at")
            .foreachPartition(_write_upload_results))

        # 3) windowed counters (driver side, tiny)
        w = F.window("kafka_ts", WINDOW)
        a, y = F.col("alert"), F.col("y_true")
        agg = (out.groupBy(w.alias("w"), "src")
               .agg(F.count("*").alias("n"),
                    F.sum(a.cast("long")).alias("alerts"),
                    F.sum((a & y).cast("long")).alias("tp"),
                    F.sum((a & ~y).cast("long")).alias("fp"),
                    F.sum((~a & y).cast("long")).alias("fn"),
                    F.sum((~a & ~y).cast("long")).alias("tn"),
                    F.sum(y.isNotNull().cast("long")).alias("labeled"),
                    F.sum(F.lit(now_ms) - F.col("ts")).alias("latency_ms_sum"))
               .collect())
        by_type = (out.filter("alert").groupBy(w.alias("w"), "src", "pred_type").count().collect())
        by_true = (out.filter(y).groupBy(w.alias("w"), "src", "label").count().collect())
        from pymongo import UpdateOne
        ops = {}

        def op(row):
            key = (row["w"]["start"], row["src"])
            return ops.setdefault(key, {"$inc": {}, "$set": {"window_end": row["w"]["end"]}})

        for r in agg:
            o = op(r)
            for k in ("n", "alerts", "tp", "fp", "fn", "tn", "labeled", "latency_ms_sum"):
                o["$inc"][k] = int(r[k] or 0)
        for r in by_type:
            op(r)["$inc"][f"by_type.{r['pred_type']}"] = int(r["count"])
        for r in by_true:
            op(r)["$inc"][f"by_label.{r['label'].replace('.', '_')}"] = int(r["count"])
        if ops:
            db.metrics.bulk_write([UpdateOne({"window_start": k[0], "src": k[1]}, v, upsert=True)
                                   for k, v in ops.items()], ordered=False)

        # 4) upload job progress
        jobs = (out.filter(F.col("src") == "upload").groupBy("job")
                .agg(F.count("*").alias("n"), F.sum(a.cast("long")).alias("alerts")).collect())
        for j in jobs:
            db.upload_jobs.update_one({"_id": j["job"]},
                                      {"$inc": {"scored": int(j["n"]), "alerts": int(j["alerts"])},
                                       "$set": {"updated_at": now_ms}})
        n = sum(int(r["n"]) for r in agg)
        db.scorer_status.update_one({"_id": "scorer"}, {"$set": {
            "last_batch": batch_id, "last_batch_rows": n, "last_batch_at": now_ms,
            "last_batch_s": round(time.time() - t_start, 3), "threshold": thr,
            "model": deploy.get("binary_algorithm_name")},
            "$inc": {"total_rows": n}}, upsert=True)
        out.unpersist()
        if n:
            print(f"[scorer] batch {batch_id}: {n} rows, thr={thr:.3f}, "
                  f"{time.time() - t_start:.2f}s", flush=True)

    return process


def main():
    from pyspark.sql import functions as F
    from pyspark.sql.types import LongType, MapType, StringType, StructField, StructType
    from .spark_utils import get_spark

    models_dir = os.environ.get("MODELS_DIR", "/opt/ids/models")
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
    topic = os.environ.get("KAFKA_TOPIC", "flows")
    ckpt = os.environ.get("CHECKPOINT_DIR", "/opt/ids/work/checkpoints/scorer")

    spark = get_spark("ids-stream-scorer", os.environ.get("SPARK_MASTER"),
                      shuffle_partitions=int(os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8")))
    deploy, bin_model, mc_model = build(spark, models_dir)
    db = mongo_db()
    ensure_indexes(db)
    if not db.config.find_one({"_id": "runtime"}):
        cfg = dict(DEFAULT_CONFIG)
        cfg["threshold"] = deploy.get("default_threshold", 0.5)
        cfg["c_fn"] = float(deploy.get("default_ratio", 50))
        db.config.insert_one(cfg)

    schema = StructType([
        StructField("uid", StringType()), StructField("src", StringType()),
        StructField("job", StringType()), StructField("row", LongType()),
        StructField("day", StringType()), StructField("ts", LongType()),
        StructField("label", StringType()),
        StructField("f", MapType(StringType(), StringType()))])
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topic)
           .option("startingOffsets", os.environ.get("STARTING_OFFSETS", "latest"))
           .option("maxOffsetsPerTrigger", os.environ.get("MAX_OFFSETS_PER_TRIGGER", "20000"))
           .option("failOnDataLoss", "false")
           .load())
    msgs = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("m"),
                       F.col("timestamp").alias("kafka_ts"))
            .select("m.*", "kafka_ts")
            .filter(F.col("f").isNotNull()))
    feats = msgs.select("*", *[F.col("f")[c].cast("double").alias(c) for c in deploy["features"]])

    q = (feats.writeStream.foreachBatch(make_processor(deploy, bin_model, mc_model))
         .option("checkpointLocation", ckpt)
         .trigger(processingTime=os.environ.get("TRIGGER", "2 seconds"))
         .start())
    print(f"[scorer] running: model={deploy.get('binary_algorithm_name')} "
          f"multiclass={'yes' if mc_model else 'no'} topic={topic}", flush=True)
    q.awaitTermination()


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from ids.stream_scorer import main as _main   # run as package so executors can unpickle
    _main()
