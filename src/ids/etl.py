"""Raw CSV -> cleaned, de-duplicated Parquet + data-quality report.

    python -m ids.etl --input data --output work/parquet --report results/quality_report.json

Steps
 1. read the 10 CSVs as strings, unify schema (20-02 has 4 extra id columns), tag `day`
 2. flag repeated header rows, cast to double, flag NaN / +-Infinity / unparsable values
 3. write staging parquet (one CSV pass), then compute the quality report from it
 4. clean  = valid rows without identifier columns                -> <output>/clean/day=...
 5. dedup  = unique (features, label) groups with dup_count, min(day) -> <output>/dedup/day=...
 6. meta.json: final feature list (identifier + constant columns removed), label counts
"""
from __future__ import annotations

import argparse
import glob
import os
import shutil
from functools import reduce
from pathlib import Path

from . import schema as S
from .spark_utils import get_spark, log, timer, write_json


def read_raw(spark, input_dir: str, files: list[str] | None = None):
    from pyspark.sql import functions as F

    paths = files or sorted(glob.glob(os.path.join(input_dir, "*.csv")))
    if not paths:
        raise FileNotFoundError(f"no CSV under {input_dir}")
    frames, per_file = [], []
    for p in paths:
        df = spark.read.option("header", True).option("inferSchema", False).csv(p)
        df = df.toDF(*[S.canon(c) for c in df.columns])
        per_file.append({"file": os.path.basename(p), "day": S.day_from_filename(p),
                         "n_columns": len(df.columns),
                         "extra_columns": sorted(set(df.columns) - set(S.NUMERIC_COLUMNS)
                                                 - {"timestamp", "label"}),
                         "bytes": os.path.getsize(p)})
        frames.append(df.withColumn("day", F.lit(S.day_from_filename(p)))
                        .withColumn("source_file", F.lit(os.path.basename(p))))
    df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)
    return df, per_file


def parse(df):
    """Cast to double and add validity flags. Header rows are kept but flagged."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType

    label = F.trim(F.col("label"))
    is_header = label == F.lit("Label")
    cols = [F.col("day"), F.col("source_file"), label.alias("label"),
            is_header.alias("is_header")]
    bad_any = F.lit(False)
    for c in S.NUMERIC_COLUMNS:
        v = F.col(c).cast("double")
        cols.append(v.alias(c))
        bad = (v.isNull() | F.isnan(v) | (F.abs(v) == float("inf")))
        bad_any = bad_any | bad
        # keep raw string only to detect "string present but not a number"
        cols.append((F.col(c).isNotNull() & v.isNull()).alias(f"_unparsable_{c}"))
    out = df.select(*cols, bad_any.alias("has_invalid"))
    return (out.withColumn("family", family_col(F.col("label")))
               .withColumn("y", (F.col("family") != "Benign").cast(IntegerType())))


def family_col(label):
    """Spark-SQL twin of schema.label_family (no Python UDF on 16M rows)."""
    from pyspark.sql import functions as F

    s = F.lower(F.trim(F.coalesce(label, F.lit(""))))
    return (F.when(s.isin("", "benign"), "Benign")
             .when(s.contains("ddos"), "DDoS")
             .when(s.startswith("dos"), "DoS")
             .when(s.contains("xss") | s.contains("sql") | s.contains("web"), "WebAttack")
             .when(s.contains("brute") | s.contains("ftp") | s.contains("ssh"), "BruteForce")
             .when(s.startswith("bot"), "Bot")
             .when(s.startswith("infil"), "Infiltration")
             .otherwise("Other"))


def quality_report(staging, per_file: list[dict]) -> dict:
    from pyspark.sql import functions as F

    rep: dict = {"files": per_file}
    log("quality: per-file counts")
    by_file = (staging.groupBy("source_file", "day")
               .agg(F.count("*").alias("rows"),
                    F.sum(F.col("is_header").cast("long")).alias("header_rows"),
                    F.sum((~F.col("is_header") & F.col("has_invalid")).cast("long"))
                     .alias("invalid_rows"))
               .orderBy("day").collect())
    rep["per_file_counts"] = [r.asDict() for r in by_file]

    data = staging.filter(~F.col("is_header"))
    log("quality: per-column NaN / Inf / unparsable / negative / min / max")
    aggs = []
    for c in S.NUMERIC_COLUMNS:
        v = F.col(c)
        aggs += [
            F.sum(F.isnan(v).cast("long")).alias(f"{c}__nan"),
            F.sum((v == float("inf")).cast("long")).alias(f"{c}__posinf"),
            F.sum((v == float("-inf")).cast("long")).alias(f"{c}__neginf"),
            F.sum(F.col(f"_unparsable_{c}").cast("long")).alias(f"{c}__unparsable"),
            F.sum(v.isNull().cast("long")).alias(f"{c}__null"),
        ]
    row = data.agg(*aggs).collect()[0].asDict()

    valid = data.filter(~F.col("has_invalid"))
    aggs2 = []
    for c in S.NUMERIC_COLUMNS:
        v = F.col(c)
        aggs2 += [F.sum((v < 0).cast("long")).alias(f"{c}__negative"),
                  F.min(v).alias(f"{c}__min"), F.max(v).alias(f"{c}__max")]
    row2 = valid.agg(*aggs2).collect()[0].asDict()

    columns = {}
    for c in S.NUMERIC_COLUMNS:
        d = {k: row[f"{c}__{k}"] for k in ("nan", "posinf", "neginf", "unparsable", "null")}
        d.update({k: row2[f"{c}__{k}"] for k in ("negative", "min", "max")})
        d["constant"] = d["min"] == d["max"]
        columns[c] = d
    rep["columns"] = columns
    rep["constant_columns"] = [c for c, d in columns.items() if d["constant"]]
    rep["columns_with_invalid"] = {c: d["nan"] + d["posinf"] + d["neginf"] + d["null"]
                                   for c, d in columns.items()
                                   if d["nan"] + d["posinf"] + d["neginf"] + d["null"] > 0}
    rep["columns_with_negative"] = {c: d["negative"] for c, d in columns.items()
                                    if d["negative"]}
    return rep


def run(input_dir: str, output_dir: str, report_path: str | None, master: str | None = None,
        keep_staging: bool = False, files: list[str] | None = None, skip_report: bool = False):
    from pyspark.sql import functions as F

    spark = get_spark("ids-etl", master)
    out = Path(output_dir)
    times: dict = {}

    raw, per_file = read_raw(spark, input_dir, files)
    log(f"reading {len(per_file)} CSV files from {input_dir}")
    staging_path = str(out / "_staging")
    with timer(times, "csv_to_staging_s"):
        parse(raw).write.mode("overwrite").parquet(staging_path)
    staging = spark.read.parquet(staging_path)

    rep: dict = {}
    if not skip_report:
        with timer(times, "quality_report_s"):
            rep = quality_report(staging, per_file)
    constant = rep.get("constant_columns", [])
    features = [c for c in S.CANDIDATE_FEATURES if c not in constant]

    keep = ["day", "label", "family", "y"] + S.CANDIDATE_FEATURES
    # `rid`: stable unique row id, so random splits are reproducible across scans
    clean = (staging.filter(~F.col("is_header") & ~F.col("has_invalid")).select(*keep)
             .withColumn("rid", F.monotonically_increasing_id()))
    with timer(times, "write_clean_s"):
        clean.write.mode("overwrite").partitionBy("day").parquet(str(out / "clean"))
    clean = spark.read.parquet(str(out / "clean"))

    # Duplicates are defined on everything the model can see (+ label): identical rows are
    # indistinguishable to any classifier, so they inflate random-split scores.
    key = S.CANDIDATE_FEATURES + ["label"]
    dedup = (clean.groupBy(*key)
             .agg(F.min("day").alias("day"), F.count("*").alias("dup_count"),
                  F.first("family").alias("family"), F.first("y").alias("y"),
                  F.min("rid").alias("rid")))
    with timer(times, "write_dedup_s"):
        dedup.write.mode("overwrite").partitionBy("day").parquet(str(out / "dedup"))
    dedup = spark.read.parquet(str(out / "dedup"))

    log("counting labels before / after de-duplication")
    before = {r["label"]: r["count"] for r in clean.groupBy("label").count().collect()}
    after = {r["label"]: r["count"] for r in dedup.groupBy("label").count().collect()}
    day_before = {(r["day"], r["label"]): r["count"]
                  for r in clean.groupBy("day", "label").count().collect()}
    # same feature vector seen with >1 label: irreducible label noise
    conflicts = (dedup.groupBy(*S.CANDIDATE_FEATURES)
                 .agg(F.countDistinct("label").alias("k"), F.sum("dup_count").alias("rows"))
                 .filter("k > 1").agg(F.count("*").alias("groups"),
                                      F.sum("rows").alias("rows")).collect()[0])

    n_before, n_after = sum(before.values()), sum(after.values())
    rep["duplicates"] = {
        "rows_clean": n_before, "rows_dedup": n_after,
        "duplicate_rows_removed": n_before - n_after,
        "duplicate_ratio": round((n_before - n_after) / max(n_before, 1), 6),
        "by_label": {lab: {"before": before.get(lab, 0), "after": after.get(lab, 0),
                           "removed_ratio": round(1 - after.get(lab, 0) / before[lab], 6)}
                     for lab in sorted(before)},
        "conflicting_label_groups": conflicts["groups"] or 0,
        "conflicting_label_rows": conflicts["rows"] or 0,
    }
    rep["label_by_day"] = {}
    for (day, lab), n in sorted(day_before.items()):
        rep["label_by_day"].setdefault(day, {})[lab] = n
    rep["features"] = features
    rep["dropped_identifier_columns"] = S.ID_COLUMNS
    rep["timings"] = times

    meta = {"features": features, "constant_columns": constant,
            "id_columns": S.ID_COLUMNS, "families": S.FAMILIES,
            "days": sorted(rep["label_by_day"]), "rows_clean": n_before, "rows_dedup": n_after}
    write_json(meta, out / "meta.json")
    if report_path:
        write_json(rep, report_path)
    if not keep_staging:
        shutil.rmtree(staging_path, ignore_errors=True)
    log(f"done: {n_before:,} clean rows, {n_after:,} after dedup, timings={times}")
    return rep


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--input", default="data")
    ap.add_argument("--output", default="work/parquet")
    ap.add_argument("--report", default="results/quality_report.json")
    ap.add_argument("--master", default=None)
    ap.add_argument("--keep-staging", action="store_true")
    a = ap.parse_args()
    run(a.input, a.output, a.report, a.master, a.keep_staging)


if __name__ == "__main__":
    main()
