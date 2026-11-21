"""Kiểm tra train/serve skew: chấm điểm cùng một ngày dữ liệu theo hai đường.

  1. Parquet đã làm sạch (đường batch, giống lúc huấn luyện)
  2. CSV gốc, đọc dạng chuỗi rồi ép kiểu double (giống job streaming nhận từ Kafka)

Nếu tỷ lệ cảnh báo của hai đường gần như bằng nhau thì phần tiền xử lý nằm trong
PipelineModel đã đảm bảo batch và streaming dùng cùng một phép biến đổi.

    docker compose --profile batch run --rm batch python3 scripts/check_skew.py
    docker compose --profile batch run --rm batch python3 scripts/check_skew.py --day 2018-02-14 --threshold 0.314
"""
import argparse
import glob

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from ids import schema as S
from ids.spark_utils import get_spark, read_json


def alert_rate(model, df, thr, name):
    p = vector_to_array(F.col("bin_prob"))[1]
    rows = (model.transform(df).select("label", p.alias("p"))
            .groupBy("label")
            .agg(F.count("*").alias("n"),
                 F.avg((F.col("p") >= thr).cast("double")).alias("alert_rate"),
                 F.avg("p").alias("mean_p"))
            .orderBy("label").collect())
    print(f"\n== {name} (ngưỡng {thr})")
    for r in rows:
        print(f"  {r['label']:<28} n={r['n']:>9,}  tỷ lệ cảnh báo={r['alert_rate']:.4f}  p trung bình={r['mean_p']:.4f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", default="2018-02-14")
    ap.add_argument("--threshold", type=float, default=None, help="mặc định: ngưỡng trong models/deploy.json")
    ap.add_argument("--models", default="models")
    ap.add_argument("--parquet", default="work/parquet/clean")
    ap.add_argument("--data", default="data")
    a = ap.parse_args()

    spark = get_spark("check-skew", "local[4]")
    deploy = read_json(f"{a.models}/deploy.json")
    thr = a.threshold if a.threshold is not None else deploy.get("default_threshold", 0.5)
    model = PipelineModel.load(f"{a.models}/{deploy.get('binary_model', 'binary')}")

    alert_rate(model, spark.read.parquet(a.parquet).filter(F.col("day") == a.day), thr, f"Parquet ngày {a.day}")

    csv = [p for p in sorted(glob.glob(f"{a.data}/*.csv")) if S.day_from_filename(p) == a.day]
    if not csv:
        raise SystemExit(f"không tìm thấy CSV của ngày {a.day} trong {a.data}/")
    raw = spark.read.option("header", True).csv(csv[0])
    raw = raw.toDF(*[S.canon(c) for c in raw.columns]).filter(F.col("label") != "Label")
    raw = raw.select("label", *[F.col(c).cast("double").alias(c) for c in deploy["features"]])
    alert_rate(model, raw, thr, f"CSV gốc ngày {a.day} (chuỗi -> double, giống streaming)")


if __name__ == "__main__":
    main()
