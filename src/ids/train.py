"""Training + evaluation scenarios (Spark MLlib).

    python -m ids.train scenario1   # stratified random split, raw (clean) vs de-duplicated
    python -m ids.train scenario2   # leave-one-day-out on de-duplicated data
    python -m ids.train multiclass  # Random Forest over 7 attack families
    python -m ids.train all

Common options: --parquet work/parquet --results results --models-out models
                --models lr,dt,rf,gbt --preset full|fast --train-frac 1.0 --seed 42

scenario1 also selects the binary model with the lowest *validation* cost (ratio 1:50 by
default) and saves it, with its validation score histogram, to <models-out>/binary + deploy.json
for the streaming scorer.
"""
from __future__ import annotations

import argparse
import shutil
import time
from pathlib import Path

from . import evaluate as E
from . import schema as S
from .features import feature_stages
from .spark_utils import get_spark, log, read_json, write_json

MODEL_NAMES = {"lr": "Logistic Regression", "dt": "Decision Tree",
               "rf": "Random Forest", "gbt": "Gradient-Boosted Trees"}

PRESETS = {
    "full": {"lr": dict(maxIter=100, regParam=1e-4),
             "dt": dict(maxDepth=14, maxBins=64, minInstancesPerNode=5),
             "rf": dict(numTrees=60, maxDepth=14, maxBins=64, subsamplingRate=0.7,
                        featureSubsetStrategy="sqrt"),
             "gbt": dict(maxIter=60, maxDepth=6, maxBins=64, stepSize=0.1)},
    "fast": {"lr": dict(maxIter=40, regParam=1e-4),
             "dt": dict(maxDepth=10, maxBins=32),
             "rf": dict(numTrees=20, maxDepth=10, maxBins=32, subsamplingRate=0.5,
                        featureSubsetStrategy="sqrt"),
             "gbt": dict(maxIter=20, maxDepth=5, maxBins=32)},
}


# --------------------------------------------------------------------------------------
# data helpers
# --------------------------------------------------------------------------------------

def load(spark, parquet_dir: str, variant: str):
    return spark.read.parquet(str(Path(parquet_dir) / variant))


def random_split(df, seed: int, val: float = 0.15, test: float = 0.15):
    """Each row gets a pseudo-uniform U(0,1) from hash(rid, seed); identical cut points for
    every label => stratified by label. Hash-based (not rand()) so the three filters below see
    exactly the same assignment however often Spark re-scans the data."""
    from pyspark.sql import functions as F

    r = (F.abs(F.xxhash64(F.col("rid"), F.lit(seed))) % 1000003) / 1000003.0
    df = df.withColumn("_split", F.when(r < test, "test")
                       .when(r < test + val, "val").otherwise("train"))
    return [df.filter(F.col("_split") == s).drop("_split") for s in ("train", "val", "test")]


def subsample(train, frac: float, seed: int, min_keep: int = 20000):
    """Stratified down-sampling of the training split; rare labels are kept whole."""
    from pyspark.sql import functions as F

    if frac >= 1.0:
        return train
    counts = {r["label"]: r["count"] for r in train.groupBy("label").count().collect()}
    keep_all = [lab for lab, n in counts.items() if n * frac < min_keep]
    u = (F.abs(F.xxhash64(F.col("rid"), F.lit(seed + 7))) % 1000003) / 1000003.0
    return train.filter(F.col("label").isin(keep_all) | (u < frac))


def add_weights(train, label_col: str = "y", mode: str = "balanced", cap: float = 100.0):
    """weight_c = N / (K * N_c)  ('balanced') or its square root ('sqrt'), capped."""
    from pyspark.sql import functions as F

    counts = {r[label_col]: r["count"] for r in train.groupBy(label_col).count().collect()}
    n, k = sum(counts.values()), len(counts)
    w = {}
    for c, nc in counts.items():
        v = n / (k * nc)
        w[c] = min(v ** 0.5 if mode == "sqrt" else v, cap)
    expr = None
    for c, v in w.items():
        expr = (F.when(F.col(label_col) == c, v) if expr is None
                else expr.when(F.col(label_col) == c, v))
    return train.withColumn("w", expr.otherwise(1.0)), {str(c): v for c, v in w.items()}


# --------------------------------------------------------------------------------------
# model builders
# --------------------------------------------------------------------------------------

def binary_pipeline(name: str, features: list[str], preset: str = "full", weighted: bool = True):
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import (DecisionTreeClassifier, GBTClassifier,
                                           LogisticRegression, RandomForestClassifier)

    stages, vec = feature_stages(features, "b_", scale=(name == "lr"))
    common = dict(featuresCol=vec, labelCol="y", predictionCol="bin_pred")
    if weighted:
        common["weightCol"] = "w"
    p = dict(PRESETS[preset][name])
    cls = {"lr": LogisticRegression, "dt": DecisionTreeClassifier,
           "rf": RandomForestClassifier, "gbt": GBTClassifier}[name]
    if name in ("rf", "gbt", "dt"):
        p["seed"] = 42
    clf = cls(**common, **p)
    # GBTClassifier's Python constructor lacks these kwargs; the setters exist on all four.
    clf.setProbabilityCol("bin_prob").setRawPredictionCol("bin_raw")
    return Pipeline(stages=stages + [clf])


def multiclass_pipeline(features: list[str], preset: str = "full"):
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import RandomForestClassifier

    stages, vec = feature_stages(features, "m_", scale=False)
    clf = RandomForestClassifier(featuresCol=vec, labelCol="fam_idx", weightCol="w",
                                 probabilityCol="mc_prob", rawPredictionCol="mc_raw",
                                 predictionCol="mc_pred", seed=42, **PRESETS[preset]["rf"])
    return Pipeline(stages=stages + [clf])


def fit_eval(name, features, train, val, test, preset, save_to: str | None = None) -> dict:
    t0 = time.perf_counter()
    model = binary_pipeline(name, features, preset).fit(train)
    fit_s = time.perf_counter() - t0
    t0 = time.perf_counter()
    vh = E.score_histograms(model.transform(val))
    th = E.score_histograms(model.transform(test))
    pred_s = time.perf_counter() - t0
    rep = E.summarize(vh, th)
    rep.update({"model": name, "model_name": MODEL_NAMES[name],
                "fit_s": round(fit_s, 2), "predict_s": round(pred_s, 2)})
    o = rep["optimal"][str(E.DEFAULT_RATIO)]
    log(f"  {name:4s} fit={fit_s:7.1f}s PR-AUC={rep['test_pr_auc']:.4f} "
        f"F1@0.5={rep['at_0.5']['f1']:.4f} | 1:{E.DEFAULT_RATIO} thr={o['threshold']:.3f} "
        f"cost={o['cost'][str(E.DEFAULT_RATIO)]:,} (0.5 -> {o['test_cost_at_0.5']:,})")
    if save_to:
        model.write().overwrite().save(save_to)
        rep["saved_to"] = save_to
    return rep


def split_sizes(**dfs) -> dict:
    return {k: v.count() for k, v in dfs.items()}


# --------------------------------------------------------------------------------------
# scenarios
# --------------------------------------------------------------------------------------

def scenario1(spark, a) -> dict:
    """Random stratified split on clean (with duplicates) vs dedup data."""
    from pyspark.sql import functions as F

    meta = read_json(Path(a.parquet) / "meta.json")
    feats = meta["features"]
    res = {"scenario": "random_split", "seed": a.seed, "preset": a.preset,
           "train_frac": a.train_frac, "features": feats, "variants": {}}
    work_models = Path(a.work) / "models" / "scenario1"
    for variant in ("clean", "dedup"):
        log(f"scenario 1 / {variant}")
        df = load(spark, a.parquet, variant)
        train, val, test = random_split(df, a.seed)
        train = subsample(train, a.train_frac, a.seed)
        train, weights = add_weights(train)
        train = train.cache()
        sizes = split_sizes(train=train, val=val, test=test)
        # how many test rows have an identical (features+label) row in train -> leakage
        h = F.xxhash64(*[F.col(c) for c in S.CANDIDATE_FEATURES + ["label"]]).alias("h")
        tr_h = train.select(h).distinct()
        overlap = test.select(h).join(tr_h, "h", "left_semi").count()
        v = {"sizes": sizes, "class_weights": weights,
             "test_rows_seen_in_train": overlap,
             "test_rows_seen_in_train_ratio": overlap / max(sizes["test"], 1), "models": {}}
        log(f"  sizes={sizes} test rows duplicated in train: {overlap:,}")
        for m in a.models:
            save = str(work_models / f"{variant}_{m}") if variant == "dedup" else None
            v["models"][m] = fit_eval(m, feats, train, val, test, a.preset, save)
        train.unpersist()
        res["variants"][variant] = v
        write_json(res, Path(a.results) / "scenario1.json")   # partial results survive crashes

    deploy_best(res, feats, a)
    return res


def deploy_best(res: dict, feats: list[str], a):
    ratio = str(E.DEFAULT_RATIO)
    models = res["variants"]["dedup"]["models"]
    best = min(models, key=lambda m: models[m]["optimal"][ratio]["val_cost"])
    out = Path(a.models_out)
    dst = out / "binary"
    shutil.rmtree(dst, ignore_errors=True)
    shutil.copytree(models[best]["saved_to"], dst)
    vc = models[best]["curves"]["val"]
    deploy = {
        "binary_model": "binary", "binary_algorithm": best,
        "binary_algorithm_name": MODEL_NAMES[best],
        "selected_by": f"min validation cost at c_FP:c_FN = 1:{ratio} (scenario 1, dedup)",
        "features": feats, "bins": E.B, "families": S.FAMILIES,
        "val_hist": vc,
        "thresholds": {r: models[best]["optimal"][r]["threshold"] for r in models[best]["optimal"]},
        "default_ratio": E.DEFAULT_RATIO,
        "default_threshold": models[best]["optimal"][ratio]["threshold"],
        "test_metrics": {"pr_auc": models[best]["test_pr_auc"],
                         "at_opt": models[best]["optimal"][ratio]},
        "created": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    prev = out / "deploy.json"
    if prev.exists():
        old = read_json(prev)
        deploy["multiclass_model"] = old.get("multiclass_model")
    write_json(deploy, prev)
    log(f"deployed {best} -> {dst} (threshold {deploy['default_threshold']})")


def scenario2(spark, a) -> dict:
    """Leave-one-day-out: train on 9 days (85/15 train/val), test on the held-out day."""
    meta = read_json(Path(a.parquet) / "meta.json")
    feats = meta["features"]
    df = load(spark, a.parquet, "dedup").cache()
    days = a.days or meta["days"]
    out_path = Path(a.results) / "scenario2.json"
    res = read_json(out_path) if (a.resume and out_path.exists()) else \
        {"scenario": "leave_one_day_out", "seed": a.seed, "preset": a.preset,
         "train_frac": a.train_frac, "folds": {}}
    for day in days:
        if day in res["folds"] and all(m in res["folds"][day]["models"] for m in a.models):
            log(f"scenario 2 / {day}: already done, skipping")
            continue
        log(f"scenario 2 / held-out day {day}")
        rest, test = df.filter(df.day != day), df.filter(df.day == day)
        train, val, _ = random_split(rest, a.seed, val=0.15, test=0.0)
        train = subsample(train, a.train_frac, a.seed)
        train, weights = add_weights(train)
        train = train.cache()
        fold = res["folds"].get(day, {"models": {}})
        fold["sizes"] = split_sizes(train=train, val=val, test=test)
        fold["test_labels"] = {r["label"]: r["count"]
                               for r in test.groupBy("label").count().collect()}
        train_labels = {r["label"] for r in train.select("label").distinct().collect()}
        fold["unseen_attack_labels"] = sorted(set(fold["test_labels"]) - train_labels)
        fold["unseen_attack_families"] = sorted(
            {S.label_family(l) for l in fold["test_labels"]}
            - {S.label_family(l) for l in train_labels})
        for m in a.models:
            fold["models"][m] = fit_eval(m, feats, train, val, test, a.preset)
        train.unpersist()
        res["folds"][day] = fold
        write_json(res, out_path)
    df.unpersist()
    return res


def multiclass(spark, a) -> dict:
    import numpy as np
    from pyspark.sql import functions as F

    meta = read_json(Path(a.parquet) / "meta.json")
    feats = meta["features"]
    fam_idx = F.create_map(*[x for f, i in S.FAMILY_INDEX.items() for x in (F.lit(f), F.lit(float(i)))])
    df = load(spark, a.parquet, "dedup").withColumn("fam_idx", fam_idx[F.col("family")])
    train, val, test = random_split(df, a.seed)
    train = subsample(train, a.train_frac, a.seed)
    train, weights = add_weights(train, "fam_idx", mode="sqrt")
    train = train.cache()
    log(f"multiclass RF on {train.count():,} training rows")
    t0 = time.perf_counter()
    model = multiclass_pipeline(feats, a.preset).fit(train)
    fit_s = time.perf_counter() - t0
    K = len(S.FAMILIES)
    cm = np.zeros((K, K), dtype=np.int64)
    for r in (model.transform(test).groupBy("fam_idx", "mc_pred").count().collect()):
        cm[int(r["fam_idx"]), int(r["mc_pred"])] += r["count"]
    per = {}
    for i, f in enumerate(S.FAMILIES):
        tp = cm[i, i]
        p = tp / cm[:, i].sum() if cm[:, i].sum() else 0.0
        rc = tp / cm[i, :].sum() if cm[i, :].sum() else 0.0
        per[f] = {"support": int(cm[i, :].sum()), "precision": p, "recall": rc,
                  "f1": 2 * p * rc / (p + rc) if p + rc else 0.0}
    sup = np.array([per[f]["support"] for f in S.FAMILIES])
    f1s = np.array([per[f]["f1"] for f in S.FAMILIES])
    present = sup > 0
    res = {"families": S.FAMILIES, "confusion_matrix": cm.tolist(), "per_class": per,
           "macro_f1": float(f1s[present].mean()),
           "weighted_f1": float((f1s * sup).sum() / sup.sum()),
           "accuracy": float(np.trace(cm) / cm.sum()), "fit_s": round(fit_s, 2),
           "class_weights": weights, "preset": a.preset, "train_frac": a.train_frac}
    log(f"  macro F1={res['macro_f1']:.4f} weighted F1={res['weighted_f1']:.4f}")
    dst = Path(a.models_out) / "multiclass_rf"
    model.write().overwrite().save(str(dst))
    dp = Path(a.models_out) / "deploy.json"
    deploy = read_json(dp) if dp.exists() else {"features": feats, "families": S.FAMILIES}
    deploy["multiclass_model"] = "multiclass_rf"
    write_json(deploy, dp)
    write_json(res, Path(a.results) / "multiclass.json")
    train.unpersist()
    return res


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("what", choices=["scenario1", "scenario2", "multiclass", "all"])
    ap.add_argument("--parquet", default="work/parquet")
    ap.add_argument("--results", default="results")
    ap.add_argument("--models-out", default="models")
    ap.add_argument("--work", default="work")
    ap.add_argument("--models", default="lr,dt,rf,gbt")
    ap.add_argument("--preset", default="full", choices=list(PRESETS))
    ap.add_argument("--train-frac", type=float, default=1.0,
                    help="stratified fraction of the training split to use (rare labels kept)")
    ap.add_argument("--days", default=None, help="scenario2: comma-separated held-out days")
    ap.add_argument("--resume", action="store_true", help="scenario2: skip finished folds")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--master", default=None)
    a = ap.parse_args()
    a.models = [m.strip() for m in a.models.split(",") if m.strip()]
    a.days = a.days.split(",") if a.days else None
    spark = get_spark(f"ids-train-{a.what}", a.master)
    if a.what in ("scenario1", "all"):
        scenario1(spark, a)
    if a.what in ("multiclass", "all"):
        multiclass(spark, a)
    if a.what in ("scenario2", "all"):
        scenario2(spark, a)


if __name__ == "__main__":
    main()
