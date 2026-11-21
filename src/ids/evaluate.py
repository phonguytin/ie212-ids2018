"""Cost-aware evaluation from binned scores.

Each prediction set is reduced (on Spark) to a histogram of P(attack) over B=1000 bins, per raw
label. From that tiny array we get, for *every* threshold t=k/B at once, TP/FP/FN/TN, precision,
recall, F1, PR-AUC, ROC-AUC, the cost FP*c_fp + FN*c_fn and per-attack recall. The same arrays
are exported to the web page so the cost-ratio slider can recompute optimal thresholds offline.

Decision rule: alert  <=>  p >= k/B  <=>  bin(p) >= k   (bin = min(floor(p*B), B-1)).
"""
from __future__ import annotations

import numpy as np

B = 1000
COST_RATIOS = [10, 50, 100]          # c_FP : c_FN = 1 : r
DEFAULT_RATIO = 50


def score_histograms(pred_df, prob_col: str = "bin_prob", label_col: str = "label") -> dict:
    """Spark DataFrame with a probability vector -> {label: np.array(B) of counts}."""
    from pyspark.ml.functions import vector_to_array
    from pyspark.sql import functions as F

    p = vector_to_array(F.col(prob_col))[1]
    b = F.least(F.floor(p * B).cast("int"), F.lit(B - 1))
    rows = pred_df.select(F.col(label_col).alias("label"), b.alias("b")) \
                  .groupBy("label", "b").count().collect()
    hist: dict[str, np.ndarray] = {}
    for r in rows:
        hist.setdefault(r["label"], np.zeros(B, dtype=np.int64))[r["b"]] += r["count"]
    return hist


def split_pos_neg(hist: dict, benign: str = "Benign"):
    neg = hist.get(benign, np.zeros(B, dtype=np.int64))
    pos = np.zeros(B, dtype=np.int64)
    for lab, h in hist.items():
        if lab != benign:
            pos = pos + h
    return pos, neg


def confusion_curve(pos: np.ndarray, neg: np.ndarray) -> dict:
    """Arrays of length B+1 indexed by k (threshold k/B)."""
    sfx = lambda a: np.concatenate([np.cumsum(a[::-1])[::-1], [0]])
    tp, fp = sfx(pos), sfx(neg)
    P, N = int(pos.sum()), int(neg.sum())
    return {"tp": tp, "fp": fp, "fn": P - tp, "tn": N - fp, "P": P, "N": N}


def costs(curve: dict, ratio: float, c_fp: float = 1.0) -> np.ndarray:
    return c_fp * curve["fp"] + ratio * c_fp * curve["fn"]


def best_k(curve: dict, ratio: float) -> int:
    c = costs(curve, ratio)
    m = c.min()
    cand = np.flatnonzero(c == m)
    return int(cand[np.argmin(np.abs(cand - B // 2))])   # ties -> closest to 0.5


def metrics_at(curve: dict, k: int, ratios=COST_RATIOS) -> dict:
    tp, fp, fn, tn = (int(curve[x][k]) for x in ("tp", "fp", "fn", "tn"))
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
    fpr = fp / (fp + tn) if fp + tn else 0.0
    n = tp + fp + fn + tn
    return {"threshold": k / B, "tp": tp, "fp": fp, "fn": fn, "tn": tn,
            "precision": prec, "recall": rec, "f1": f1, "fpr": fpr,
            "accuracy": (tp + tn) / n if n else 0.0,
            "cost": {str(r): fp + r * fn for r in ratios},
            "cost_per_1k_flows": {str(r): 1000 * (fp + r * fn) / n if n else 0.0 for r in ratios}}


def pr_auc(curve: dict) -> float:
    """Average precision (step-wise area under the PR curve) over the B thresholds."""
    tp, fp, P = curve["tp"].astype(float), curve["fp"].astype(float), curve["P"]
    if P == 0:
        return float("nan")
    with np.errstate(invalid="ignore", divide="ignore"):
        prec = np.where(tp + fp > 0, tp / (tp + fp), 1.0)
    rec = tp / P
    # thresholds high -> low: recall increases
    r, p = rec[::-1], prec[::-1]
    return float(np.sum(np.diff(np.concatenate([[0.0], r])) * p))


def roc_auc(curve: dict) -> float:
    if curve["P"] == 0 or curve["N"] == 0:
        return float("nan")
    tpr = (curve["tp"] / curve["P"])[::-1]
    fpr = (curve["fp"] / curve["N"])[::-1]
    return float(np.trapezoid(tpr, fpr) if hasattr(np, "trapezoid") else np.trapz(tpr, fpr))


def per_attack(test_hist: dict, ks: dict, ratio_cost: float = DEFAULT_RATIO) -> dict:
    """Recall and missed-attack cost per raw attack label at the given thresholds {name: k}."""
    out = {}
    for lab, h in sorted(test_hist.items()):
        if lab == "Benign":
            continue
        n = int(h.sum())
        row = {"n": n}
        for name, k in ks.items():
            detected = int(h[k:].sum())
            row[f"recall@{name}"] = detected / n if n else 0.0
            row[f"fn@{name}"] = n - detected
            row[f"fn_cost@{name}"] = (n - detected) * ratio_cost
        out[lab] = row
    return out


def summarize(val_hist: dict, test_hist: dict, ratios=COST_RATIOS) -> dict:
    """Full report: metrics at 0.5 and at the validation-optimal threshold for each ratio."""
    vpos, vneg = split_pos_neg(val_hist)
    tpos, tneg = split_pos_neg(test_hist)
    vc, tc = confusion_curve(vpos, vneg), confusion_curve(tpos, tneg)
    k05 = B // 2
    rep = {"test_pr_auc": pr_auc(tc), "test_roc_auc": roc_auc(tc), "val_pr_auc": pr_auc(vc),
           "at_0.5": metrics_at(tc, k05, ratios), "optimal": {}}
    ks = {"0.5": k05}
    for r in ratios:
        k = best_k(vc, r)
        m = metrics_at(tc, k, ratios)
        m["val_cost"] = int(costs(vc, r)[k])
        m["test_cost_at_0.5"] = int(costs(tc, r)[k05])
        m["test_cost_oracle"] = int(costs(tc, r).min())   # best possible on test (for reference)
        m["cost_reduction_vs_0.5"] = (1 - m["cost"][str(r)] / m["test_cost_at_0.5"]
                                      if m["test_cost_at_0.5"] else 0.0)
        rep["optimal"][str(r)] = m
        ks[f"opt{r}"] = k
    rep["per_attack"] = per_attack(test_hist, ks)
    rep["curves"] = {"val": {"pos": vpos.tolist(), "neg": vneg.tolist()},
                     "test": {"pos": tpos.tolist(), "neg": tneg.tolist()}}
    rep["test_label_hist"] = {lab: _sparse(h) for lab, h in test_hist.items() if lab != "Benign"}
    return rep


def _sparse(h: np.ndarray) -> dict:
    nz = np.flatnonzero(h)
    return {"b": nz.tolist(), "n": h[nz].tolist()}
