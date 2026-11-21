"""Figures (PNG) and Markdown tables for the written report, from results/*.json.

    python -m ids.figures --results results          # -> results/figures/*.png, results/summary.md
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np

from . import evaluate as E
from .spark_utils import read_json

MN = {"lr": "Logistic Regression", "dt": "Decision Tree", "rf": "Random Forest",
      "gbt": "Gradient-Boosted Trees"}


def _load(results: Path, name: str):
    p = results / f"{name}.json"
    return read_json(p) if p.exists() else None


def _curves(m):
    vc = E.confusion_curve(np.array(m["curves"]["val"]["pos"]), np.array(m["curves"]["val"]["neg"]))
    tc = E.confusion_curve(np.array(m["curves"]["test"]["pos"]), np.array(m["curves"]["test"]["neg"]))
    return vc, tc


def main():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="results")
    a = ap.parse_args()
    res = Path(a.results)
    fig_dir = res / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)
    md: list[str] = ["# Tóm tắt kết quả thực nghiệm\n"]
    plt.rcParams.update({"figure.dpi": 130, "font.size": 9, "axes.spines.top": False,
                         "axes.spines.right": False})

    def save(name):
        plt.tight_layout()
        plt.savefig(fig_dir / name)
        plt.close()
        print(f"[ids] wrote {fig_dir / name}")

    # ---------------- data quality
    q = _load(res, "quality_report")
    if q:
        d = q["duplicates"]
        raw = sum(f["rows"] for f in q["per_file_counts"])
        md += ["## 1. Chất lượng dữ liệu\n",
               "| Chỉ số | Giá trị |", "|---|---:|",
               f"| Dòng đọc từ CSV | {raw:,} |",
               f"| Dòng tiêu đề lặp | {sum(f['header_rows'] for f in q['per_file_counts']):,} |",
               f"| Dòng chứa NaN/Infinity | {sum(f['invalid_rows'] for f in q['per_file_counts']):,} |",
               f"| Dòng sạch | {d['rows_clean']:,} |",
               f"| Bản ghi trùng lặp bị loại | {d['duplicate_rows_removed']:,} ({d['duplicate_ratio']:.2%}) |",
               f"| Dòng sau khử trùng lặp | {d['rows_dedup']:,} |",
               f"| Vector đặc trưng trùng nhưng khác nhãn (dòng / nhóm) | {d['conflicting_label_rows']:,} / {d['conflicting_label_groups']:,} |",
               f"| Số đặc trưng dùng cho mô hình | {len(q['features'])} |",
               f"| Cột hằng số bị loại | {', '.join(q['constant_columns'])} |", "",
               "| Nhãn | Trước | Sau | % loại |", "|---|---:|---:|---:|"]
        items = sorted(d["by_label"].items(), key=lambda x: -x[1]["before"])
        md += [f"| {k} | {v['before']:,} | {v['after']:,} | {v['removed_ratio']:.1%} |" for k, v in items]
        md.append("")
        plt.figure(figsize=(7, 4))
        plt.barh([k for k, _ in items][::-1], [v["removed_ratio"] * 100 for _, v in items][::-1],
                 color="#d1495b")
        plt.xlabel("% bản ghi trùng lặp bị loại")
        plt.title("Tỷ lệ trùng lặp theo nhãn")
        save("quality_duplicates.png")

    # ---------------- scenario 1
    s1 = _load(res, "scenario1")
    if s1:
        md += ["## 2. Kịch bản 1 — chia ngẫu nhiên phân tầng\n"]
        for vn, v in s1["variants"].items():
            md += [f"**{'Có trùng lặp' if vn == 'clean' else 'Đã khử trùng lặp'}** — train/val/test "
                   f"= {v['sizes']['train']:,} / {v['sizes']['val']:,} / {v['sizes']['test']:,}; "
                   f"{v['test_rows_seen_in_train_ratio']:.2%} dòng test có bản sao trong train.\n"]
        for r in E.COST_RATIOS:
            md += [f"\n**Tỷ lệ chi phí 1:{r}**\n",
                   "| Dữ liệu | Mô hình | PR-AUC | F1@0.5 | Chi phí@0.5 | thr* | Recall@thr* | Precision@thr* | FP | FN | Chi phí@thr* | Giảm |",
                   "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
            for vn, v in s1["variants"].items():
                for mk, m in v["models"].items():
                    o = m["optimal"][str(r)]
                    md.append(f"| {vn} | {MN[mk]} | {m['test_pr_auc']:.4f} | {m['at_0.5']['f1']:.4f} | "
                              f"{o['test_cost_at_0.5']:,} | {o['threshold']:.3f} | {o['recall']:.4f} | "
                              f"{o['precision']:.4f} | {o['fp']:,} | {o['fn']:,} | {o['cost'][str(r)]:,} | "
                              f"{o['cost_reduction_vs_0.5']:.1%} |")
        md.append("")
        mks = list(s1["variants"]["clean"]["models"])
        x = np.arange(len(mks))
        plt.figure(figsize=(6.5, 3.6))
        for i, (vn, v) in enumerate(s1["variants"].items()):
            plt.bar(x + (i - .5) * .38, [v["models"][m]["test_pr_auc"] for m in mks], .38,
                    label="Có trùng lặp" if vn == "clean" else "Đã khử trùng lặp",
                    color=["#d1495b", "#2e86ab"][i])
        plt.xticks(x, [MN[m] for m in mks])
        lo = min(v["models"][m]["test_pr_auc"] for v in s1["variants"].values() for m in mks)
        plt.ylim(max(0, lo - .05), 1)
        plt.ylabel("PR-AUC (test)")
        plt.legend()
        plt.title("Kịch bản 1: ảnh hưởng của bản ghi trùng lặp")
        save("s1_prauc_dup_vs_dedup.png")

        dd = s1["variants"]["dedup"]["models"]
        fig, axes = plt.subplots(1, len(E.COST_RATIOS), figsize=(10, 3.4))
        for ax, r in zip(axes, E.COST_RATIOS):
            c05 = [dd[m]["optimal"][str(r)]["test_cost_at_0.5"] for m in dd]
            copt = [dd[m]["optimal"][str(r)]["cost"][str(r)] for m in dd]
            xx = np.arange(len(dd))
            ax.bar(xx - .2, c05, .4, label="ngưỡng 0,5", color="#999")
            ax.bar(xx + .2, copt, .4, label="ngưỡng tối ưu (val)", color="#2e86ab")
            ax.set_xticks(xx, [m.upper() for m in dd])
            ax.set_title(f"c_FP:c_FN = 1:{r}")
            ax.ticklabel_format(axis="y", style="sci", scilimits=(0, 0))
        axes[0].set_ylabel("chi phí trên test")
        axes[0].legend()
        save("s1_cost_threshold_vs_05.png")

        plt.figure(figsize=(6.5, 3.6))
        for mk, m in dd.items():
            _, tc = _curves(m)
            c = E.costs(tc, E.DEFAULT_RATIO)
            plt.plot(np.arange(len(c)) / E.B, c, label=MN[mk])
        plt.yscale("log")
        plt.xlabel("ngưỡng")
        plt.ylabel(f"chi phí test (1:{E.DEFAULT_RATIO})")
        plt.legend()
        plt.title("Chi phí theo ngưỡng (dữ liệu khử trùng lặp)")
        save("s1_cost_curve.png")

    # ---------------- scenario 2
    s2 = _load(res, "scenario2")
    if s2 and s2.get("folds"):
        days = sorted(s2["folds"])
        mks = list(s2["folds"][days[0]]["models"])
        r = str(E.DEFAULT_RATIO)
        md += ["## 3. Kịch bản 2 — leave-one-day-out (tỷ lệ 1:50)\n",
               "| Ngày test | Họ tấn công mới | " + " | ".join(f"{MN[m]} recall / chi phí" for m in mks) + " |",
               "|---|---|" + "---:|" * len(mks)]
        rec = np.full((len(days), len(mks)), np.nan)
        tot = {m: 0 for m in mks}
        for i, d in enumerate(days):
            f = s2["folds"][d]
            cells = []
            for j, m in enumerate(mks):
                if m not in f["models"]:
                    cells.append("–")
                    continue
                o = f["models"][m]["optimal"][r]
                rec[i, j] = o["recall"]
                tot[m] += o["cost"][r]
                cells.append(f"{o['recall']:.3f} / {o['cost'][r]:,}")
            md.append(f"| {d} | {', '.join(f['unseen_attack_families']) or '–'} | " + " | ".join(cells) + " |")
        md.append("| **Tổng chi phí** | | " + " | ".join(f"**{tot[m]:,}**" for m in mks) + " |\n")
        plt.figure(figsize=(6, 4.2))
        plt.imshow(rec, cmap="RdYlGn", vmin=0, vmax=1, aspect="auto")
        plt.colorbar(label="recall tại ngưỡng tối ưu")
        plt.xticks(range(len(mks)), [m.upper() for m in mks])
        plt.yticks(range(len(days)), days)
        for i in range(len(days)):
            for j in range(len(mks)):
                if not np.isnan(rec[i, j]):
                    plt.text(j, i, f"{rec[i, j]:.2f}", ha="center", va="center", fontsize=7)
        plt.title("Kịch bản 2: recall trên ngày chưa thấy (1:50)")
        save("s2_lodo_recall.png")

    # ---------------- multiclass
    mc = _load(res, "multiclass")
    if mc:
        F = mc["families"]
        cm = np.array(mc["confusion_matrix"], dtype=float)
        norm = cm / np.maximum(cm.sum(1, keepdims=True), 1)
        plt.figure(figsize=(5.5, 4.6))
        plt.imshow(norm, cmap="Blues", vmin=0, vmax=1)
        plt.xticks(range(len(F)), F, rotation=45, ha="right")
        plt.yticks(range(len(F)), F)
        for i in range(len(F)):
            for j in range(len(F)):
                if cm[i, j]:
                    plt.text(j, i, f"{norm[i, j]:.2f}", ha="center", va="center", fontsize=7,
                             color="white" if norm[i, j] > .5 else "black")
        plt.xlabel("dự đoán")
        plt.ylabel("thật")
        plt.title("RF đa lớp: ma trận nhầm lẫn (chuẩn hoá theo hàng)")
        save("multiclass_confusion.png")
        md += ["## 4. Random Forest đa lớp\n", "| Lớp | Support | Precision | Recall | F1 |",
               "|---|---:|---:|---:|---:|"]
        md += [f"| {f} | {mc['per_class'][f]['support']:,} | {mc['per_class'][f]['precision']:.4f} | "
               f"{mc['per_class'][f]['recall']:.4f} | {mc['per_class'][f]['f1']:.4f} |" for f in F]
        md.append(f"\nMacro-F1 = {mc['macro_f1']:.4f}, weighted-F1 = {mc['weighted_f1']:.4f}\n")

    # ---------------- scalability
    sc = _load(res, "scalability")
    if sc:
        md += ["## 5. Khả năng mở rộng\n",
               "| Engine | Tác vụ | Dữ liệu | Core | Trạng thái | Tổng (s) | Fit (s) | RAM đỉnh (MB) |",
               "|---|---|---:|---:|---|---:|---:|---:|"]
        for r_ in sc["runs"]:
            md.append(f"| {r_['engine']} | {r_['task']} | {r_['frac']:.0%} | {r_['cores']} | {r_['status']} | "
                      f"{r_.get('wall_s', r_['elapsed_s']):.1f} | {r_.get('fit_s', float('nan')):.1f} | "
                      f"{r_.get('peak_rss_mb', float('nan'))} |")
        fig, axes = plt.subplots(1, 2, figsize=(10, 3.6))
        for ax, task in zip(axes, ("etl", "train")):
            runs = [x for x in sc["runs"] if x["task"] == task]
            for key in sorted({(x["engine"], x["cores"]) for x in runs}):
                pts = sorted((x["frac"], x.get("fit_s" if task == "train" else "wall_s"))
                             for x in runs if (x["engine"], x["cores"]) == key and x["status"] == "ok")
                if pts:
                    ax.plot([p[0] * 100 for p in pts], [p[1] for p in pts], marker="o",
                            ls="-" if key[0] == "spark" else "--", label=f"{key[0]} {key[1]} core")
                failed = [x for x in runs if (x["engine"], x["cores"]) == key and x["status"] != "ok"]
                for x in failed:
                    ax.annotate(x["status"].upper(), (x["frac"] * 100, 0), color="red", fontsize=7)
            ax.set_xlabel("% dữ liệu thật")
            ax.set_ylabel("giây")
            ax.set_title("Xử lý CSV → Parquet" if task == "etl" else "Huấn luyện Random Forest")
            ax.legend(fontsize=7)
        save("scalability.png")

    (res / "summary.md").write_text("\n".join(md), encoding="utf-8")
    print(f"[ids] wrote {res / 'summary.md'}")


if __name__ == "__main__":
    main()
