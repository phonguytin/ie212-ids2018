"""Serving layer: FastAPI backend + static web pages for SOC operators."""
from __future__ import annotations

import glob
import json
import os
import time
from datetime import timezone
from pathlib import Path

from bson import ObjectId
from fastapi import Body, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pymongo import DESCENDING, MongoClient

from . import producer as P

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
DB = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)[os.environ.get("MONGO_DB", "ids")]
MODELS_DIR = Path(os.environ.get("MODELS_DIR", "/opt/ids/models"))
RESULTS_DIR = Path(os.environ.get("RESULTS_DIR", "/opt/ids/results"))
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
UPLOAD_MAX_ROWS = int(os.environ.get("UPLOAD_MAX_ROWS", "200000"))
STATIC = Path(__file__).parent / "static"
BINS = 1000

app = FastAPI(title="IDS2018 cost-sensitive detection")
replayer = P.Replayer()


@app.middleware("http")
async def no_stale_pages(request, call_next):
    # pages/JS/CSS revalidate on every load (ETag), so edits show up without a hard refresh
    resp = await call_next(request)
    if not request.url.path.startswith("/api/"):
        resp.headers["Cache-Control"] = "no-cache"
    return resp


def _json(path: Path):
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return None


def _clean(doc):
    if doc and "_id" in doc:
        doc["id"] = str(doc.pop("_id"))
    return doc


def _iso(t):
    """Mongo returns naive UTC datetimes; mark them as UTC for the browser."""
    if hasattr(t, "isoformat"):
        return (t if t.tzinfo else t.replace(tzinfo=timezone.utc)).isoformat()
    return t


def deploy_info():
    return _json(MODELS_DIR / "deploy.json") or {}


def get_config():
    cfg = DB.config.find_one({"_id": "runtime"})
    if not cfg:
        d = deploy_info()
        cfg = {"_id": "runtime", "threshold": d.get("default_threshold", 0.5),
               "c_fp": 1.0, "c_fn": float(d.get("default_ratio", 50))}
        DB.config.insert_one(cfg)
    cfg.pop("_id", None)
    return cfg


def optimal_threshold(ratio: float) -> float | None:
    """Min-cost threshold on the deployed model's validation score histogram."""
    d = deploy_info()
    vh = d.get("val_hist")
    if not vh:
        return None
    pos, neg = vh["pos"], vh["neg"]
    P_, best, best_k = sum(pos), None, BINS // 2
    tp = fp = 0
    # walk thresholds from high to low: at k we alert on bins >= k
    for k in range(BINS, -1, -1):
        if k < BINS:
            tp += pos[k]
            fp += neg[k]
        cost = fp + ratio * (P_ - tp)
        if best is None or cost < best or (cost == best and abs(k - BINS // 2) < abs(best_k - BINS // 2)):
            best, best_k = cost, k
    return best_k / BINS


# ------------------------------------------------------------------ status / config

@app.get("/api/status")
def status():
    sc = DB.scorer_status.find_one({"_id": "scorer"}) or {}
    sc.pop("_id", None)
    d = deploy_info()
    return {"scorer": sc, "replay": replayer.state, "config": get_config(),
            "model": {k: d.get(k) for k in ("binary_algorithm", "binary_algorithm_name",
                                            "selected_by", "default_threshold", "thresholds",
                                            "multiclass_model", "created", "test_metrics")},
            "server_time": int(time.time() * 1000)}


@app.get("/api/config")
def read_config():
    cfg = get_config()
    cfg["optimal_threshold"] = optimal_threshold(cfg["c_fn"] / cfg["c_fp"])
    return cfg


@app.post("/api/config")
def write_config(body: dict = Body(...)):
    cfg = get_config()
    for k in ("c_fp", "c_fn"):
        if k in body and body[k] is not None:
            v = float(body[k])
            if v <= 0:
                raise HTTPException(400, f"{k} must be > 0")
            cfg[k] = v
    if body.get("use_optimal"):
        thr = optimal_threshold(cfg["c_fn"] / cfg["c_fp"])
        if thr is None:
            raise HTTPException(400, "deploy.json has no validation histogram")
        cfg["threshold"] = thr
    elif body.get("threshold") is not None:
        t = float(body["threshold"])
        if not 0.0 <= t <= 1.0:
            raise HTTPException(400, "threshold must be in [0, 1]")
        cfg["threshold"] = t
    cfg["updated_at"] = int(time.time() * 1000)
    DB.config.update_one({"_id": "runtime"}, {"$set": cfg}, upsert=True)
    DB.config_history.insert_one(dict(cfg))
    return read_config()


@app.get("/api/deploy")
def deploy():
    return deploy_info()


# ------------------------------------------------------------------ live dashboard

@app.get("/api/overview")
def overview(src: str = "replay", windows: int = 90):
    cfg = get_config()
    q = {} if src == "all" else {"src": src}
    docs = list(DB.metrics.find(q, {"_id": 0}).sort("window_start", DESCENDING).limit(windows))
    docs.reverse()
    keys = ("n", "alerts", "tp", "fp", "fn", "tn", "labeled", "latency_ms_sum")
    tot = {k: 0 for k in keys}
    by_type, by_label = {}, {}
    for d in DB.metrics.find(q, {"_id": 0}):
        for k in keys:
            tot[k] += d.get(k, 0)
        for t, n in (d.get("by_type") or {}).items():
            by_type[t] = by_type.get(t, 0) + n
        for t, n in (d.get("by_label") or {}).items():
            by_label[t] = by_label.get(t, 0) + n
    c_fp, c_fn = cfg["c_fp"], cfg["c_fn"]
    # cumulative cost series over *all* windows, then keep the tail that is displayed
    cum, series = 0.0, []
    first_shown = docs[0]["window_start"] if docs else None
    for d in DB.metrics.find(q, {"_id": 0, "window_start": 1, "fp": 1, "fn": 1}).sort("window_start", 1):
        cum += c_fp * d.get("fp", 0) + c_fn * d.get("fn", 0)
        if first_shown and d["window_start"] >= first_shown:
            series.append(cum)
    win = [{"t": _iso(d["window_start"]), "n": d.get("n", 0), "alerts": d.get("alerts", 0),
            "fp": d.get("fp", 0), "fn": d.get("fn", 0), "tp": d.get("tp", 0),
            "cum_cost": series[i] if i < len(series) else None} for i, d in enumerate(docs)]
    tp, fp, fn = tot["tp"], tot["fp"], tot["fn"]
    return {"config": cfg, "totals": tot,
            "precision": tp / (tp + fp) if tp + fp else None,
            "recall": tp / (tp + fn) if tp + fn else None,
            "cost": c_fp * fp + c_fn * fn,
            "avg_latency_ms": tot["latency_ms_sum"] / tot["n"] if tot["n"] else None,
            "by_type": by_type, "by_label": by_label, "windows": win,
            "replay": replayer.state}


@app.get("/api/alerts")
def alerts(limit: int = Query(50, le=500), skip: int = 0, type: str | None = None,
           src: str | None = None, min_p: float = 0.0, outcome: str | None = None):
    q: dict = {}
    if type:
        q["attack_type"] = type
    if src:
        q["src"] = src
    if min_p > 0:
        q["p_attack"] = {"$gte": min_p}
    if outcome == "fp":
        q["y_true"] = False
    elif outcome == "tp":
        q["y_true"] = True
    total = DB.alerts.count_documents(q) if q else DB.alerts.estimated_document_count()
    cur = DB.alerts.find(q, {"features": 0}).sort("scored_at", DESCENDING).skip(skip).limit(limit)
    return {"total": total, "items": [_clean(d) for d in cur]}


@app.get("/api/alerts/{alert_id}")
def alert_detail(alert_id: str):
    try:
        d = DB.alerts.find_one({"_id": ObjectId(alert_id)})
    except Exception:
        d = None
    if not d:
        raise HTTPException(404, "alert not found")
    return _clean(d)


# ------------------------------------------------------------------ replay

@app.get("/api/files")
def files():
    return [{"name": os.path.basename(p), "size": os.path.getsize(p)}
            for p in sorted(glob.glob(str(DATA_DIR / "*.csv")))]


@app.post("/api/replay/start")
def replay_start(body: dict = Body(...)):
    name = os.path.basename(body.get("file", ""))
    path = DATA_DIR / name
    if not path.exists():
        raise HTTPException(404, f"{name} not found in data dir")
    rate = max(1, min(int(body.get("rate", 500)), 20000))
    replayer.start(str(path), rate, float(body.get("start_pct", 0)), body.get("limit"))
    return replayer.state


@app.post("/api/replay/stop")
def replay_stop():
    replayer.stop()
    return replayer.state


@app.get("/api/replay")
def replay_state():
    return replayer.state


@app.post("/api/reset")
def reset():
    replayer.stop()
    for c in ("alerts", "metrics", "upload_jobs", "upload_results"):
        DB[c].delete_many({})
    return {"ok": True}


# ------------------------------------------------------------------ uploads

@app.post("/api/upload")
async def upload(file: UploadFile = File(...)):
    content = await file.read()
    job = P.new_job_id()

    def done():
        DB.upload_jobs.update_one({"_id": job}, {"$set": {"published": True}})
    try:
        n, header = P.publish_upload(content, job, UPLOAD_MAX_ROWS, on_done=done)
    except (ValueError, StopIteration) as e:
        raise HTTPException(400, f"invalid CSV: {e}")
    DB.upload_jobs.insert_one({"_id": job, "filename": file.filename, "total": n, "scored": 0,
                               "alerts": 0, "has_label": "label" in header,
                               "created_at": int(time.time() * 1000), "published": False})
    return {"job": job, "total": n, "truncated": n >= UPLOAD_MAX_ROWS}


@app.get("/api/uploads")
def uploads():
    return [_clean(d) for d in DB.upload_jobs.find().sort("created_at", DESCENDING).limit(20)]


@app.get("/api/uploads/{job}")
def upload_job(job: str, limit: int = Query(100, le=1000)):
    j = DB.upload_jobs.find_one({"_id": job})
    if not j:
        raise HTTPException(404, "job not found")
    j = _clean(j)
    agg = list(DB.upload_results.aggregate([
        {"$match": {"job": job}},
        {"$group": {"_id": {"t": "$attack_type", "l": "$label", "a": "$alert"}, "n": {"$sum": 1}}}]))
    by_type, conf = {}, {"tp": 0, "fp": 0, "fn": 0, "tn": 0}
    for g in agg:
        t, lab, a, n = g["_id"].get("t"), g["_id"].get("l"), g["_id"].get("a"), g["n"]
        if a:
            by_type[t or "?"] = by_type.get(t or "?", 0) + n
        if lab:
            att = lab.strip().lower() != "benign"
            conf[("tp" if att else "fp") if a else ("fn" if att else "tn")] += n
    hist = [0] * 20
    for g in DB.upload_results.aggregate([
            {"$match": {"job": job}},
            {"$bucket": {"groupBy": "$p_attack", "boundaries": [i / 20 for i in range(20)] + [1.01],
                         "default": "other", "output": {"n": {"$sum": 1}}}}]):
        if g["_id"] != "other":
            hist[min(int(round(g["_id"] * 20)), 19)] += g["n"]
    top = [_clean(d) for d in DB.upload_results.find({"job": job, "alert": True})
           .sort("p_attack", DESCENDING).limit(limit)]
    j.update({"by_type": by_type, "confusion": conf if j.get("has_label") else None,
              "p_hist": hist, "top_alerts": top, "config": get_config()})
    return j


# ------------------------------------------------------------------ experiments

@app.get("/api/experiments")
def experiments():
    out = {}
    for name in ("quality_report", "scenario1", "scenario2", "multiclass", "scalability"):
        out[name] = _json(RESULTS_DIR / f"{name}.json")
    out["deploy"] = {k: v for k, v in deploy_info().items() if k != "val_hist"}
    return out


# ------------------------------------------------------------------ pages

for page in ("index", "alerts", "upload", "experiments", "config"):
    def _mk(p=page):
        return lambda: FileResponse(STATIC / f"{p}.html")
    app.add_api_route("/" if page == "index" else f"/{page}", _mk(), include_in_schema=False)
app.mount("/static", StaticFiles(directory=STATIC), name="static")
