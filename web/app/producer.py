"""Kafka producers used by the web app: replay a day of CSV flows, or push an uploaded file.

Rows are sent with canonical column names (ids.schema.canon) and raw string values; the Spark
scorer and the saved model take care of casting / cleaning, exactly as in batch.
CLI:  python -m app.producer --file /data/Wednesday-14-02-2018_TrafficForML_CICFlowMeter.csv --rate 500
"""
from __future__ import annotations

import csv
import io
import json
import os
import threading
import time
import uuid

from confluent_kafka import Producer

from ids import schema as S

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "flows")
NUMERIC = set(S.NUMERIC_COLUMNS)


def make_producer() -> Producer:
    return Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20, "batch.num.messages": 5000,
                     "queue.buffering.max.messages": 500000, "compression.type": "lz4"})


def row_message(header: list[str], values: list[str], src: str, uid: str, row: int,
                day: str | None, job: str | None = None) -> bytes | None:
    if len(values) != len(header):
        return None
    d = dict(zip(header, values))
    label = d.get("label")
    if label is not None and label.strip() == "Label":      # repeated header row in the CSV
        return None
    f = {k: v for k, v in d.items() if k in NUMERIC}
    return json.dumps({"uid": uid, "src": src, "job": job, "row": row, "day": day,
                       "ts": int(time.time() * 1000), "label": label, "f": f},
                      separators=(",", ":")).encode()


def _send(p: Producer, msg: bytes):
    while True:
        try:
            p.produce(TOPIC, msg)
            p.poll(0)
            return
        except BufferError:
            p.poll(0.2)


class Replayer:
    """Background thread streaming a CSV file into Kafka at a fixed rate (flows / second)."""

    def __init__(self):
        self.thread: threading.Thread | None = None
        self.stop_evt = threading.Event()
        self.state = {"running": False}

    def start(self, path: str, rate: int, start_pct: float = 0.0, limit: int | None = None):
        self.stop()
        self.stop_evt.clear()
        self.state = {"running": True, "file": os.path.basename(path), "rate": rate,
                      "start_pct": start_pct, "limit": limit, "sent": 0,
                      "started_at": int(time.time() * 1000), "error": None}
        self.thread = threading.Thread(target=self._run, args=(path, rate, start_pct, limit),
                                       daemon=True)
        self.thread.start()

    def stop(self):
        if self.thread and self.thread.is_alive():
            self.stop_evt.set()
            self.thread.join(timeout=10)
        self.state["running"] = False

    def _run(self, path, rate, start_pct, limit):
        try:
            day = S.day_from_filename(path)
            p = make_producer()
            size = os.path.getsize(path)
            with open(path, newline="", encoding="utf-8", errors="replace") as fh:
                header = [S.canon(c) for c in next(csv.reader([fh.readline()]))]
                if start_pct > 0:
                    fh.seek(int(size * start_pct / 100.0))
                    fh.readline()                       # realign on a line boundary
                lineno = 0
                tick, per_tick = 0.1, max(1, int(rate * 0.1))
                t_next = time.perf_counter()
                reader = csv.reader(fh)
                while not self.stop_evt.is_set():
                    for _ in range(per_tick):
                        try:
                            values = next(reader)
                        except StopIteration:
                            self.stop_evt.set()
                            break
                        lineno += 1
                        msg = row_message(header, values, "replay",
                                          f"{day}-{int(start_pct * 100)}-{lineno}", lineno, day)
                        if msg:
                            _send(p, msg)
                            self.state["sent"] += 1
                        if limit and self.state["sent"] >= limit:
                            self.stop_evt.set()
                            break
                    t_next += tick
                    time.sleep(max(0.0, t_next - time.perf_counter()))
            p.flush(30)
        except Exception as e:  # surfaced on the dashboard
            self.state["error"] = repr(e)
        finally:
            self.state["running"] = False
            self.state["finished_at"] = int(time.time() * 1000)


def publish_upload(content: bytes, job: str, max_rows: int, on_done=None) -> tuple[int, list[str]]:
    """Validate an uploaded CICFlowMeter CSV and push its rows to Kafka (background thread).

    Returns (rows queued, header). Raises ValueError if required feature columns are missing.
    """
    text = content.decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(text))
    header = [S.canon(c) for c in next(reader)]
    rows = []
    for i, values in enumerate(reader):
        if i >= max_rows:
            break
        rows.append(values)
    missing = [c for c in S.CANDIDATE_FEATURES if c not in header]
    if len(missing) > 10:
        raise ValueError(f"file is missing {len(missing)} feature columns, e.g. {missing[:8]}")

    def run():
        p = make_producer()
        for i, values in enumerate(rows):
            msg = row_message(header, values, "upload", f"{job}-{i}", i, None, job)
            if msg:
                _send(p, msg)
        p.flush(60)
        if on_done:
            on_done()

    threading.Thread(target=run, daemon=True).start()
    return len(rows), header


def new_job_id() -> str:
    return uuid.uuid4().hex[:12]


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--rate", type=int, default=500)
    ap.add_argument("--start-pct", type=float, default=0.0)
    ap.add_argument("--limit", type=int, default=None)
    a = ap.parse_args()
    r = Replayer()
    r.start(a.file, a.rate, a.start_pct, a.limit)
    while r.state["running"]:
        time.sleep(2)
        print(r.state, flush=True)
