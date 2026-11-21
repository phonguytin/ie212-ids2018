// Shared helpers for all pages.
const PAGES = [
  ["/", "Tổng quan"], ["/alerts", "Cảnh báo"], ["/upload", "Phân tích file"],
  ["/experiments", "Thực nghiệm"], ["/config", "Cấu hình ngưỡng"],
];
const COLORS = ["#4c9aff", "#f85149", "#d29922", "#3fb950", "#a371f7", "#db61a2", "#39c5cf", "#8b98a5"];
const TYPE_COLORS = { DoS: "#f85149", DDoS: "#db61a2", BruteForce: "#d29922", WebAttack: "#a371f7",
  Bot: "#39c5cf", Infiltration: "#3fb950", Attack: "#4c9aff", Benign: "#8b98a5" };

function header() {
  const here = location.pathname.replace(/\/$/, "") || "/";
  const h = document.createElement("header");
  h.innerHTML = `<div class="brand">IDS2018 <small>Phát hiện xâm nhập nhạy chi phí · Spark + Kafka</small></div>
    <nav>${PAGES.map(([u, t]) => `<a href="${u}" class="${(u === "/" ? here === "/" : here.startsWith(u)) ? "active" : ""}">${t}</a>`).join("")}</nav>
    <div class="status"><span id="st-scorer"><span class="dot"></span>Scorer</span><span id="st-thr"></span></div>`;
  document.body.prepend(h);
  refreshStatus(); setInterval(refreshStatus, 5000);
}

async function refreshStatus() {
  try {
    const s = await api("/api/status");
    const last = s.scorer.last_batch_at || 0;
    const alive = s.server_time - last < 30000;
    // Structured Streaming only runs a micro-batch when Kafka has new records.
    document.querySelector("#st-scorer").innerHTML = !last
      ? `<span class="dot off"></span>Scorer chưa chạy batch nào`
      : `<span class="dot ${alive ? "on" : ""}"></span>Scorer ${alive ? "đang xử lý" : "chờ dữ liệu · batch cuối " + time(last)}`;
    document.querySelector("#st-thr").textContent =
      `Mô hình: ${s.model.binary_algorithm_name || "?"} · ngưỡng ${fmt(s.config.threshold, 3)} · c_FP:c_FN = ${fmt(s.config.c_fp, 0)}:${fmt(s.config.c_fn, 0)}`;
  } catch (e) { /* api down */ }
}

async function api(url, opts = {}) {
  const r = await fetch(url, opts);
  if (!r.ok) { let m = r.statusText; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
  return r.json();
}
const post = (url, body) => api(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });

function fmt(v, d = 0) {
  if (v === null || v === undefined || Number.isNaN(v)) return "–";
  return Number(v).toLocaleString("vi-VN", { minimumFractionDigits: d, maximumFractionDigits: d });
}
const pct = (v, d = 2) => (v === null || v === undefined || Number.isNaN(v)) ? "–" : fmt(v * 100, d) + "%";
const esc = s => String(s ?? "").replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const time = ms => ms ? new Date(ms).toLocaleTimeString("vi-VN") : "–";
const dtime = ms => ms ? new Date(ms).toLocaleString("vi-VN") : "–";
function outcomeTag(y) {
  if (y === true) return `<span class="tag tp">TP</span>`;
  if (y === false) return `<span class="tag fp">FP</span>`;
  return `<span class="tag unk">không nhãn</span>`;
}

// ---- cost curves from score histograms (same math as src/ids/evaluate.py) ----
function confCurve(pos, neg) {
  const B = pos.length, tp = new Array(B + 1).fill(0), fp = new Array(B + 1).fill(0);
  for (let k = B - 1; k >= 0; k--) { tp[k] = tp[k + 1] + pos[k]; fp[k] = fp[k + 1] + neg[k]; }
  const P = tp[0], N = fp[0];
  return { tp, fp, fn: tp.map(x => P - x), tn: fp.map(x => N - x), P, N, B };
}
function costAt(c, k, ratio) { return c.fp[k] + ratio * c.fn[k]; }
function bestK(c, ratio) {
  let best = Infinity, bk = c.B / 2;
  for (let k = 0; k <= c.B; k++) {
    const v = costAt(c, k, ratio);
    if (v < best || (v === best && Math.abs(k - c.B / 2) < Math.abs(bk - c.B / 2))) { best = v; bk = k; }
  }
  return bk;
}
function metricsAt(c, k) {
  const tp = c.tp[k], fp = c.fp[k], fn = c.fn[k];
  const p = tp + fp ? tp / (tp + fp) : 0, r = tp + fn ? tp / (tp + fn) : 0;
  return { tp, fp, fn, precision: p, recall: r, f1: p + r ? 2 * p * r / (p + r) : 0 };
}

const charts = {};
function chart(id, cfg) {
  if (charts[id]) { charts[id].data = cfg.data; if (cfg.options) charts[id].options = cfg.options; charts[id].update("none"); return charts[id]; }
  cfg.options = Object.assign({ responsive: true, maintainAspectRatio: false, animation: false }, cfg.options || {});
  charts[id] = new Chart(document.getElementById(id), cfg);
  return charts[id];
}
if (window.Chart) {
  Chart.defaults.color = "#8b98a5"; Chart.defaults.borderColor = "#2a3542";
  Chart.defaults.font.family = "system-ui, -apple-system, Segoe UI, Roboto, sans-serif";
}
document.addEventListener("DOMContentLoaded", header);
