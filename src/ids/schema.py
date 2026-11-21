"""Column naming, label mapping and feature lists shared by batch and streaming code.

All code paths (ETL, training, streaming scorer, web upload) go through these helpers so that
a flow is always turned into exactly the same feature vector.
"""
from __future__ import annotations

import re

# --------------------------------------------------------------------------------------
# Column names
# --------------------------------------------------------------------------------------

def canon(name: str) -> str:
    """'Flow Byts/s' -> 'flow_byts_s', 'Fwd Pkt Len Max' -> 'fwd_pkt_len_max'."""
    return re.sub(r"[^0-9a-z]+", "_", name.strip().lower()).strip("_")


# The 79 non-label columns of the "Processed Traffic Data for ML Algorithms" CSVs
# (9 of the 10 days). Thuesday-20-02-2018 has 4 more: Flow ID, Src IP, Src Port, Dst IP.
RAW_COLUMNS = [
    "Dst Port", "Protocol", "Timestamp", "Flow Duration", "Tot Fwd Pkts", "Tot Bwd Pkts",
    "TotLen Fwd Pkts", "TotLen Bwd Pkts", "Fwd Pkt Len Max", "Fwd Pkt Len Min",
    "Fwd Pkt Len Mean", "Fwd Pkt Len Std", "Bwd Pkt Len Max", "Bwd Pkt Len Min",
    "Bwd Pkt Len Mean", "Bwd Pkt Len Std", "Flow Byts/s", "Flow Pkts/s", "Flow IAT Mean",
    "Flow IAT Std", "Flow IAT Max", "Flow IAT Min", "Fwd IAT Tot", "Fwd IAT Mean",
    "Fwd IAT Std", "Fwd IAT Max", "Fwd IAT Min", "Bwd IAT Tot", "Bwd IAT Mean", "Bwd IAT Std",
    "Bwd IAT Max", "Bwd IAT Min", "Fwd PSH Flags", "Bwd PSH Flags", "Fwd URG Flags",
    "Bwd URG Flags", "Fwd Header Len", "Bwd Header Len", "Fwd Pkts/s", "Bwd Pkts/s",
    "Pkt Len Min", "Pkt Len Max", "Pkt Len Mean", "Pkt Len Std", "Pkt Len Var",
    "FIN Flag Cnt", "SYN Flag Cnt", "RST Flag Cnt", "PSH Flag Cnt", "ACK Flag Cnt",
    "URG Flag Cnt", "CWE Flag Count", "ECE Flag Cnt", "Down/Up Ratio", "Pkt Size Avg",
    "Fwd Seg Size Avg", "Bwd Seg Size Avg", "Fwd Byts/b Avg", "Fwd Pkts/b Avg",
    "Fwd Blk Rate Avg", "Bwd Byts/b Avg", "Bwd Pkts/b Avg", "Bwd Blk Rate Avg",
    "Subflow Fwd Pkts", "Subflow Fwd Byts", "Subflow Bwd Pkts", "Subflow Bwd Byts",
    "Init Fwd Win Byts", "Init Bwd Win Byts", "Fwd Act Data Pkts", "Fwd Seg Size Min",
    "Active Mean", "Active Std", "Active Max", "Active Min", "Idle Mean", "Idle Std",
    "Idle Max", "Idle Min",
]
EXTRA_ID_COLUMNS = ["Flow ID", "Src IP", "Src Port", "Dst IP"]
LABEL_RAW = "Label"

# Identifier-like columns: removed from the feature set to avoid leakage (the model would
# otherwise memorise attacker IPs / ports / attack time windows of the lab network).
ID_COLUMNS = [canon(c) for c in EXTRA_ID_COLUMNS + ["Dst Port", "Timestamp"]]

NUMERIC_COLUMNS = [canon(c) for c in RAW_COLUMNS if c != "Timestamp"]  # 78, includes dst_port
# Candidate model inputs: everything numeric except identifiers. Constant columns are removed
# later, based on the data-quality report (see etl.py -> meta.json).
CANDIDATE_FEATURES = [c for c in NUMERIC_COLUMNS if c not in ID_COLUMNS]

# --------------------------------------------------------------------------------------
# Labels
# --------------------------------------------------------------------------------------

FAMILIES = ["Benign", "DoS", "DDoS", "BruteForce", "WebAttack", "Bot", "Infiltration"]
FAMILY_INDEX = {f: i for i, f in enumerate(FAMILIES)}


def label_family(label: str | None) -> str:
    """Map the 15 raw labels (with their original typos) to 7 families."""
    s = (label or "").strip().lower()
    if s in ("", "benign"):
        return "Benign"
    if "ddos" in s:
        return "DDoS"
    if s.startswith("dos"):
        return "DoS"
    if "xss" in s or "sql" in s or "web" in s:
        return "WebAttack"
    if "brute" in s or "ftp" in s or "ssh" in s:
        return "BruteForce"
    if s.startswith("bot"):
        return "Bot"
    if s.startswith("infil"):
        return "Infiltration"
    return "Other"


def day_from_filename(path: str) -> str:
    """'.../Thuesday-20-02-2018_TrafficForML_CICFlowMeter.csv' -> '2018-02-20'."""
    m = re.search(r"(\d{2})-(\d{2})-(\d{4})", path)
    if not m:
        raise ValueError(f"cannot find a date in {path}")
    d, mo, y = m.groups()
    return f"{y}-{mo}-{d}"


# Features shown first on the alert detail page.
KEY_FEATURES = [
    "protocol", "flow_duration", "tot_fwd_pkts", "tot_bwd_pkts", "totlen_fwd_pkts",
    "totlen_bwd_pkts", "flow_byts_s", "flow_pkts_s", "flow_iat_mean", "fwd_pkt_len_max",
    "syn_flag_cnt", "ack_flag_cnt", "init_fwd_win_byts", "init_bwd_win_byts",
]
