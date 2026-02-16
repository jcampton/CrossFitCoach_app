# 01_Athlete.py
# Athlete-facing Streamlit UI to select Program & Session, view prescribed work, and log actuals (append-only).
# Guard rails: never crash on missing data; append-only CSV; tolerant Program/Session linking; unit + rounding fallbacks.
# Uses OneRMs sheet as the single source of truth for load prescriptions.

from __future__ import annotations

import gcsfs
import io
import json
import re
import uuid
from datetime import datetime
from pathlib import Path
from io import BytesIO
from typing import Union, Optional

import pandas as pd
import streamlit as st
from st_files_connection import FilesConnection

from google.oauth2 import service_account
from coach_io import derive_paths
from coach_io import (
    load_settings,
    pick_series,
    round_to_increment,
    ensure_arrow_compat,
    compute_prescribed_loads,
    build_one_rm_lookup,
)

# ──────────────────────────────────────────────────────────────────────────────
# Cloud / GCS helpers (single source of truth)
# ──────────────────────────────────────────────────────────────────────────────

from google.cloud import storage
from google.oauth2 import service_account
from contextlib import contextmanager

def is_cloud() -> bool:
    try:
        return hasattr(st, "runtime") and st.runtime.exists()
    except Exception:
        return False

def _strip_gcs_scheme(p: str) -> str:
    p = str(p)
    if p.startswith("gs://") or p.startswith("gcs://"):
        return p.split("://", 1)[1]
    return p

def _split_bucket_key(path: str) -> tuple[str, str]:
    """Accepts 'bucket/key' or 'gs://bucket/key'."""
    p = _strip_gcs_scheme(path).lstrip("/")
    if "/" not in p:
        raise ValueError(f"GCS path must be 'bucket/object'. Got: {path}")
    bucket, key = p.split("/", 1)
    return bucket, key

@st.cache_resource(show_spinner=False)
def _gcs_client():
    # secrets must be flat under [connections.gcs] like the JSON key file fields
    sa = dict(st.secrets["connections"]["gcs"])
    creds = service_account.Credentials.from_service_account_info(sa)
    return storage.Client(project=sa.get("project_id"), credentials=creds)

def gcs_exists(path: str) -> bool:
    client = _gcs_client()
    bucket_name, blob_name = _split_bucket_key(path)
    blob = client.bucket(bucket_name).blob(blob_name)
    return blob.exists(client)

def gcs_read_bytes(path: str) -> bytes:
    client = _gcs_client()
    bucket_name, blob_name = _split_bucket_key(path)
    blob = client.bucket(bucket_name).blob(blob_name)
    return blob.download_as_bytes(client=client)

def gcs_write_bytes(path: str, data: bytes, content_type: str | None = None) -> None:
    client = _gcs_client()
    bucket_name, blob_name = _split_bucket_key(path)
    blob = client.bucket(bucket_name).blob(blob_name)
    blob.upload_from_string(data, content_type=content_type)

@contextmanager
def gcs_open(path: str, mode: str = "rb"):
    """
    Minimal file-like interface:
      - rb: returns BytesIO for reading
      - wb/wt/at: buffers then uploads on close
    """
    if "r" in mode:
        bio = BytesIO(gcs_read_bytes(path))
        yield bio
        return

    # write/append modes: read existing if append requested
    buffer = BytesIO()
    if "a" in mode:
        try:
            buffer.write(gcs_read_bytes(path))
        except Exception:
            pass
    yield buffer
    # upload on close
    gcs_write_bytes(path, buffer.getvalue(),
                    content_type="text/csv" if ("t" in mode or "a" in mode) else None)

# ──────────────────────────────────────────────────────────────────────────────
# Page config
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Athlete — Log Training", layout="wide")
st.title("🏋️ Athlete — Log Training")

# ──────────────────────────────────────────────────────────────────────────────
# Read excel from GCS helper
# ──────────────────────────────────────────────────────────────────────────────
def load_excel_tables_any(excel_path):
    """
    Load Excel sheets from local disk (Path/str) or from GCS (cloud).
    Returns dict of sheet_name -> DataFrame
    """
    if is_cloud():
        with gcs_open(str(excel_path), "rb") as f:
            data = f.read()
        xls = pd.ExcelFile(io.BytesIO(data))
        return {s: xls.parse(s) for s in xls.sheet_names}

    # Local path
    p = Path(excel_path) if not isinstance(excel_path, Path) else excel_path
    if not p.exists():
        return {}
    xls = pd.ExcelFile(p)
    return {s: xls.parse(s) for s in xls.sheet_names}

def load_excel_tables_cloudaware(workbook_path: PathLike) -> dict[str, pd.DataFrame]:
    if is_cloud():
        p = _strip_gcs_scheme(str(workbook_path))
        with gcs_open(p, "rb") as f:
            data = f.read()
        xls = pd.ExcelFile(BytesIO(data))
        return {s: xls.parse(s) for s in xls.sheet_names}

# ──────────────────────────────────────────────────────────────────────────────
# Append-only logging helpers
# ──────────────────────────────────────────────────────────────────────────────
def mk_log_id() -> str:
    """Unique LogID: timestamp + short random tail."""
    return f"LOG_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4().int)[:6]}"

_LOG_COLS = [
    "LogID", "SessionID", "MovementID",
    "Sets_Prescribed", "Reps_Prescribed", "Load_Prescribed", "Pct1RM_Prescribed",
    "Sets_Actual", "Reps_Actual", "Load_Actual", "RPE", "Notes", "Timestamp"
]

# Typed map for reading log CSVs safely
LOG_DTYPE_MAP: dict[str, str] = {
    "LogID": "string",
    "SessionID": "string",
    "MovementID": "string",
    "Sets_Prescribed": "Int64",
    "Reps_Prescribed": "Int64",
    "Load_Prescribed": "Float64",
    "Pct1RM_Prescribed": "Float64",
    "Sets_Actual": "Int64",
    "Reps_Actual": "Int64",
    "Load_Actual": "Float64",
    "RPE": "Int64",
    "Notes": "string",
    "Timestamp": "string",  # parse to dt in code where needed
}

def ensure_log_csv(path: PathLike) -> None:
    """Create log CSV if missing (cloud-safe)."""
    cols = _LOG_COLS
    if is_cloud():
        p = _as_text_path(path)
        if not gcs_exists(p):
            with gcs_open(p, "wt") as f:
                pd.DataFrame(columns=cols).to_csv(f, index=False)
        return

    # Local behavior (keep your stronger validation/backups if you want)
    from pathlib import Path
    p = Path(path)
    if not p.exists():
        pd.DataFrame(columns=cols).to_csv(p, index=False)


def append_log_row(path: PathLike, row: dict) -> None:
    """Append one row to log (cloud-safe)."""
    ensure_log_csv(path)
    safe = {col: row.get(col, pd.NA) for col in _LOG_COLS}
    one = pd.DataFrame([safe], columns=_LOG_COLS)

    if is_cloud():
        p = _as_text_path(path)
        # Try text-append; if backend doesn’t support true append, fallback to rewrite.
        try:
            with gcs_open(p, "at") as f:
                one.to_csv(f, header=False, index=False)
            return
        except Exception:
            # Fallback: read + concat + rewrite (OK for small logs)
            with gcs_open(p, "rb") as rf:
                existing = pd.read_csv(rf)
            combined = pd.concat([existing, one], ignore_index=True)
            with gcs_open(p, "wt") as wf:
                combined.to_csv(wf, index=False)
            return

    # Local append
    one.to_csv(path, mode="a", header=False, index=False)

def read_csv_typed(path: PathLike, usecols: Optional[list[str]] = None) -> pd.DataFrame:
    """Read CSV with safe dtypes from local or GCS."""
    ensure_log_csv(path)
    try:
        if usecols is None:
            dtype = {k: v for k, v in LOG_DTYPE_MAP.items() if v != "datetime64[ns]"}
        else:
            dtype = {k: v for k, v in LOG_DTYPE_MAP.items() if k in usecols and v != "datetime64[ns]"}

        if is_cloud():
            with gcs_open(_as_text_path(path), "rb") as f:
                return pd.read_csv(f, usecols=usecols, dtype=dtype)
        else:
            return pd.read_csv(path, usecols=usecols, dtype=dtype)
    except Exception:
        return pd.DataFrame(columns=usecols or _LOG_COLS)

def get_session_logs(path: Path, session_id: str) -> pd.DataFrame:
    """All log rows for a SessionID (sorted by Timestamp if present)."""
    df = read_csv_typed(path)
    if df.empty:
        return df
    if "Timestamp" in df.columns:
        # parse Timestamp into dt for sorting
        dt = pd.to_datetime(df["Timestamp"].astype(str), errors="coerce")
        df = df.assign(_dt=dt).sort_values("_dt").drop(columns=["_dt"])
    if "SessionID" in df.columns:
        return df[df["SessionID"].astype(str) == str(session_id)].copy()
    return pd.DataFrame()

# ──────────────────────────────────────────────────────────────────────────────
# Closed-session registry helpers
# ──────────────────────────────────────────────────────────────────────────────
_CLOSED_COLS = ["SessionID", "ClosedBy", "ClosedAt", "Notes"]

def ensure_closed_csv(path: PathLike) -> None:
    if is_cloud():
        p = _as_text_path(path)
        if not gcs_exists(p):
            with gcs_open(p, "wt") as f:
                pd.DataFrame(columns=_CLOSED_COLS).to_csv(f, index=False)
        return

    from pathlib import Path
    p = Path(path)
    if not p.exists():
        pd.DataFrame(columns=_CLOSED_COLS).to_csv(p, index=False)

def load_closed_sessions(path: PathLike) -> pd.DataFrame:
    ensure_closed_csv(path)
    if is_cloud():
        with gcs_open(_as_text_path(path), "rb") as f:
            return pd.read_csv(f)
    return pd.read_csv(path)

def save_closed_sessions(path: PathLike, df: pd.DataFrame) -> None:
    ensure_closed_csv(path)
    if is_cloud():
        with gcs_open(_as_text_path(path), "wt") as f:
            df.to_csv(f, index=False)
        return
    df.to_csv(path, index=False)

def is_session_closed(path: PathLike, session_id: str) -> bool:
    df = load_closed_sessions(path)
    return session_id in set(df["SessionID"].astype(str))

# ──────────────────────────────────────────────────────────────────────────────
# Small parsing helpers
# ──────────────────────────────────────────────────────────────────────────────
def safe_int(x, default=0):
    try:
        if pd.isna(x):
            return default
        return int(x)
    except Exception:
        return default

def safe_float(x, default=None):
    try:
        if pd.isna(x) or x == "":
            return default
        return float(x)
    except Exception:
        return default

# ──────────────────────────────────────────────────────────────────────────────
# Complex reps parser
# ──────────────────────────────────────────────────────────────────────────────
def parse_complex_reps(val):
    try:
        if pd.isna(val):
            return None, None
    except Exception:
        pass
    s = str(val).strip()
    if not s:
        return None, None

    if s.isdigit():
        return int(s), None

    def _norm(p):
        p = re.sub(r'[^0-9\+]+', '+', p)
        p = re.sub(r'\++', '+', p).strip('+')
        return p

    m = re.match(r'^\s*(\d+)\s*(?:x|×)?\s*[\(\[]\s*([0-9\+\s,]+)\s*[\)\]]\s*$', s)
    if m:
        return int(m.group(1)), _norm(m.group(2))

    m = re.match(r'^\s*(\d+)\s*(?:x|×)\s*([0-9\+\s,]+)\s*$', s)
    if m:
        return int(m.group(1)), _norm(m.group(2))

    if re.match(r'^\s*[0-9\+\s,]+\s*$', s):
        return None, _norm(s)

    return None, None

# ──────────────────────────────────────────────────────────────────────────────
# Movement history helpers (cached)
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def _load_logs_minimal(path: Path) -> pd.DataFrame:
    """Load minimal columns once; cached for speed."""
    cols = ["MovementID","Sets_Actual","Reps_Actual","Load_Actual","RPE","Notes","Timestamp"]
    df = read_csv_typed(path, usecols=cols)
    if df.empty:
        return pd.DataFrame(columns=cols + ["dt"])
    df["dt"] = pd.to_datetime(df["Timestamp"].astype(str), errors="coerce")
    df = df.dropna(subset=["dt"])
    df["MovementID"] = df["MovementID"].astype("string")
    return df

def _render_last_n_logs_for_movement(logs_df: pd.DataFrame, movement_id: str, n: int = 3) -> None:
    if logs_df.empty:
        st.caption("History: none yet")
        return
    sub = logs_df[logs_df["MovementID"] == str(movement_id)].sort_values("dt", ascending=False).head(n)
    if sub.empty:
        st.caption("History: none yet")
        return

    lines = []
    for _, r in sub.iterrows():
        date = r["dt"].strftime("%d/%m")
        sets = safe_int(r.get("Sets_Actual"), 0)
        reps = safe_int(r.get("Reps_Actual"), 0)
        load = r.get("Load_Actual")
        load_str = f" @ {float(load):g}" if pd.notna(load) else ""

        rpe_val = r.get("RPE")
        rpe_str = f"  • RPE {int(rpe_val)}" if pd.notna(rpe_val) and safe_int(rpe_val, 0) > 0 else ""

        note_val = r.get("Notes", "")
        if pd.isna(note_val):
            note = ""
        else:
            note = str(note_val).strip()
            if note.lower() in ("nan", "none"):
                note = ""
        note_str = f"  • {note[:80]}" if note else ""

        lines.append(f"- {date}  • {sets}×{reps}{load_str}{rpe_str}{note_str}")

    st.markdown("**History (last 3):**\n" + "\n".join(lines))

# ──────────────────────────────────────────────────────────────────────────────
# Completion / progress helpers
# ──────────────────────────────────────────────────────────────────────────────
def build_completion_map(log_path: Path) -> dict[str, str]:
    """SessionID -> last log date (dd/mm/YYYY) for marking sessions as complete."""
    df = read_csv_typed(log_path, usecols=["SessionID", "Timestamp"])
    if df.empty or "SessionID" not in df.columns:
        return {}
    df["SessionID"] = df["SessionID"].astype("string")
    df["dt"] = pd.to_datetime(df["Timestamp"].astype(str), errors="coerce")
    df = df.dropna(subset=["SessionID", "dt"])
    if df.empty:
        return {}
    last = df.groupby("SessionID")["dt"].max()
    return {sid: d.strftime("%d/%m/%Y") for sid, d in last.items()}

def program_progress_for_selected(
    sel_join: str,
    progs_df: pd.DataFrame,
    sess_df: pd.DataFrame,
    log_path: Path,
) -> dict:
    """
    Return dict with completed, total, percent for the selected program.
    Hardening goals:
      - Never raise NameError/UnboundLocalError
      - Always return a dict (never None)
      - Tolerate missing columns (_JOIN, _pname, ProgramCode, SessionID)
    """

    # ---- logs: always define log_sids ----
    log_sids: set[str] = set()
    try:
        logs = read_csv_typed(log_path, usecols=["SessionID"])
        if not logs.empty and "SessionID" in logs.columns:
            log_sids = set(logs["SessionID"].dropna().astype(str))
    except Exception:
        log_sids = set()

    # ---- sessions subset: always define sess_sel + total ----
    try:
        if "_JOIN" in sess_df.columns:
            sess_sel = sess_df[sess_df["_JOIN"] == sel_join]
        else:
            sess_sel = sess_df
        total = int(len(sess_sel))
    except Exception:
        sess_sel = sess_df
        total = int(len(sess_df)) if sess_df is not None else 0

    if total <= 0:
        return {"completed": 0, "total": 0, "percent": 0}

    # ---- primary path: SessionID present in Sessions sheet ----
    completed = 0
    try:
        if "SessionID" in sess_sel.columns and sess_sel["SessionID"].notna().any():
            known = set(sess_sel["SessionID"].dropna().astype(str))
            completed = len(log_sids & known)
        else:
            # ---- fallback path: prefix match using derived ProgramCode ----
            # Ensure _pname exists for mapping (don't mutate original unless needed)
            progs_tmp = progs_df.copy()
            if "_pname" not in progs_tmp.columns:
                # minimal safe normalization
                progs_tmp["_pname"] = (
                    progs_tmp.get("Name", pd.Series([None] * len(progs_tmp)))
                    .astype(str).str.strip().str.lower()
                    .str.replace(r"[^a-z0-9]+", "_", regex=True)
                    .str.replace(r"_+", "_", regex=True)
                    .str.strip("_")
                )

            def _derive_progcode(row) -> str:
                pc = row.get("ProgramCode")
                if isinstance(pc, str) and pc.strip():
                    return pc.strip()
                nm = str(row.get("Name", "PRG")).upper().strip()
                s = re.sub(r"[^A-Z0-9]+", "-", nm)
                s = re.sub(r"-+", "-", s).strip("-")
                return s[:24] if s else "PRG"

            progs_tmp["_ProgCode_fallback"] = progs_tmp.apply(_derive_progcode, axis=1)

            # Build a JOIN->progcode mapping safely
            join_to_progcode = {}
            if "_JOIN" in progs_tmp.columns:
                # If _JOIN exists, prefer it
                for _, r in progs_tmp[["_JOIN", "_ProgCode_fallback"]].dropna().drop_duplicates().iterrows():
                    join_to_progcode[str(r["_JOIN"])] = str(r["_ProgCode_fallback"])
            else:
                # Otherwise fall back to pname mapping
                for _, r in progs_tmp[["_pname", "_ProgCode_fallback"]].dropna().drop_duplicates().iterrows():
                    join_to_progcode[str(r["_pname"])] = str(r["_ProgCode_fallback"])

            progcode = join_to_progcode.get(str(sel_join), "PRG")
            prefix = f"{progcode}-"  # <-- prefix is ALWAYS defined here
            completed = sum(1 for sid in log_sids if str(sid).startswith(prefix))
    except Exception:
        # Worst case: don’t crash the app; show 0 completed.
        completed = 0

    percent = int(round((completed / total) * 100)) if total else 0
    return {"completed": completed, "total": total, "percent": percent}

# ──────────────────────────────────────────────────────────────────────────────
# Load settings, resolve paths, and read tables
# ──────────────────────────────────────────────────────────────────────────────
cfg = load_settings("data/settings.json")
paths = derive_paths(cfg)

_raw = load_excel_tables_cloudaware(paths["excel"])
tables = {
    "programs":          _raw.get(cfg["sheets"]["programs"], pd.DataFrame()),
    "sessions":          _raw.get(cfg["sheets"]["sessions"], pd.DataFrame()),
    "session_movements": _raw.get(cfg["sheets"]["session_movements"], pd.DataFrame()),
    "movement_library":  _raw.get(cfg["sheets"]["movements"], pd.DataFrame()),
    # OneRMs (single source for prescriptions)
    "one_rms":           _raw.get(cfg["sheets"]["one_rms"], pd.DataFrame()),
}

progs = tables["programs"].copy()
sess  = tables["sessions"].copy()
movs  = tables["session_movements"].copy()
lib   = tables["movement_library"].copy()
one_rms_df = tables["one_rms"].copy()

# Guard: missing core sheets
if progs.empty or sess.empty or movs.empty:
    st.error("Missing required sheets. Ensure your workbook has 'Programs', 'Sessions', and 'SessionMovements'.")
    st.stop()

# Ensure CSVs exist / are well-formed
if not is_cloud():
    ensure_log_csv(paths["log_csv"])
    ensure_closed_csv(paths["closed_csv"])

# Sidebar: quick settings + display preferences
with st.sidebar:
#    st.subheader("Settings")
#    st.code(json.dumps({
#        "excel_path": str(paths["excel"]),
#        "log_csv": str(paths["log_csv"]),
#        "closed_csv": str(paths["closed_csv"]),
#        "sheets": cfg.get("sheets", {})
#    }, indent=2), language="json")
#    st.caption("Edit data/settings.json to change the Excel path / sheet names. "
#               "The log and closed-session CSVs live next to the workbook.")

    disp_cfg = cfg.get("display", {})
    round_inc = disp_cfg.get("round_increment", 2.5)
    default_unit = disp_cfg.get("default_unit", "kg")

# Sanitize display options
try:
    round_inc = float(round_inc)
    if round_inc <= 0 or pd.isna(round_inc):
        round_inc = 0.0  # gracefully skip rounding
except Exception:
    round_inc = 0.0

default_unit = (str(default_unit).strip() or "kg")

# ──────────────────────────────────────────────────────────────────────────────
# Program / Session selectors (robust, with synthetic SessionID fallback)
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("Select Program & Session")

def _norm_text(x):
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    s = str(x).strip().lower()
    s = re.sub(r'[^a-z0-9]+', '_', s)
    s = re.sub(r'_+', '_', s).strip('_')
    return s

# Programs: normalised name
progs["_pname"] = progs.get("Name", pd.Series([None]*len(progs))).apply(_norm_text)

# Sessions: try to extract program name from ProgramID like 'PRG_HSPU_8W_20250810_001'
sess["_pname_from_pid"] = sess.get("ProgramID", pd.Series([None]*len(sess))).astype(str).str.strip().str.lower()
sess["_pname_from_pid"] = sess["_pname_from_pid"] \
    .str.replace(r'^prg_', '', regex=True) \
    .str.replace(r'_\d{8}(_\d+)?$', '', regex=True) \
    .str.replace(r'[^a-z0-9]+', '_', regex=True) \
    .str.strip('_')

# Default join key = program name
progs["_JOIN"] = progs["_pname"]
sess["_JOIN"]  = sess["_pname_from_pid"]

# If both sides have usable ProgramID values that overlap, prefer ID join
if "ProgramID" in progs.columns and progs["ProgramID"].notna().any() and sess["ProgramID"].notna().any():
    progs["_pid_norm"] = progs["ProgramID"].astype(str).str.strip().str.lower()
    sess["_pid_norm"]  = sess["ProgramID"].astype(str).str.strip().str.lower()
    if set(progs["_pid_norm"]) & set(sess["_pid_norm"]):
        progs["_JOIN"] = progs["_pid_norm"]
        sess["_JOIN"]  = sess["_pid_norm"]

# ── NEW: Filter programs by Status only (dates ignored)
ACTIVE_STATUSES = {s.upper().strip() for s in cfg.get("program_active_statuses", ["ACTIVE", "IN PROGRESS"])}

if 'show_active_programs_only' not in st.session_state:
    st.session_state['show_active_programs_only'] = True  # default ON

st.caption("Filters") #Title for filters

show_active_only = st.checkbox(
    "Show active programs only",
    value=st.session_state['show_active_programs_only'],
    help=f"Active when Programs[Status] is in {sorted(ACTIVE_STATUSES)}"
)

st.session_state['show_active_programs_only'] = show_active_only

progs_view = progs.copy()
if show_active_only:
    if "Status" in progs_view.columns:
        status_norm = progs_view["Status"].astype(str).str.upper().str.strip()
        mask = status_norm.isin(ACTIVE_STATUSES)
        if mask.any():
            progs_view = progs_view[mask]
        else:
            st.info("No programs have an active Status. Showing all programs for now.")
            progs_view = progs.copy()
    else:
        st.info("Programs sheet has no 'Status' column. Showing all programs.")
        progs_view = progs.copy()

hide_completed = st.checkbox(
    "Hide completed sessions",
    value=st.session_state.get("hide_completed", True),
    key="hide_completed",
    help="Hide sessions that already have at least one log."
)

# Program dropdown (plain label; badge below)
progs["Label"] = progs.get("Name", progs.get("ProgramCode", progs.get("ProgramID", ""))).astype(str)
progs_view["Label"] = progs_view.get("Name", progs_view.get("ProgramCode", progs_view.get("ProgramID", ""))).astype(str)

prog_options_src = progs_view if not progs_view.empty else progs
if prog_options_src.empty:
    st.error("Programs sheet has no identifiable programs (no Name/ProgramID).")
    st.stop()

prog_options = prog_options_src[["_JOIN", "Label"]].dropna().drop_duplicates().reset_index(drop=True)

sel_prog_label = st.selectbox("Program", options=prog_options["Label"].tolist())
sel_prog_join  = prog_options.loc[prog_options["Label"] == sel_prog_label, "_JOIN"].iloc[0]

# Optional caption of visible programs
st.caption("Visible programs: " + ", ".join(prog_options["Label"].astype(str).tolist()))

# Program progress badge
prog_prog = program_progress_for_selected(sel_prog_join, progs, sess, paths["log_csv"])
badge_text = f"**Completed:** {prog_prog['completed']} / {prog_prog['total']} ({prog_prog['percent']}%)"
st.markdown(badge_text)
st.progress(prog_prog["percent"] / 100 if prog_prog["total"] else 0.0)

# Sessions for the chosen program
sess_prog = sess[sess["_JOIN"] == sel_prog_join].copy()

# Fallback: still nothing? Let you pick any key present in Sessions
if sess_prog.empty:
    st.warning("No sessions matched that Program. Using program keys found in the Sessions sheet so you can continue.")
    sess_keys = sorted(sess["_JOIN"].dropna().unique().tolist())
    if not sess_keys:
        st.error("Your Sessions sheet has no rows. Add sessions in Excel.")
        st.stop()
    sel_prog_join = st.selectbox("Program (from Sessions)", options=sess_keys, key="alt_prog_select")
    sess_prog = sess[sess["_JOIN"] == sel_prog_join].copy()

# ── Ensure we have a SessionID (synthesize if blank): <ProgramCode>-<SessionCode>-<seq2>
def _derive_progcode(row) -> str:
    pc = row.get("ProgramCode")
    if isinstance(pc, str) and pc.strip():
        return pc.strip()
    nm = str(row.get("Name", "PRG")).upper().strip()
    s = re.sub(r'[^A-Z0-9]+', '-', nm)
    s = re.sub(r'-+', '-', s).strip('-')
    return s[:24] if s else "PRG"

progs["_ProgCode_fallback"] = progs.apply(_derive_progcode, axis=1)
name_to_progcode = dict(zip(progs["_pname"], progs["_ProgCode_fallback"]))

if "SessionID" not in sess_prog.columns or sess_prog["SessionID"].isna().all():
    sess_prog = sess_prog.copy()
    if "_pname" not in sess_prog.columns or sess_prog["_pname"].isna().all():
        sess_prog["_pname"] = sess_prog["_JOIN"]  # when JOIN is name-based
    sess_prog["ProgramCode_synth"] = sess_prog["_pname"].map(name_to_progcode).fillna("PRG")
    if "Date" in sess_prog.columns:
        sess_prog = sess_prog.sort_values("Date").reset_index(drop=True)
    else:
        sess_prog = sess_prog.reset_index(drop=True)
    n_codes = max(1, int((sess_prog.get("SessionCode", pd.Series(dtype=object)).dropna().nunique()) or 3))
    seq_idx = (pd.Series(range(len(sess_prog))) // n_codes) + 1
    sess_prog["_seq2"] = seq_idx.astype(int)
    def _mk_sid(r):
        scode = str(r.get("SessionCode", "S")).strip()
        return f"{r['ProgramCode_synth']}-{scode}-{int(r['_seq2']):02d}"
    sess_prog["SessionID"] = sess_prog.apply(_mk_sid, axis=1)

# Human-friendly session label
if "SessionLabel" in sess_prog.columns and sess_prog["SessionLabel"].notna().any():
    sess_prog["Label"] = sess_prog["SessionLabel"].astype(str)
elif "SessionID" in sess_prog.columns:
    sess_prog["Label"] = sess_prog["SessionID"].astype(str)
else:
    sess_prog["Label"] = sess_prog.apply(lambda r: f"{r.get('SessionCode','?')} — {r.get('Date','')}", axis=1)

# Completion badge + display label
completion_map = build_completion_map(paths["log_csv"])
sess_prog["CompletedOn"] = sess_prog["SessionID"].astype(str).map(completion_map)
sess_prog["Completed"] = sess_prog["CompletedOn"].notna() & (sess_prog["CompletedOn"] != "")
sess_prog["LabelDisplay"] = sess_prog.apply(
    lambda r: f"{r['Label']} — ✓ Complete ({r['CompletedOn']})" if r["Completed"] else r["Label"],
    axis=1
)

# Hide completed sessions if chosen
if hide_completed:
    sess_prog = sess_prog[~sess_prog["Completed"]].copy()
    if sess_prog.empty:
        st.info("All sessions in this program are complete. Uncheck “Hide completed sessions” in the sidebar to show them.")
        st.stop()

# Gentle sort (no custom sorting by completion; keep original date/order)
if "Date" in sess_prog.columns:
    sess_prog["_sort_date"] = pd.to_datetime(sess_prog["Date"], errors="coerce")
    sort_cols = ["_sort_date"]
    if "SessionOrder" in sess_prog.columns: sort_cols.append("SessionOrder")
    if "SessionCode" in sess_prog.columns:  sort_cols.append("SessionCode")
    sess_prog = sess_prog.sort_values(sort_cols, na_position="last").drop(columns=["_sort_date"], errors="ignore")
elif "SessionOrder" in sess_prog.columns:
    sess_prog = sess_prog.sort_values(["SessionOrder", "SessionCode"] if "SessionCode" in sess_prog.columns else ["SessionOrder"])

# Session picker
sel_sess_label = st.selectbox("Session", options=sess_prog["LabelDisplay"].tolist())
sel_sess = sess_prog.loc[sess_prog["LabelDisplay"] == sel_sess_label, "SessionID"].iloc[0]

# ──────────────────────────────────────────────────────────────────────────────
# Closed/open banner + toggle actions
# ──────────────────────────────────────────────────────────────────────────────
this_closed = is_session_closed(paths["closed_csv"], sel_sess)
if this_closed:
    st.info("🔒 This session is **closed**. Logging is disabled.")
else:
    st.success("✅ This session is **open** for logging.")

left, right = st.columns(2)
with left:
    if not this_closed and st.button("🔒 Close session"):
        df = load_closed_sessions(paths["closed_csv"])
        df = pd.concat([df, pd.DataFrame([{
            "SessionID": sel_sess,
            "ClosedBy": "User",  # TODO: replace with real username for multi-user
            "ClosedAt": datetime.now().isoformat(timespec="seconds"),
            "Notes": ""
        }])], ignore_index=True)
        save_closed_sessions(paths["closed_csv"], df)
        st.experimental_rerun()
with right:
    if this_closed and st.button("🔓 Reopen session"):
        df = load_closed_sessions(paths["closed_csv"])
        df = df[df["SessionID"].astype(str) != str(sel_sess)]
        save_closed_sessions(paths["closed_csv"], df)
        st.experimental_rerun()

# ──────────────────────────────────────────────────────────────────────────────
# Prescribed movements for the selected session
# ──────────────────────────────────────────────────────────────────────────────
target_sid_norm = str(sel_sess).strip().lower()
movs_sid_norm = movs.get("SessionID", pd.Series([""]*len(movs))).astype(str).str.strip().str.lower()
presc = movs[movs_sid_norm == target_sid_norm].copy()

if presc.empty:
    st.warning("No prescribed work found for this session.")
    # Still show historical logs below for context
    logs_df = get_session_logs(paths["log_csv"], sel_sess)
    st.markdown("### Logged entries for this session")
    if logs_df.empty:
        st.info("No logs yet for this session.")
    else:
        view_cols = [
            "Timestamp","MovementID",
            "Sets_Prescribed","Reps_Prescribed","Load_Prescribed","Pct1RM_Prescribed",
            "Sets_Actual","Reps_Actual","Load_Actual","RPE","Notes"
        ]
        existing = [c for c in view_cols if c in logs_df.columns]
        st.dataframe(ensure_arrow_compat(logs_df[existing]), use_container_width=True)
    st.stop()

# ──────────────────────────────────────────────────────────────────────────────
# Enrich with Movement Library names/units (needed before compute)
# ──────────────────────────────────────────────────────────────────────────────
if not lib.empty and "MovementID" in lib.columns:
    lib_cols = ["MovementID"]
    if "Name" in lib.columns: lib_cols.append("Name")
    if "Unit" in lib.columns: lib_cols.append("Unit")
    lib_slim = lib[lib_cols].drop_duplicates(subset="MovementID")
    presc = presc.merge(lib_slim, on="MovementID", how="left", suffixes=("", "_Lib"))
    presc["MovementName"] = presc.get("Name", presc.get("MovName", presc["MovementID"]))
    if "Unit" in presc.columns and "Unit_Lib" in presc.columns:
        presc["Unit"] = presc["Unit"].fillna(presc["Unit_Lib"])
else:
    presc["MovementName"] = presc.get("MovName", presc["MovementID"])

# Unit fallback (so compute knows if the row is load-bearing)
if "Unit" not in presc.columns:
    presc["Unit"] = default_unit
else:
    presc["Unit"] = presc["Unit"].fillna(default_unit).replace("", default_unit)

# ──────────────────────────────────────────────────────────────────────────────
# Compute prescribed loads (manual Load wins; otherwise %1RM × 1RM lookup)
# ──────────────────────────────────────────────────────────────────────────────
one_rm_lookup = build_one_rm_lookup(one_rms_df) if isinstance(one_rms_df, pd.DataFrame) else {}

def _normalize_pct(p):
    """
    Accepts 0.65, 65, or '65%'. Returns fraction (0.65) or None.
    """
    if p is None:
        return None
    try:
        if pd.isna(p):
            return None
    except Exception:
        pass

    s = str(p).strip()
    if not s:
        return None
    try:
        if s.endswith("%"):
            v = float(s[:-1]) / 100.0
        else:
            v = float(s)
            if v > 1.5:   # treat 65 as 65%
                v = v / 100.0
        return v if v >= 0 else None
    except Exception:
        return None

# Pull %1RM and Load from the session movements table (your “programming CSV/sheet”)
_pct_raw = pick_series(presc, ["%1RM", "Percent1RM", "Pct1RM"])
_load_raw = pick_series(presc, ["Load", "Prescribed Load", "Target Load"])

# Normalize %1RM → fraction for logging + compute
presc["Pct1RM_Prescribed"] = _pct_raw.apply(_normalize_pct)

# Compute per-row prescribed load:
# 1) If manual Load > 0 → use it
# 2) Else if %1RM present and 1RM exists → compute
# 3) Else → blank
def _resolve_load(row):
    # Manual load wins
    try:
        manual = row.get("_ManualLoad", None)
        if manual is not None and pd.notna(manual) and float(manual) > 0:
            return float(manual), "Manual Load"
    except Exception:
        pass

    pct = row.get("Pct1RM_Prescribed", None)
    if pct is None or (isinstance(pct, float) and pd.isna(pct)):
        return None, "No %1RM"

    mv = str(row.get("MovementID") or "").strip()
    base_1rm = one_rm_lookup.get(mv, None)
    if base_1rm is None or pd.isna(base_1rm) or float(base_1rm) <= 0:
        return None, "No 1RM"

    try:
        return float(base_1rm) * float(pct), f"%1RM × 1RM ({mv})"
    except Exception:
        return None, "Bad inputs"

# Coerce manual load numeric (so comparisons work)
presc["_ManualLoad"] = pd.to_numeric(_load_raw, errors="coerce")

resolved = presc.apply(_resolve_load, axis=1, result_type="expand")
presc["Load_Prescribed"] = resolved[0]
presc["Load_Source"] = resolved[1]  # optional diagnostic column (safe to keep or remove)

# Round for UI / prefill (only if we have a number)
def _round_if(x):
    try:
        if x is None or pd.isna(x):
            return None
        return round_to_increment(float(x), round_inc) if round_inc > 0 else float(x)
    except Exception:
        return None

presc["Load_Prescribed_Rounded"] = presc["Load_Prescribed"].apply(_round_if)

# Cleanup helper column (optional)
presc = presc.drop(columns=["_ManualLoad"], errors="ignore")

# ──────────────────────────────────────────────────────────────────────────────
# Diagnostics (Session-level)
# ──────────────────────────────────────────────────────────────────────────────
with st.expander("🔎 Diagnostics – Session data health", expanded=False):
    st.write("**SessionMovements columns (all):**", list(movs.columns))
    st.write("**SessionMovements columns (this session):**", list(presc.columns))

    calc_candidates = ["FinalLoad", "Final Load", "CalcLoad", "Calculated Load", "Prescribed Load", "TargetLoad", "Target Load", "Load"]
    calc_series = pick_series(presc, calc_candidates)
    n_rows = len(presc)
    n_calc_blank = int(calc_series.isna().sum() + (calc_series == "").sum())
    st.write(f"**Load-like column candidates:** {calc_candidates}")
    st.write(f"**Rows in session:** {n_rows}  |  **Load blanks:** {n_calc_blank}")

    pct_candidates = ["%1RM", "Percent1RM", "Pct1RM"]
    pct_series = pick_series(presc, pct_candidates)
    n_pct_present = int(pct_series.notna().sum())
    st.write(f"**%1RM-like column candidates:** {pct_candidates}  |  **%1RM present rows:** {n_pct_present}")

    if not lib.empty and "MovementID" in lib.columns:
        known_mids = set(lib["MovementID"].astype(str))
        missing_in_lib = presc[~presc["MovementID"].astype(str).isin(known_mids)]
        if not missing_in_lib.empty:
            st.error(f"{len(missing_in_lib)} movements in this session are not found in MovementLibrary.")
            st.dataframe(ensure_arrow_compat(missing_in_lib[["MovementID"]].drop_duplicates()),
                         use_container_width=True)
        else:
            st.success("All MovementIDs in this session exist in MovementLibrary.")
    else:
        st.info("MovementLibrary sheet missing/empty, skipping MovementID coverage check.")

    if this_closed and not get_session_logs(paths["log_csv"], sel_sess).empty:
        st.info("This session is currently closed but has existing logs. That’s OK (historical), just a heads-up.")

# Inline coverage guard
if not lib.empty and "MovementID" in lib.columns:
    known_mids = set(lib["MovementID"].astype(str))
    _missing = presc[~presc["MovementID"].astype(str).isin(known_mids)]
    if not _missing.empty:
        st.error(
            f"{_missing['MovementID'].nunique()} MovementID(s) in this session "
            "are not found in MovementLibrary. Please fix before logging."
        )
        st.dataframe(
            ensure_arrow_compat(_missing[["MovementID"]].drop_duplicates().reset_index(drop=True)),
            use_container_width=True
        )

# ──────────────────────────────────────────────────────────────────────────────
# 📋 Prescribed Work (display)
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("📋 Prescribed Work")
display = presc.copy()
for dup in ["Load", "%1RM"]:
    if dup in display.columns:
        display = display.drop(columns=[dup])
display = display.rename(columns={
    "Load_Prescribed_Rounded": "Load",
    "Pct1RM_Prescribed": "%1RM",
})
cols_to_show = [c for c in [
    "MovementID", "MovementName", "Sets", "Reps",
    "Load", "Notes"
] if c in display.columns]
st.dataframe(ensure_arrow_compat(display[cols_to_show].reset_index(drop=True)), use_container_width=True)

# ──────────────────────────────────────────────────────────────────────────────
# ✍️ Log Actuals UI (single "Submit session" button)
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("✍️ Log Actuals")
logs_df = get_session_logs(paths["log_csv"], sel_sess)

# ✅ Stabilize order so the index is deterministic
presc = presc.reset_index(drop=True)

if this_closed:
    st.info("This session is closed. Logging inputs are disabled. Reopen above if needed.")
else:
    with st.form(f"log_form_{sel_sess}", clear_on_submit=False):
        entries = []

        _hist_df = _load_logs_minimal(paths["log_csv"])

        for i, (_, rx) in enumerate(presc.iterrows(), start=1):
            mv_id = rx["MovementID"]
            mv_name = rx.get("MovementName", mv_id)

            sets_p = safe_int(rx.get("Sets"), 0)

            cycles_p, complex_pat = parse_complex_reps(rx.get("Reps"))
            if complex_pat and cycles_p is None:
                reps_p = 1
            else:
                reps_p = cycles_p if cycles_p is not None else safe_int(rx.get("Reps"), 0)

            # Use computed FinalLoad as the prescribed default
            load_p = safe_float(rx.get("Load_Prescribed_Rounded"), None)

            # Use session-entered % if present, else the PercentUsed from compute
            pct_p = rx.get("Pct1RM_Prescribed", rx.get("PercentUsed", None))
            unit   = (rx.get("Unit") or default_unit) if "Unit" in rx else default_unit

            with st.expander(f"{mv_id} — {mv_name}", expanded=False):
                c1, c2, c3, c4, c5 = st.columns([1, 1, 1, 1, 2])

                sets_a = c1.number_input(
                    "Sets (actual)",
                    min_value=0, step=1, value=sets_p,
                    key=f"sets_{sel_sess}_{mv_id}_{i}"
                )

                reps_label = "Reps (actual cycles)" if complex_pat else "Reps (actual)"
                reps_a = c2.number_input(
                    reps_label,
                    min_value=0, step=1, value=reps_p,
                    key=f"reps_{sel_sess}_{mv_id}_{i}"
                )
                if complex_pat:
                    try:
                        parts = [int(x) for x in complex_pat.split('+') if x.strip().isdigit()]
                        total_parts = sum(parts) if parts else None
                    except Exception:
                        total_parts = None
                    suffix = f" (per set: {total_parts} parts)" if total_parts else ""
                    c2.caption(f"Complex: {complex_pat}{suffix}")

                load_a = c3.number_input(
                    f"Load (actual) [{unit}]",
                    min_value=0.0,
                    step=round_inc if round_inc > 0 else 1.0,
                    value=float(load_p) if load_p is not None and not pd.isna(load_p) else 0.0,
                    key=f"load_{sel_sess}_{mv_id}_{i}",
                )
                rpe_a  = c4.slider("RPE", min_value=1, max_value=10, value=6, key=f"rpe_{sel_sess}_{mv_id}_{i}")
                notes  = c5.text_input("Notes", value="", key=f"notes_{sel_sess}_{mv_id}_{i}")

                st.divider()
                _render_last_n_logs_for_movement(_hist_df, mv_id, n=3)

                entries.append({
                    "mv_id": mv_id,
                    "mv_name": mv_name,
                    "sets_p": sets_p,
                    "reps_p": reps_p,      # store cycles when complex (pattern-only defaults to 1)
                    "load_p": load_p,
                    "pct_p":  pct_p,
                    "unit":   unit,
                    "sets_a": sets_a,
                    "reps_a": reps_a,      # user enters cycles
                    "load_a": load_a,
                    "rpe_a":  rpe_a,
                    "notes":  notes,
                })

        submitted = st.form_submit_button("💾 Submit session")

    if submitted:
        if is_session_closed(paths["closed_csv"], sel_sess):
            st.warning("This session was closed before saving. Reopen it above to add logs.")
        else:
            rows_to_save = []
            for e in entries:
                has_data = (e["sets_a"] or e["reps_a"] or e["load_a"] or (e["notes"] and e["notes"].strip()))
                if not has_data:
                    continue

                actual_value = float(e["load_a"])
                if round_inc > 0:
                    actual_value = round_to_increment(actual_value, round_inc)

                rows_to_save.append({
                    "LogID": mk_log_id(),
                    "SessionID": sel_sess,
                    "MovementID": e["mv_id"],
                    "Sets_Prescribed": e["sets_p"] if e["sets_p"] else pd.NA,
                    "Reps_Prescribed": e["reps_p"] if e["reps_p"] else pd.NA,
                    "Load_Prescribed": e["load_p"] if e["load_p"] is not None and not pd.isna(e["load_p"]) else pd.NA,
                    "Pct1RM_Prescribed": e["pct_p"] if e["pct_p"] is not None and not pd.isna(e["pct_p"]) else pd.NA,
                    "Sets_Actual": int(e["sets_a"]),
                    "Reps_Actual": int(e["reps_a"]),
                    "Load_Actual": actual_value,
                    "RPE": int(e["rpe_a"]),
                    "Notes": e["notes"],
                    "Timestamp": datetime.now().isoformat(timespec="seconds"),
                })

            if not rows_to_save:
                st.info("Nothing to save — enter at least one set/reps/load or a note.")
            else:
                for row in rows_to_save:
                    append_log_row(paths["log_csv"], row)
                st.success(f"Saved {len(rows_to_save)} entr{'y' if len(rows_to_save)==1 else 'ies'} ✅")
                logs_df = get_session_logs(paths["log_csv"], sel_sess)

# ──────────────────────────────────────────────────────────────────────────────
# Logged entries table
# ──────────────────────────────────────────────────────────────────────────────
st.markdown("### Logged entries for this session")
logs_df = get_session_logs(paths["log_csv"], sel_sess)
if logs_df.empty:
    st.info("No logs yet for this session.")
else:
    view_cols = [
        "Timestamp", "MovementID",
        "Sets_Prescribed", "Reps_Prescribed", "Load_Prescribed", "Pct1RM_Prescribed",
        "Sets_Actual", "Reps_Actual", "Load_Actual", "RPE", "Notes"
    ]
    existing = [c for c in view_cols if c in logs_df.columns]
    st.dataframe(ensure_arrow_compat(logs_df[existing]), use_container_width=True)

st.caption("The app **never overwrites** your planning sheets. Actuals are appended to a CSV next to your Excel file.")
