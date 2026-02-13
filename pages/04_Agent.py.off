# 04_Agent.py
# Coach Agent MVP: reads logs + sheets, applies heuristics, suggests tweaks.

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from datetime import datetime
import numpy as np
import pandas as pd
import streamlit as st

# Shared helpers
from coach_io import (
    load_settings,
    derive_paths,
    load_excel_tables,
    read_log,
    pick_series,
)

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="CrossFit AI Coach – Agent", page_icon="🤖", layout="wide")
st.title("🤖 Coach Agent — Training Review & Suggestions")

# ── Load config/paths/sheets ──────────────────────────────────────────────────
cfg = load_settings("data/settings.json")
paths = derive_paths(cfg)

_raw = load_excel_tables(paths["excel"])
sheet_programs = cfg["sheets"]["programs"]
sheet_sessions = cfg["sheets"]["sessions"]
sheet_session_movs = cfg["sheets"]["session_movements"]
sheet_mov_lib = cfg["sheets"]["movement_library"]

progs = _raw.get(sheet_programs, pd.DataFrame()).copy()
sess  = _raw.get(sheet_sessions, pd.DataFrame()).copy()
presc = _raw.get(sheet_session_movs, pd.DataFrame()).copy()
lib   = _raw.get(sheet_mov_lib, pd.DataFrame()).copy()

try:
    logs = read_log(paths["log_csv"])
except Exception:
    logs = pd.DataFrame()

with st.sidebar:
    st.subheader("Agent Settings")
    review_weeks   = st.slider("Review window (weeks)", 4, 16, 8)
    min_weeks_trend = st.slider("Min weeks for trend check", 3, review_weeks, 4)
    stall_pct      = st.slider("Stall threshold (tonnage change %)", 5, 40, 15) / 100.0
    fatigue_rpe    = st.slider("High fatigue avg RPE", 6, 10, 8)
    fatigue_drop   = st.slider("Tonnage drop % (with RPE↑)", 5, 50, 20) / 100.0
    low_adherence  = st.slider("Low adherence %", 30, 100, 60)
    focus_min_weeks = st.slider("Min weeks to judge focus areas", 3, review_weeks, 4)

    st.caption("Tune thresholds. The agent uses simple rules; we can evolve to ML later.")

# ── Guard rails ───────────────────────────────────────────────────────────────
if progs.empty or sess.empty or presc.empty:
    st.info("Need Programs, Sessions, and SessionMovements sheets to analyze. Check your workbook.")
    st.stop()

if logs.empty or "SessionID" not in logs.columns:
    st.info("No logs yet. Log a few sessions in Athlete page, then revisit the Agent.")
    st.stop()

# ── Normalize & join basics ───────────────────────────────────────────────────
# Sessions join
sess_cols = ["SessionID"]
for c in ["Date", "ProgramID", "SessionOrder", "SessionLabel"]:
    if c in sess.columns: sess_cols.append(c)
sess_slim = sess[sess_cols].drop_duplicates("SessionID")

df = logs.merge(sess_slim, on="SessionID", how="left")
# Bring movement names / units (optional)
if not lib.empty and "MovementID" in lib.columns:
    lib_slim = lib[["MovementID"] + [c for c in ["Name", "Unit", "Category", "Modality"] if c in lib.columns]].drop_duplicates("MovementID")
    df = df.merge(lib_slim, on="MovementID", how="left")

# Prescribed (to measure adherence)
presc_slim = presc[["SessionID","MovementID"]].drop_duplicates()

# Computed fields
for c in ["Sets_Actual","Reps_Actual","Load_Actual","RPE"]:
    if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")

df["Tonnage"] = df.get("Sets_Actual", pd.NA) * df.get("Reps_Actual", pd.NA) * df.get("Load_Actual", pd.NA)

# Week key by ISO week on Session Date
dts = pd.to_datetime(df.get("Date"), errors="coerce")
df["WeekKey"] = dts.dt.to_period("W").astype(str)  # e.g., '2025-08-25/2025-08-31'
df["YearWeek"] = dts.dt.isocalendar().week
df["Year"] = dts.dt.isocalendar().year

# Focus areas from library (fallback to MovementID prefixes if no Category)
if "Category" in df.columns:
    df["Focus"] = df["Category"].fillna("Uncategorized")
else:
    # crude fallback: use MovementID as focus buckets
    df["Focus"] = df["MovementID"].astype(str)

# ── Program picker ────────────────────────────────────────────────────────────
if "Name" in progs.columns:
    progs["ProgramName"] = progs["Name"].fillna(progs.get("ProgramID"))
else:
    progs["ProgramName"] = progs.get("ProgramID", pd.Series([], dtype=str)).astype(str)

used_pids = df["ProgramID"].dropna().unique().tolist()
pg = progs[progs["ProgramID"].isin(used_pids)][["ProgramID","ProgramName"]].drop_duplicates()

if pg.empty:
    st.info("No programs have logged data yet.")
    st.stop()

sel_label = st.selectbox("Program to review", options=pg["ProgramName"].tolist())
sel_pid = pg.loc[pg["ProgramName"] == sel_label, "ProgramID"].iloc[0]
d = df[df["ProgramID"] == sel_pid].copy()
p = presc_slim.merge(sess_slim[["SessionID","ProgramID"]], on="SessionID", how="left")
p = p[p["ProgramID"] == sel_pid]

# ── Limit to review window ────────────────────────────────────────────────────
if "Date" in d.columns:
    max_date = pd.to_datetime(d["Date"], errors="coerce").max()
    if pd.notna(max_date):
        min_date = max_date - pd.Timedelta(days=7*review_weeks)
        d = d[pd.to_datetime(d["Date"], errors="coerce") >= min_date]

# ── Weekly aggregates ────────────────────────────────────────────────────────
weekly = d.groupby("WeekKey", dropna=True).agg(
    Sessions=("SessionID","nunique"),
    MovementsLogged=("MovementID","count"),
    TotalSets=("Sets_Actual","sum"),
    TotalReps=("Reps_Actual","sum"),
    TotalTonnage=("Tonnage","sum"),
    AvgRPE=("RPE","mean"),
).reset_index()

# Adherence: logged movements vs prescribed movements per week
sp = sess_slim[sess_slim["ProgramID"] == sel_pid][["SessionID","Date"]].copy()
sp["WeekKey"] = pd.to_datetime(sp["Date"], errors="coerce").dt.to_period("W").astype(str)
presc_week = p.merge(sp[["SessionID","WeekKey"]], on="SessionID", how="left")
adherence = presc_week.groupby("WeekKey").size().rename("PrescribedMovements").reset_index()

weekly = weekly.merge(adherence, on="WeekKey", how="left")
weekly["PrescribedMovements"] = weekly["PrescribedMovements"].fillna(0).astype(int)
weekly["AdherencePct"] = np.where(
    weekly["PrescribedMovements"] > 0,
    (weekly["MovementsLogged"] / weekly["PrescribedMovements"]) * 100.0,
    np.nan
)

# ── Trend checks ─────────────────────────────────────────────────────────────
def pct_change_series(x: pd.Series) -> Optional[float]:
    x = x.dropna()
    if len(x) < min_weeks_trend: return None
    first = x.iloc[0:len(x)//2].mean()
    last  = x.iloc[len(x)//2:].mean()
    if first == 0 or np.isnan(first) or np.isnan(last): return None
    return (last - first) / abs(first)

tonnage_trend = pct_change_series(weekly["TotalTonnage"]) if "TotalTonnage" in weekly else None
rpe_trend     = pct_change_series(weekly["AvgRPE"]) if "AvgRPE" in weekly else None
adh_trend     = pct_change_series(weekly["AdherencePct"]) if "AdherencePct" in weekly else None

# Focus-area micro-trends
focus_weekly = d.groupby(["WeekKey","Focus"], dropna=True).agg(
    Tonnage=("Tonnage","sum"),
    AvgRPE=("RPE","mean")
).reset_index()

focus_summary = []
for focus, grp in focus_weekly.groupby("Focus"):
    if len(grp) < focus_min_weeks: 
        continue
    t_tr = pct_change_series(grp.sort_values("WeekKey")["Tonnage"])
    r_tr = pct_change_series(grp.sort_values("WeekKey")["AvgRPE"])
    focus_summary.append({"Focus": focus, "TonnageTrend": t_tr, "RPETrend": r_tr})
focus_df = pd.DataFrame(focus_summary).sort_values("TonnageTrend", ascending=True, na_position="last")

# ── Heuristic recommendations ────────────────────────────────────────────────
recs = []

def add(msg: str): recs.append(f"• {msg}")

# Global tonnage trend
if tonnage_trend is not None:
    if tonnage_trend < -stall_pct:
        add("Overall training **tonnage is trending down**. Consider a small volume or load bump next week (+5–10%).")
    elif tonnage_trend > stall_pct:
        add("Overall tonnage **increasing** appropriately. Keep progression steady.")
    else:
        add("Overall tonnage is **flat**. If intentional (maintenance), fine; else nudge progression.")

# Fatigue signal: RPE up while tonnage down
if rpe_trend is not None and tonnage_trend is not None:
    if rpe_trend > 0 and tonnage_trend < -fatigue_drop:
        add("**Fatigue risk**: Avg RPE rising while tonnage is dropping. Consider a deload or extra recovery (sleep/nutrition).")

# Adherence
if "AdherencePct" in weekly and weekly["AdherencePct"].notna().any():
    recent_adherence = weekly["AdherencePct"].tail(min_weeks_trend).mean()
    if recent_adherence < low_adherence:
        add(f"**Low adherence** (~{recent_adherence:.0f}%). Simplify sessions or reduce movements until adherence improves.")
    else:
        add(f"Adherence looks **good** (~{recent_adherence:.0f}%).")

# Focus-area suggestions
if not focus_df.empty:
    weak = focus_df.head(2).dropna(subset=["TonnageTrend"])
    strong = focus_df.tail(2).dropna(subset=["TonnageTrend"])
    if not weak.empty:
        names = ", ".join(weak["Focus"].astype(str).tolist())
        add(f"Focus areas stalling: **{names}**. Add a small dose (+1 set or +2.5–5 kg) or re-order in the week.")
    if not strong.empty:
        names = ", ".join(strong["Focus"].astype(str).tolist())
        add(f"Areas progressing well: **{names}**. Maintain progression; watch for rising RPE.")

# ── Render outputs ────────────────────────────────────────────────────────────
st.subheader(f"Program: {sel_label}")
if recs:
    st.success("Recommendations")
    st.write("\n".join(recs))
else:
    st.info("No strong signals yet. Keep logging to build trend confidence.")

col1, col2 = st.columns(2)
with col1:
    st.markdown("### Weekly Overview")
    show_cols = ["WeekKey","Sessions","MovementsLogged","PrescribedMovements","AdherencePct","TotalTonnage","AvgRPE"]
    exist = [c for c in show_cols if c in weekly.columns]
    st.dataframe(weekly[exist].reset_index(drop=True), use_container_width=True)
with col2:
    st.markdown("### Focus Trends")
    if focus_df.empty:
        st.caption("Need more weeks of data per focus area.")
    else:
        st.dataframe(
            focus_df.rename(columns={"TonnageTrend":"TonnageTrendPct","RPETrend":"RPETrend"})
                    .assign(TonnageTrendPct=lambda x: (x["TonnageTrend"]*100).round(1))
                    [["Focus","TonnageTrendPct","RPETrend"]]
                    .reset_index(drop=True),
            use_container_width=True
        )

st.caption("Heuristic agent v0.1 — rules based on tonnage/RPE/adherence trends. We can expand to ML once enough data accrues.")
