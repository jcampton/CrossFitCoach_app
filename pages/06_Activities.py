# 06_Activities.py
# Append-only Activities page (Garmin-friendly) writing to data/StrengthApp.xlsx → Activities
from __future__ import annotations

import json, uuid, getpass
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import streamlit as st
from openpyxl import load_workbook, Workbook

# ------------------ Config ------------------
SHEET_NAME = "Activities"
DATE_UI_FORMAT = "DD/MM/YYYY"

COLUMNS = [
    "ActivityID","Date","LocalTimezone",
    "Source","EntryMethod","ImportBatchID",
    "ActivityType","ActivitySubType",
    "Duration_min","Distance_km",
    "AvgHR_bpm","MaxHR_bpm",
    "TRIMP","TE_Aerobic","TE_Anaerobic",
    "RPE_1to10","Calories_kcal",
    "Notes",
    "LoggedAt_UTC","LoggedBy"
]

DEFAULT_TZ = "Australia/Sydney"

ACTIVITY_TYPES = ["run","ride","row","ski","swim","metcon","cf_wod","strength","other"]

# ------------------ Settings load ------------------
def load_settings(p: Path) -> dict:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

def resolve_workbook_path(cfg: dict) -> Path:
    strengthapp = Path("data") / "StrengthApp.xlsx"
    if strengthapp.exists():
        return strengthapp
    # fallbacks that may exist in your repo
    for key in [("data","workbook_path"), ("", "workbook_path"), ("", "data_path")]:
        try:
            root, k = key
            v = cfg.get(root, {}).get(k) if root else cfg.get(k)
            if v:
                return Path(v)
        except Exception:
            pass
    return strengthapp

# ------------------ Excel IO ------------------
def ensure_sheet(wb_path: Path, sheet_name: str):
    if not wb_path.exists():
        wb = Workbook()
        ws = wb.active
        ws.title = sheet_name
        ws.append(COLUMNS)
        wb.save(wb_path); return
    wb = load_workbook(wb_path)
    if sheet_name not in wb.sheetnames:
        wb.create_sheet(sheet_name).append(COLUMNS)
        wb.save(wb_path); return
    ws = wb[sheet_name]
    if [c.value for c in ws[1]] != COLUMNS:
        # header mismatch → create a fresh sheet to protect existing data
        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        wb.create_sheet(f"{sheet_name}_v{ts}").append(COLUMNS)
        wb.save(wb_path)

def append_row(wb_path: Path, sheet_name: str, row: dict):
    wb = load_workbook(wb_path)
    ws = wb[sheet_name]
    ws.append([row.get(col, "") for col in COLUMNS])
    wb.save(wb_path)

def read_recent(wb_path: Path, sheet_name: str, n=14) -> pd.DataFrame:
    try:
        df = pd.read_excel(wb_path, sheet_name=sheet_name, engine="openpyxl")
        if "Date" in df.columns:
            # display dd/MM/yyyy regardless of underlying type
            try:
                df["Date"] = pd.to_datetime(df["Date"], dayfirst=True).dt.strftime("%d/%m/%Y")
            except Exception:
                pass
        return df.tail(n)
    except Exception:
        return pd.DataFrame(columns=COLUMNS)

# ------------------ Helpers ------------------
def make_activity_id(date_val: date) -> str:
    # Keep IDs human-readable and unique
    return f"ACT-{date_val.strftime('%d/%m/%Y')}-{uuid.uuid4().hex[:4].upper()}"

def fnum(x):
    if x in ("", None): return ""
    try: return float(x)
    except Exception: return ""

# ------------------ UI ------------------
st.title("Activities (Append-only)")

cfg = load_settings(Path("data") / "settings.json")
wb_path = resolve_workbook_path(cfg)
st.caption(f"Workbook: {wb_path.resolve()} • Sheet: {SHEET_NAME}")

ensure_sheet(wb_path, SHEET_NAME)

with st.form("act_form"):
    try:
        date_val = st.date_input("Date (dd/MM/yyyy)", value=datetime.now().date(), format=DATE_UI_FORMAT)
    except TypeError:
        date_val = st.date_input("Date (dd/MM/yyyy)", value=datetime.now().date())

    c1, c2, c3, c4 = st.columns([1,1,1,1])
    with c1:
        source = st.selectbox("Source", ["manual","garmin","other"], index=1)
    with c2:
        a_type = st.selectbox("Activity Type", ACTIVITY_TYPES, index=5)  # metcon default
    with c3:
        sub_type = st.text_input("Activity SubType", placeholder="e.g., easy, threshold, Hero WOD")
    with c4:
        import_batch = st.text_input("ImportBatchID", value="" if source!="garmin" else f"GARMIN-{datetime.now():%Y%m%d}")

    c5, c6, c7 = st.columns([1,1,1])
    with c5:
        duration_min = st.number_input("Duration (min)", min_value=0.0, step=1.0)
    with c6:
        distance_km = st.text_input("Distance (km)", placeholder="")
    with c7:
        rpe = st.number_input("RPE (1–10)", min_value=1, max_value=10, value=6, step=1)

    c8, c9, c10 = st.columns([1,1,1])
    with c8:
        avg_hr = st.text_input("Avg HR (bpm)", placeholder="")
    with c9:
        max_hr = st.text_input("Max HR (bpm)", placeholder="")
    with c10:
        calories = st.text_input("Calories (kcal)", placeholder="")

    c11, c12, c13 = st.columns([1,1,1])
    with c11:
        trimp = st.text_input("TRIMP", placeholder="")         # optional; leave blank
    with c12:
        te_aer = st.text_input("TE Aerobic", placeholder="")   # optional
    with c13:
        te_ana = st.text_input("TE Anaerobic", placeholder="") # optional

    notes = st.text_area("Notes", value="", height=80)

    submitted = st.form_submit_button("Append Activity Row")

    if submitted:
        act_id = make_activity_id(date_val)
        row = {
            "ActivityID": act_id,
            "Date": date_val.strftime("%d/%m/%Y"),
            "LocalTimezone": "Australia/Sydney",
            "Source": source,
            "EntryMethod": "ui" if source != "garmin" else "import",
            "ImportBatchID": import_batch.strip(),
            "ActivityType": a_type,
            "ActivitySubType": sub_type.strip(),
            "Duration_min": float(duration_min) if duration_min != "" else "",
            "Distance_km": fnum(distance_km),
            "AvgHR_bpm": fnum(avg_hr),
            "MaxHR_bpm": fnum(max_hr),
            "TRIMP": fnum(trimp),
            "TE_Aerobic": fnum(te_aer),
            "TE_Anaerobic": fnum(te_ana),
            "RPE_1to10": int(rpe) if rpe else "",
            "Calories_kcal": fnum(calories),
            "Notes": notes.strip(),
            "LoggedAt_UTC": datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
            "LoggedBy": getpass.getuser() or "unknown"
        }
        append_row(wb_path, SHEET_NAME, row)
        st.success(f"Saved Activity {act_id} ({a_type}, {duration_min} min)")
        st.caption("Append-only: existing data was not modified.")

st.divider()
st.subheader("Recent entries")
st.dataframe(read_recent(wb_path, SHEET_NAME, n=14), use_container_width=True)
