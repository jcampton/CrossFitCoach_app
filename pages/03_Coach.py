# 03_Builder.py
# Program / Workout Builder (Step 5 – Options A/B/C)
# Create or clone Sessions and prescribe movements, append-only to Excel.

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Builder",
    page_icon="🧱",
    layout="wide"
)

# Force sidebar label
st.markdown(
    "<style>section[data-testid='stSidebarNav'] li a div p {text-transform: capitalize;}</style>",
    unsafe_allow_html=True
)

# Shared helpers from coach_io
from coach_io import (
    load_settings,
    load_excel_tables,
    derive_paths as derive_paths_from_coach,
    build_one_rm_lookup,
    round_to_increment,
    pick_series,
    new_session_id,
    append_rows_to_sheet,
    calc_load,
)

# ──────────────────────────────────────────────────────────────────────────────
# Page config
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="CrossFit AI Coach – Builder", layout="wide")
st.title("🧱 Program / Workout Builder")

# Local wrapper (alias to coach_io's derive_paths for consistency with 01/02)
def derive_paths(cfg: dict) -> dict:
    return derive_paths_from_coach(cfg)

# ──────────────────────────────────────────────────────────────────────────────
# Load settings + data
# ──────────────────────────────────────────────────────────────────────────────
cfg = load_settings("data/settings.json")
paths = derive_paths(cfg)

# Display knobs
_disp = cfg.get("display", {})
round_inc = _disp.get("round_increment", 2.5)
default_unit = (_disp.get("default_unit", "kg") or "kg").strip()
try:
    round_inc = float(round_inc)
    if round_inc <= 0 or pd.isna(round_inc):
        round_inc = 0.0
except Exception:
    round_inc = 0.0

_raw = load_excel_tables(paths["excel"])  # all sheets

# Map logical names from settings
sheet_programs = cfg["sheets"]["programs"]
sheet_sessions = cfg["sheets"]["sessions"]
sheet_session_movs = cfg["sheets"]["session_movements"]
sheet_mov_lib = cfg["sheets"]["movement_library"]
sheet_one_rms = cfg["sheets"].get("one_rms", "OneRMs")

progs = _raw.get(sheet_programs, pd.DataFrame()).copy()
sess = _raw.get(sheet_sessions, pd.DataFrame()).copy()
movs = _raw.get(sheet_session_movs, pd.DataFrame()).copy()
lib = _raw.get(sheet_mov_lib, pd.DataFrame()).copy()
one_rms = _raw.get(sheet_one_rms, pd.DataFrame()).copy()

if progs.empty:
    st.warning("Programs sheet is empty. You can create a new program below.")

# OneRM lookup (optional)
one_rm_lookup = build_one_rm_lookup(one_rms)

# ──────────────────────────────────────────────────────────────────────────────
# Sidebar context block
# ──────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("Builder")
    st.subheader("Settings")
    st.code(
        (
            "{\n"
            f"  \"excel\": \"{paths['excel']}\",\n"
            f"  \"log_csv\": \"{paths['log_csv']}\",\n"
            f"  \"closed_csv\": \"{paths['closed_csv']}\"\n"
            "}"
        ),
        language="json",
    )
    st.caption("Builder app writes new rows to Sessions and SessionMovements only (append-only).")

# ──────────────────────────────────────────────────────────────────────────────
# Program selection or creation
# ──────────────────────────────────────────────────────────────────────────────
prog_mode = st.radio("Program source", ["Use existing program", "Create new program"], horizontal=True)

new_program_saved = False
sel_pid = None

if prog_mode == "Use existing program":
    if "Name" in progs.columns:
        progs["Label"] = progs["Name"].fillna(progs.get("ProgramID", ""))
    else:
        progs["Label"] = progs.get("ProgramID", pd.Series(dtype=str)).astype(str)

    prog_opts = progs[["ProgramID", "Label"]].drop_duplicates().reset_index(drop=True)
    if prog_opts.empty:
        st.info("No programs found yet. Switch to 'Create new program' to add one.")
        sel_pid = None
    else:
        sel_prog_label = st.selectbox("Program", options=prog_opts["Label"].tolist())
        sel_pid = prog_opts.loc[prog_opts["Label"] == sel_prog_label, "ProgramID"].iloc[0]
else:
    st.markdown("### New Program")
    colp1, colp2, colp3 = st.columns([1,1,1])
    with colp1:
        new_prog_name = st.text_input("Program Name", value="")
    with colp2:
        new_prog_code = st.text_input("Program Code", value="")
    with colp3:
        new_prog_start = st.date_input("Start Date", value=datetime.now().date())
    prog_notes = st.text_area("Program Notes", value="")

    if st.button("✅ Save Program", type="primary"):
        # Append to Programs sheet WITHOUT ProgramID so Excel formula can populate it later.
        row = {
            "ProgramID": pd.NA,  # leave blank for Excel formula / table calc
            "ProgramName": new_prog_name,
            "ProgramCode": new_prog_code,
            "StartDate": pd.to_datetime(new_prog_start).date(),
            "Notes": prog_notes,
        }
        try:
            append_rows_to_sheet(paths["excel"], sheet_programs, pd.DataFrame([row]))
            st.success("Program saved. Open the workbook to allow Excel to compute ProgramID, then return here to add sessions.")
            new_program_saved = True
        except Exception as e:
            st.error(f"Failed to save program: {e}")

    # We cannot create sessions until Excel has computed a ProgramID.
    st.info("Session builder becomes available once the new ProgramID exists in the workbook.")
    st.stop()

# ──────────────────────────────────────────────────────────────────────────────
# Program picker + action (New vs Clone existing session)
# ──────────────────────────────────────────────────────────────────────────────
left, right = st.columns(2)
with left:
    mode = st.radio("Builder mode", ["New Session", "Clone Existing"], horizontal=True)

# Sessions available for cloning
sess_prog = sess[sess.get("ProgramID").astype(str) == str(sel_pid)] if (sel_pid is not None and not sess.empty and "ProgramID" in sess.columns) else pd.DataFrame()

src_session_id = None
if mode == "Clone Existing":
    if sess_prog.empty:
        st.warning("No sessions found in this program to clone. Switch to New Session.")
    else:
        lab_col = "SessionLabel" if "SessionLabel" in sess_prog.columns else "SessionID"
        sess_prog = sess_prog.copy()
        sess_prog["Label"] = sess_prog[lab_col].astype(str)
        if "Date" in sess_prog.columns:
            sess_prog["_d"] = pd.to_datetime(sess_prog["Date"], errors="coerce")
            sort_cols = ["_d"]
            if "SessionOrder" in sess_prog.columns:
                sort_cols.append("SessionOrder")
            sess_prog = sess_prog.sort_values(sort_cols, na_position="last")
        src_label = st.selectbox("Clone source session", options=sess_prog["Label"].tolist())
        src_session_id = sess_prog.loc[sess_prog["Label"] == src_label, "SessionID"].iloc[0]

# ──────────────────────────────────────────────────────────────────────────────
# Session metadata
# ──────────────────────────────────────────────────────────────────────────────

colA, colB, colC = st.columns([1,1,1])
with colA:
    session_label = st.text_input("Session Label", value=("Cloned" if mode=="Clone Existing" else "New Session"))
with colB:
    session_order = st.number_input("Session Order (week/seq)", min_value=1, step=1, value=1)
with colC:
    session_date = st.date_input("Session Date", value=datetime.now().date())

notes = st.text_area("Session Notes", value="")

# ──────────────────────────────────────────────────────────────────────────────
# Movement editor
# ──────────────────────────────────────────────────────────────────────────────

# Build choices for MovementID + default units
if not lib.empty and "MovementID" in lib.columns:
    lib_view = lib[[c for c in ["MovementID", "Name", "Unit"] if c in lib.columns]].drop_duplicates("MovementID")
else:
    lib_view = pd.DataFrame({"MovementID": [], "Name": [], "Unit": []})

# If cloning: prefill from source session
if mode == "Clone Existing" and src_session_id is not None and not movs.empty:
    base_rows = movs[movs.get("SessionID").astype(str) == str(src_session_id)].copy()
else:
    base_rows = pd.DataFrame(columns=["MovementID","Sets","Reps","Pct1RM","Load","Unit","Notes"])  # empty template

# Normalize columns for the editor
base_rows = base_rows.rename(columns={"Percent1RM":"Pct1RM", "%1RM":"Pct1RM", "CalcLoad":"Load"})
for col in ["MovementID","Sets","Reps","Pct1RM","Load","Unit","Notes"]:
    if col not in base_rows.columns:
        base_rows[col] = "" if col in ["MovementID","Unit","Notes"] else 0

# Default units via library if missing
if "Unit" in lib_view.columns and not lib_view.empty:
    unit_map = dict(zip(lib_view["MovementID"].astype(str), lib_view["Unit"].astype(str)))
    base_rows["Unit"] = base_rows.apply(lambda r: unit_map.get(str(r["MovementID"]), r.get("Unit") or default_unit), axis=1)
else:
    base_rows["Unit"] = base_rows.get("Unit", default_unit).replace("", default_unit)

st.markdown("### Prescribe Movements")

# Editor schema & hints
_editor_cols = {
    "MovementID": st.column_config.SelectboxColumn(
        label="MovementID",
        options=sorted(lib_view["MovementID"].astype(str).unique().tolist()) if not lib_view.empty else [],
        help="Pick from MovementLibrary",
        width="medium",
    ),
    "Sets": st.column_config.NumberColumn("Sets", min_value=0, step=1),
    "Reps": st.column_config.NumberColumn("Reps", min_value=0, step=1),
    "Pct1RM": st.column_config.NumberColumn("%1RM (e.g. 75 or 0.75)", help="We’ll compute Load if OneRM available", min_value=0.0),
    "Load": st.column_config.NumberColumn("Load", help="If both %1RM and Load are provided, Load wins."),
    "Unit": st.column_config.SelectboxColumn(
        label="Unit",
        options=sorted(set([default_unit] + lib_view.get("Unit", pd.Series([], dtype=str)).dropna().astype(str).tolist())) if not lib_view.empty else [default_unit],
        default=default_unit,
        width="small",
    ),
    "Notes": st.column_config.TextColumn("Notes", width="large"),
}

edited = st.data_editor(
    base_rows[["MovementID","Sets","Reps","Pct1RM","Load","Unit","Notes"]],
    num_rows="dynamic",
    use_container_width=True,
    column_config=_editor_cols,
    hide_index=True,
)

# Live preview: compute CalcLoad from %1RM if available
preview = edited.copy()

# Coerce numerics
for col in ["Sets","Reps","Pct1RM","Load"]:
    if col in preview.columns:
        preview[col] = pd.to_numeric(preview[col], errors="coerce")

if not preview.empty:
    # For rows where %1RM is provided and Load is blank, compute
    calc_loads = []
    for _, r in preview.iterrows():
        mv = str(r.get("MovementID") or "")
        pct = r.get("Pct1RM")
        load = r.get("Load")
        if pd.notna(load):
            calc_loads.append(load)
        else:
            calc = calc_load(one_rm_lookup, mv, pct, round_inc)
            calc_loads.append(calc)
    preview["CalcLoad"] = calc_loads

st.markdown("#### Preview (computed loads)")
st.dataframe(preview.fillna(""), use_container_width=True)

# Inline warnings (guards)
missing_ids = preview["MovementID"].isna() | (preview["MovementID"].astype(str).str.strip()=="") if "MovementID" in preview.columns else []
if missing_ids.any():
    st.warning("Some rows are missing MovementID – they will be ignored on save.")

if one_rm_lookup and (preview["Pct1RM"].notna().any() if "Pct1RM" in preview.columns else False):
    missing_orm = []
    for _, r in preview.iterrows():
        if pd.notna(r.get("Pct1RM")) and (str(r.get("MovementID") or "") not in one_rm_lookup) and pd.isna(r.get("Load")):
            missing_orm.append(r.get("MovementID"))
    if missing_orm:
        st.info(f"No OneRM found for: {sorted(set(map(str, missing_orm)))} — Load will remain blank unless you enter it.")

# ──────────────────────────────────────────────────────────────────────────────
# Save action (append-only)
# ──────────────────────────────────────────────────────────────────────────────

save_col1, save_col2 = st.columns([1,3])
with save_col1:
    do_save = st.button("✅ Save Session & Movements", type="primary")

if do_save:
    # Build new Session row
    sid = new_session_id()
    sess_row = pd.DataFrame([
        {
            "SessionID": sid,
            "ProgramID": sel_pid,
            "Date": pd.to_datetime(session_date).date(),
            "SessionOrder": int(session_order),
            "SessionLabel": session_label.strip() or sid,
            "Notes": notes,
        }
    ])

    # Filter valid movement rows
    if preview.empty:
        st.warning("No movement rows to save.")
        st.stop()

    valid = preview.copy()
    valid["MovementID"] = valid["MovementID"].astype(str).str.strip()
    valid = valid[valid["MovementID"] != ""].copy()

    if valid.empty:
        st.warning("All rows are missing MovementID.")
        st.stop()

    # Finalize numerics & fallback units
    for col in ["Sets","Reps","Pct1RM","Load","CalcLoad"]:
        if col in valid.columns:
            valid[col] = pd.to_numeric(valid[col], errors="coerce")
    if "Unit" in valid.columns:
        valid["Unit"] = valid["Unit"].astype(str).replace({"": default_unit}).fillna(default_unit)
    else:
        valid["Unit"] = default_unit

    # Compute CalcLoad where needed (respect Load if provided)
    loads_final = []
    for _, r in valid.iterrows():
        if pd.notna(r.get("Load")):
            val = float(r.get("Load"))
            if round_inc > 0:
                val = round_to_increment(val, round_inc)
            loads_final.append(val)
        else:
            val = calc_load(one_rm_lookup, str(r["MovementID"]), r.get("Pct1RM"), round_inc)
            loads_final.append(val)
    valid["CalcLoad"] = loads_final

    # Shape for SessionMovements sheet (append-only)
    out_cols = [
        "SessionID","MovementID","Sets","Reps","Pct1RM","CalcLoad","Load","Unit","Notes"
    ]
    mov_rows = valid.copy()
    mov_rows.insert(0, "SessionID", sid)
    for c in out_cols:
        if c not in mov_rows.columns:
            mov_rows[c] = pd.NA
    mov_rows = mov_rows[out_cols]

    # Write to Excel
    try:
        append_rows_to_sheet(paths["excel"], sheet_sessions, sess_row)
        append_rows_to_sheet(paths["excel"], sheet_session_movs, mov_rows)
    except Exception as e:
        st.error(f"Failed to save: {e}")
        st.stop()

    st.success(f"Saved session {sid} with {len(mov_rows)} movement rows.")
    st.balloons()
