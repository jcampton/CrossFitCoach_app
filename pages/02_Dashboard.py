# 02_Dashboard.py  (a.k.a. dashboard_streamlit.py)
from __future__ import annotations

from pathlib import Path
from datetime import datetime
import re
import numpy as np
import pandas as pd
import streamlit as st

# Shared helpers
from coach_io import load_settings, load_excel_tables, pick_series

# ──────────────────────────────────────────────────────────────────────────────
# Page config
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="CrossFit AI Coach – Dashboard", layout="wide")
st.title("📊 CrossFit AI Coach — Dashboard / Insights")

# ──────────────────────────────────────────────────────────────────────────────
# Settings & paths
# ──────────────────────────────────────────────────────────────────────────────
def derive_paths(cfg: dict) -> dict:
    base_excel = Path(cfg["data_path"])              # single source of truth
    log_csv = base_excel.with_name(cfg.get("log_csv", "SessionsMovements_Log.csv"))
    closed_csv = base_excel.with_name("Sessions_Closed.csv")
    return {"excel": base_excel, "log_csv": log_csv, "closed_csv": closed_csv}

cfg = load_settings("data/settings.json")
paths = derive_paths(cfg)

disp_cfg = cfg.get("display", {})
round_inc = disp_cfg.get("round_increment", 2.5)
default_unit = (str(disp_cfg.get("default_unit", "kg")).strip() or "kg")

try:
    round_inc = float(round_inc)
    if round_inc <= 0 or pd.isna(round_inc):
        round_inc = 0.0
except Exception:
    round_inc = 0.0

# ──────────────────────────────────────────────────────────────────────────────
# Load workbook (NO path guessing — use settings.json only)
# ──────────────────────────────────────────────────────────────────────────────
def read_all_sheets(path: Path) -> dict[str, pd.DataFrame]:
    try:
        return load_excel_tables(path)
    except Exception:
        try:
            return pd.read_excel(path, sheet_name=None)
        except Exception:
            return {}

resolved_excel = paths["excel"]
raw = read_all_sheets(resolved_excel)

sheets_cfg = cfg.get("sheets", {})
progs = raw.get(sheets_cfg.get("programs", "Programs"), pd.DataFrame()).copy()
sess  = raw.get(sheets_cfg.get("sessions", "Sessions"), pd.DataFrame()).copy()
movs  = raw.get(sheets_cfg.get("session_movements", "SessionMovements"), pd.DataFrame()).copy()
lib   = raw.get(sheets_cfg.get("movement_library", "MovementLibrary"), pd.DataFrame()).copy()

# ──────────────────────────────────────────────────────────────────────────────
# Log CSV (append-only safety net)
# ──────────────────────────────────────────────────────────────────────────────
_LOG_COLS = [
    "LogID","SessionID","MovementID",
    "Sets_Prescribed","Reps_Prescribed","Load_Prescribed","Pct1RM_Prescribed",
    "Sets_Actual","Reps_Actual","Load_Actual","RPE","Notes","Timestamp"
]
def _ensure_log_csv(path: Path):
    p = Path(path)
    if not p.exists():
        pd.DataFrame(columns=_LOG_COLS).to_csv(p, index=False)
        return
    try:
        head = pd.read_csv(p, nrows=5)
        if not set(_LOG_COLS).issubset(set(head.columns)):
            backup = p.with_suffix(p.suffix + ".bak_dashboard_" + datetime.now().strftime("%Y%m%d_%H%M%S"))
            p.rename(backup)
            pd.DataFrame(columns=_LOG_COLS).to_csv(p, index=False)
    except Exception:
        backup = p.with_suffix(p.suffix + ".corrupt_dashboard_" + datetime.now().strftime("%Y%m%d_%H%M%S"))
        try:
            p.rename(backup)
        except Exception:
            pass
        pd.DataFrame(columns=_LOG_COLS).to_csv(p, index=False)

_ensure_log_csv(paths["log_csv"])
try:
    all_logs = pd.read_csv(paths["log_csv"])
except Exception:
    all_logs = pd.DataFrame()

with st.sidebar:
    st.subheader("Settings")
    st.code(
        f'{{\n  "excel_path": "{paths["excel"]}",\n  "log_csv": "{paths["log_csv"]}"\n}}',
        language="json"
    )

# ──────────────────────────────────────────────────────────────────────────────
# Program insights (only if logs present)
# ──────────────────────────────────────────────────────────────────────────────
if all_logs.empty or "SessionID" not in all_logs.columns:
    st.info("No app logs recorded yet — program insights will appear after you complete a session in the coach app.")
else:
    if sess.empty:
        st.error("Sessions sheet not found/empty. Check your workbook and settings.")
    elif progs.empty:
        st.error("Programs sheet not found/empty. Check your workbook and settings.")
    else:
        sess_cols = ["SessionID"]
        for col in ["Date", "ProgramID", "ProgramCode", "SessionOrder", "SessionCode", "SessionLabel"]:
            if col in sess.columns:
                sess_cols.append(col)
        sess_slim = sess[sess_cols].drop_duplicates("SessionID")
        df = all_logs.merge(sess_slim, on="SessionID", how="left")

        # Bring in prescribed data from SessionMovements (Excel)
        if not movs.empty and {"SessionID", "MovementID"}.issubset(movs.columns) and "MovementID" in df.columns:
            presc_cols_candidates = [
                "SessionID", "MovementID",
                "Sets", "Reps",
                "CalcLoad", "Calculated Load", "Prescribed Load", "Load",
                "%1RM", "Percent1RM", "Pct1RM",
                "Notes"
            ]
            presc_cols = [c for c in presc_cols_candidates if c in movs.columns]
            presc_slim = movs[presc_cols].copy()
            df = df.merge(presc_slim, on=["SessionID", "MovementID"], how="left", suffixes=("", "_Rx"))

        for c in ["Sets_Actual", "Reps_Actual", "Load_Actual", "RPE"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        df["Load_Prescribed"] = pick_series(df, ["CalcLoad", "Calculated Load", "Prescribed Load", "Load", "Load_Prescribed"])
        df["Pct1RM_Prescribed"] = pick_series(df, ["%1RM", "Percent1RM", "Pct1RM", "Pct1RM_Prescribed"])
        df["Sets_Prescribed"] = pick_series(df, ["Sets_Prescribed", "Sets"])
        df["Reps_Prescribed"] = pick_series(df, ["Reps_Prescribed", "Reps"])

        for c in ["Sets_Prescribed", "Reps_Prescribed", "Load_Prescribed"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        df["Tonnage"] = df.get("Sets_Actual", pd.NA) * df.get("Reps_Actual", pd.NA) * df.get("Load_Actual", pd.NA)
        df["Tonnage_Prescribed"] = df.get("Sets_Prescribed", pd.NA) * df.get("Reps_Prescribed", pd.NA) * df.get("Load_Prescribed", pd.NA)

        if "SessionOrder" in df.columns and df["SessionOrder"].notna().any():
            df["Week"] = df["SessionOrder"]
        else:
            dts = pd.to_datetime(df.get("Date"), errors="coerce")
            df["Week"] = dts.dt.isocalendar().week

        if not lib.empty and "Unit" in lib.columns:
            _unit_mode = (lib["Unit"].dropna().astype(str).str.strip()).mode()
            inferred_unit = _unit_mode.iat[0] if not _unit_mode.empty else "kg"
        else:
            inferred_unit = "kg"
        unit_label = (default_unit or inferred_unit or "kg")

        if "Name" in progs.columns:
            progs["ProgramName"] = progs["Name"].fillna(progs["ProgramID"])
        else:
            progs["ProgramName"] = progs["ProgramID"].astype(str)

        used_pids = df["ProgramID"].dropna().unique().tolist()
        pg = progs[progs["ProgramID"].isin(used_pids)][["ProgramID", "ProgramName"]].drop_duplicates()

        if pg.empty:
            st.info("No programs have logged data yet.")
        else:
            sel_label = st.selectbox("Program for insights", options=pg["ProgramName"].tolist())
            sel_pid   = pg.loc[pg["ProgramName"] == sel_label, "ProgramID"].iloc[0]
            dfx = df[df["ProgramID"] == sel_pid].copy()

            if dfx.empty:
                st.info("No logs yet for this program.")
            else:
                agg = dfx.groupby("Week", dropna=True).agg(
                    Sessions=("SessionID", "nunique"),
                    TotalSets=("Sets_Actual", "sum"),
                    TotalReps=("Reps_Actual", "sum"),
                    TotalTonnage=("Tonnage", "sum"),
                    PrescribedTonnage=("Tonnage_Prescribed", "sum"),
                    AvgRPE=("RPE", "mean"),
                ).reset_index().sort_values("Week")

                c1, c2 = st.columns(2)
                with c1:
                    st.subheader(f"Weekly Tonnage ({unit_label})")
                    if not agg.empty and "TotalTonnage" in agg and not agg["TotalTonnage"].isna().all():
                        st.line_chart(agg.set_index("Week")[["TotalTonnage", "PrescribedTonnage"]])
                    else:
                        st.caption("No load data yet to compute tonnage.")
                with c2:
                    st.subheader("Average RPE")
                    if not agg.empty and "AvgRPE" in agg and not agg["AvgRPE"].isna().all():
                        st.line_chart(agg.set_index("Week")["AvgRPE"])
                    else:
                        st.caption("No RPE data yet.")

                st.subheader(f"Weekly Summary ({unit_label})")
                if agg.empty:
                    st.caption("No weekly data to summarize yet.")
                else:
                    cols_to_show = ["Week", "Sessions", "TotalSets", "TotalReps", "TotalTonnage", "PrescribedTonnage", "AvgRPE"]
                    existing = [c for c in cols_to_show if c in agg.columns]
                    st.dataframe(agg[existing], use_container_width=True)

# ──────────────────────────────────────────────────────────────────────────────
# WELLNESS — HRV / Resting HR / Body Battery / Sleep (7-day rolling only)
# ──────────────────────────────────────────────────────────────────────────────
st.divider()
st.header("🩺 Wellness — HRV / Resting HR / Sleep")

def pick_sheet_case_insensitive(sheets: dict[str, pd.DataFrame], target: str) -> tuple[pd.DataFrame, str]:
    if target in sheets:
        return sheets[target].copy(), target
    for k in sheets:
        if k.lower() == target.lower():
            return sheets[k].copy(), k
    return pd.DataFrame(), ""

def parse_dates_best(df: pd.DataFrame) -> tuple[pd.Series, str]:
    """Return best-parsed datetime series and the column used."""
    candidates = [c for c in df.columns if "date" in str(c).lower()]
    if not candidates:
        return pd.Series(pd.NaT, index=df.index), ""
    best = (pd.Series(pd.NaT, index=df.index), "")
    for c in candidates:
        s = df[c]
        # strip strings
        if s.dtype == object:
            s = s.astype(str).str.strip()
        # try dd/MM/yyyy first (your data)
        dt1 = pd.to_datetime(s, errors="coerce", dayfirst=True)
        # ISO fallback
        dt2 = pd.to_datetime(s, errors="coerce")
        # Excel serials (numeric or numeric-looking strings)
        if pd.api.types.is_numeric_dtype(df[c]) or (df[c].astype(str).str.fullmatch(r"\d+").any()):
            ser_num = pd.to_numeric(df[c], errors="coerce")
            dt3 = pd.to_datetime(ser_num, unit="D", origin="1899-12-30", errors="coerce")
        else:
            dt3 = pd.Series(pd.NaT, index=df.index)
        parsed = dt1.combine_first(dt2).combine_first(dt3)
        if parsed.notna().sum() > best[0].notna().sum():
            best = (parsed, c)
    return best

wellness_sheet_name = sheets_cfg.get("wellness", "Wellness")
wel, wel_used = pick_sheet_case_insensitive(raw, wellness_sheet_name)

if wel.empty:
    st.info("No Wellness sheet detected or it is empty. Use the Wellness page to add entries.")
else:
    w = wel.copy()

    # Parse a single canonical date column
    date_parsed, chosen_date_col = parse_dates_best(w)
    w["__Date"] = date_parsed
    w = w.dropna(subset=["__Date"]).sort_values("__Date")

    # Helper to pick metrics case-insensitively
    def pick_num(df, names: list[str]) -> pd.Series:
        # exact match first
        for n in names:
            if n in df.columns:
                return pd.to_numeric(df[n], errors="coerce")
        # case-insensitive fallback
        lowmap = {str(c).lower(): c for c in df.columns}
        for n in names:
            if n.lower() in lowmap:
                return pd.to_numeric(df[lowmap[n.lower()]], errors="coerce")
        return pd.Series([np.nan] * len(df))

    # HRV (ms) & Resting HR (bpm)
    hrv = pick_num(w, ["HRV_rmssd_ms", "HRV", "HRV rMSSD"])
    rhr = pick_num(w, ["RestingHR_bpm", "Resting HR", "RHR"])

    # Body Battery (start of day)
    bb_start = pick_num(w, ["BodyBattery_Start", "Body Battery Start", "BodyBatteryStart"])

    # Sleep Score 0–100
    sleep_score = pick_num(w, ["SleepScore_0to100", "Sleep Score", "SleepScore"])
    if sleep_score.isna().all():
        # Try extracting from Notes like "[SleepScore=72]"
        if "Notes" in w.columns:
            nums = (
                w["Notes"]
                .astype(str)
                .str.extract(r"SleepScore\s*=\s*(\d{1,3})", expand=False)
            )
            sleep_score = pd.to_numeric(nums, errors="coerce")
    if sleep_score.isna().all():
        # Last resort: convert 1–5 quality to 0–100
        q = pick_num(w, ["SleepQuality_1to5", "Sleep Quality 1to5"])
        sleep_score = (q * 20.0).clip(0, 100)

    # Build a compact frame
    w2 = pd.DataFrame(
        {
            "Date": w["__Date"].astype("datetime64[ns]"),
            "HRV": hrv,
            "RHR": rhr,
            "BodyBattery_Start": bb_start,
            "SleepScore": sleep_score,
        }
    ).dropna(subset=["Date"]).sort_values("Date")

    # Rolling 7-day averages only
    for col in ["HRV", "RHR", "BodyBattery_Start", "SleepScore"]:
        w2[f"{col}_MA7"] = w2[col].rolling(window=7, min_periods=1).mean()
        w2[f"{col}_MA28"] = w2[col].rolling(window=28, min_periods=1).mean()
        denom = w2[f"{col}_MA28"].replace({0: np.nan})
        w2[f"{col}_DeltaPct"] = (w2[f"{col}_MA7"] - w2[f"{col}_MA28"]) / denom * 100.0

    # KPIs (current 7-day + delta vs 28-day)
    last = w2.tail(1).squeeze()
    k1, k2, k3 = st.columns(3)
    def metric(holder, label, val, delta):
        v = "—" if pd.isna(val) else (f"{val:.0f}" if isinstance(val, (int, float, np.floating)) else str(val))
        d = None if pd.isna(delta) else f"{delta:+.1f}%"
        holder.metric(label, v, d)

    metric(k1, "HRV rMSSD (7d avg)", last.get("HRV_MA7", np.nan), last.get("HRV_DeltaPct", np.nan))
    metric(k2, "Resting HR (7d avg)", last.get("RHR_MA7", np.nan), last.get("RHR_DeltaPct", np.nan))
    metric(k3, "Sleep Score 0–100 (7d avg)", last.get("SleepScore_MA7", np.nan), last.get("SleepScore_DeltaPct", np.nan))

    # Quick date-range sanity line
    if not w2.empty:
        dmin, dmax = w2["Date"].min(), w2["Date"].max()
        st.caption(f"Parsed Wellness date range: {dmin:%d/%m/%Y} → {dmax:%d/%m/%Y}  •  rows: {len(w2)}")

    # Trend charts — 7-day rolling only
    st.markdown("#### Trends")
    c1, c2 = st.columns(2)
    c3, c4 = st.columns(2)

    def plot_ma7(col_holder, df, colname, title):
        with col_holder:
            if f"{colname}_MA7" not in df.columns or df["Date"].nunique() < 2:
                st.caption("No dated rows to plot yet.")
                return
            chart_df = df[["Date", f"{colname}_MA7"]].set_index("Date")
            st.caption(title)
            st.line_chart(chart_df, use_container_width=True)

    plot_ma7(c1, w2, "HRV", "HRV rMSSD — 7-day rolling")
    plot_ma7(c2, w2, "RHR", "Resting HR — 7-day rolling")
    plot_ma7(c3, w2, "BodyBattery_Start", "Body Battery (start) — 7-day rolling")
    plot_ma7(c4, w2, "SleepScore", "Sleep Score (0–100) — 7-day rolling")

    # Recent entries in an expander (clean UI)
    with st.expander("Recent Wellness entries (last 14 rows)", expanded=False):
        show_cols = ["Date", "HRV", "RHR", "BodyBattery_Start", "SleepScore",
                     "HRV_MA7", "RHR_MA7", "BodyBattery_Start_MA7", "SleepScore_MA7"]
        available = [c for c in show_cols if c in w2.columns]
        df_show = w2[available].tail(14).copy()
        st.dataframe(df_show, use_container_width=True)

# ──────────────────────────────────────────────────────────────────────────────
# PR Board
# ──────────────────────────────────────────────────────────────────────────────
st.divider()
st.header("🏆 Current PR Board")

pr = pd.DataFrame()
pr_sheet_used = ""

# Try scanning the workbook first
for name, df_sheet in raw.items():
    cols_lower = [c.lower() for c in df_sheet.columns.astype(str)]
    if {"movement name","result"} <= set(cols_lower) or {"movement","result"} <= set(cols_lower):
        pr = df_sheet.copy()
        pr_sheet_used = f"{resolved_excel.name} → {name}"
        break

# Fallback names
if pr.empty:
    for nm in ["PR_History","PR History","PR_Histry","PRs","PR"]:
        try:
            df_try = pd.read_excel(resolved_excel, sheet_name=nm)
            if not df_try.empty:
                pr = df_try.copy()
                pr_sheet_used = f"{resolved_excel.name} → {nm}"
                break
        except Exception:
            pass

# If still empty, try a baseline companion (kept from earlier behavior)
if pr.empty:
    baseline_path = resolved_excel.with_name("CrossFit_AI_Coach_Baseline.xlsx")
    if baseline_path.exists():
        try:
            all_pr = pd.read_excel(baseline_path, sheet_name=None)
            if "PR_Histry" in all_pr:
                pr = all_pr["PR_Histry"].copy()
                pr_sheet_used = f"{baseline_path.name} → PR_Histry"
            else:
                for name, df_sheet in all_pr.items():
                    cols_lower = [c.lower() for c in df_sheet.columns.astype(str)]
                    if {"movement name","result"} <= set(cols_lower) or {"movement","result"} <= set(cols_lower):
                        pr = df_sheet.copy()
                        pr_sheet_used = f"{baseline_path.name} → {name}"
                        break
        except Exception:
            pass

if pr.empty:
    st.caption("No PR data found (looked for PR_History / PR_Histry).")
else:
    for flag in ["IsPR", "IsLatestPR"]:
        if flag in pr.columns:
            pr[flag] = pr[flag].astype(str).str.upper().isin(["TRUE","1","YES","Y"])
    if "Date" in pr.columns:
        pr["Date"] = pd.to_datetime(pr["Date"], errors="coerce").dt.date
    cols = [c for c in ["Movement Name","Movement","Result","Unit","Date","Notes"] if c in pr.columns]
    if not cols:
        st.dataframe(pr, use_container_width=True)
    else:
        sort_by = "Movement Name" if "Movement Name" in cols else ("Movement" if "Movement" in cols else cols[0])
        st.dataframe(pr[cols].sort_values(sort_by), use_container_width=True)
    st.caption(f"Source: {pr_sheet_used or 'detected automatically'}")

# ──────────────────────────────────────────────────────────────────────────────
# Debug footer — always know what file/sheet we used
# ──────────────────────────────────────────────────────────────────────────────
st.divider()
with st.expander("🧩 Debug — Data sources", expanded=False):
    st.write("Workbook configured (settings.json):", str(paths["excel"]))
    st.write("Workbook actually used:", str(resolved_excel))
    if 'wel_used' in locals():
        st.write("Wellness sheet used:", wel_used or "(none)")
    if 'w2' in locals() and isinstance(w2, pd.DataFrame):
        st.write("Wellness rows parsed:", int(w2.shape[0]))
        st.write("Wellness date range:", f"{w2['Date'].min():%d/%m/%Y} → {w2['Date'].max():%d/%m/%Y}")
    if 'chosen_date_col' in locals():
        st.write("Date column chosen:", chosen_date_col or "(none)")
