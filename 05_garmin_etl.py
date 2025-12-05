# 05_Garmin_ETL.py
# One-off (or repeatable) ETL to import Garmin CSV exports into the Excel workbook
# Adds/updates two optional sheets: Activities, Wellness
# - Activities: per-workout metrics (DateTimeStart, DurationMin, AvgHR, TE, etc.)
# - Wellness: daily metrics (SleepHours, HRV, RestingHR, Stress, BodyBattery, etc.)
#
# Usage (from project root):
#   python 05_Garmin_ETL.py --activities exports/garmin_activities.csv --wellness exports/garmin_wellness.csv
# Options:
#   --tz 'Australia/Sydney'      # timezone for timestamps
#   --dry-run                    # parse + show preview, but do not write to Excel
#
# Notes:
# - This ETL is non-destructive: it APPENDS to sheets using coach_io.append_rows_to_sheet (preserves formulas)
# - Deduping: we generate a stable RowKey hash and drop duplicates already present in workbook (if those sheets exist)
# - You can run this periodically after downloading new CSVs from Garmin Connect.

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
from typing import Dict, Any

import numpy as np
import pandas as pd

from coach_io import load_settings, derive_paths, load_excel_tables, append_rows_to_sheet

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _hash_row_key(*parts: Any) -> str:
    s = "|".join(["" if p is None else str(p) for p in parts])
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]


def _as_datetime(series: pd.Series, tz: str | None = None) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    if tz:
        try:
            return dt.dt.tz_convert(tz)
        except Exception:
            return dt
    return dt


def _to_minutes(x) -> float | None:
    try:
        return float(x) / 60.0
    except Exception:
        return None


def _bannister_trimp(duration_min: float | None, avg_hr: float | None, max_hr: float | None, sex: str | None = None) -> float | None:
    """Very rough TRIMP estimate when not provided by export.
    TRIMP ≈ duration_min * HR_fraction * k * exp(λ * HR_fraction)
    λ≈1.92 (men) or 1.67 (women); k≈0.64 (men) or 0.86 (women). If sex unknown, use men constants.
    """
    if duration_min is None or avg_hr is None or max_hr is None:
        return None
    try:
        hr_frac = float(avg_hr) / float(max_hr)
        if np.isnan(hr_frac) or hr_frac <= 0 or hr_frac > 1.5:
            return None
        if (sex or "").lower().startswith("f"):
            lam, k = 1.67, 0.86
        else:
            lam, k = 1.92, 0.64
        return float(duration_min) * hr_frac * k * np.exp(lam * hr_frac)
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Mappers
# ──────────────────────────────────────────────────────────────────────────────

def map_activities(df: pd.DataFrame, tz: str) -> pd.DataFrame:
    """Map a Garmin 'activities' CSV to our Activities sheet schema.
    We try a few common Garmin/Strava export column names.
    """
    # Column aliases
    aliases = {
        "start_time": ["Start Time", "start_time", "startTimeLocal", "Activity Date"],
        "sport": ["Activity Type", "sport", "type"],
        "duration_sec": ["Duration (s)", "duration", "elapsed_time"],
        "moving_sec": ["Moving Duration (s)", "moving_time"],
        "avg_hr": ["Average HR", "avg_hr", "average_heartrate"],
        "max_hr": ["Max HR", "max_hr", "max_heartrate"],
        "distance_km": ["Distance (km)", "distance_km"],
        "distance_m": ["Distance (m)", "distance"],
        "elev_gain_m": ["Elevation Gain (m)", "total_elevation_gain", "elev_gain_m"],
        "calories": ["Calories", "calories"],
        "te_aer": ["Training Effect Aerobic", "aerobic_training_effect", "TE_Aer"],
        "te_ana": ["Training Effect Anaerobic", "anaerobic_training_effect", "TE_Ana"],
        "device": ["Device", "device_name"],
        "activity_id": ["Activity ID", "activity_id", "id"],
    }

    def pick(df, keys):
        for k in keys:
            if k in df.columns:
                return df[k]
        return pd.Series([None] * len(df))

    out = pd.DataFrame()
    out["DateTimeStart"] = _as_datetime(pick(df, aliases["start_time"]), tz)
    out["Date"] = pd.to_datetime(out["DateTimeStart"]).dt.date
    out["Sport"] = pick(df, aliases["sport"]).astype(str)

    # Duration
    dur_sec = pd.to_numeric(pick(df, aliases["duration_sec"]), errors="coerce")
    mov_sec = pd.to_numeric(pick(df, aliases["moving_sec"]), errors="coerce")
    dur_min = dur_sec.apply(_to_minutes)
    mov_min = mov_sec.apply(_to_minutes)
    out["DurationMin"] = dur_min
    out["MovingMin"] = mov_min

    # HR / Distance / Elevation / Calories
    out["AvgHR"] = pd.to_numeric(pick(df, aliases["avg_hr"]), errors="coerce")
    out["MaxHR"] = pd.to_numeric(pick(df, aliases["max_hr"]), errors="coerce")

    dist_km = pd.to_numeric(pick(df, aliases["distance_km"]), errors="coerce")
    dist_m = pd.to_numeric(pick(df, aliases["distance_m"]), errors="coerce")
    out["DistanceKm"] = dist_km.fillna(dist_m / 1000.0)

    out["ElevGainM"] = pd.to_numeric(pick(df, aliases["elev_gain_m"]), errors="coerce")
    out["Calories"] = pd.to_numeric(pick(df, aliases["calories"]), errors="coerce")

    # Training Effect (if present)
    out["TE_Aer"] = pd.to_numeric(pick(df, aliases["te_aer"]), errors="coerce")
    out["TE_Ana"] = pd.to_numeric(pick(df, aliases["te_ana"]), errors="coerce")

    out["Device"] = pick(df, aliases["device"]).astype(str)
    out["ActivityID"] = pick(df, aliases["activity_id"]).astype(str)

    # TRIMP estimate if not provided
    if "TRIMP" not in df.columns:
        out["TRIMP"] = [
            _bannister_trimp(d, a, m) if pd.notna(d) and pd.notna(a) and pd.notna(m) else np.nan
            for d, a, m in zip(out["DurationMin"], out["AvgHR"], out["MaxHR"]) 
        ]

    # Stable RowKey for deduping
    out["RowKey"] = [
        _hash_row_key(dt.isoformat() if pd.notna(dt) else None, sid, sport, dur)
        for dt, sid, sport, dur in zip(out["DateTimeStart"], out["ActivityID"], out["Sport"], out["DurationMin"]) 
    ]

    # Keep tidy order
    cols = [
        "RowKey","Date","DateTimeStart","Sport","DurationMin","MovingMin",
        "AvgHR","MaxHR","DistanceKm","ElevGainM","Calories","TE_Aer","TE_Ana","TRIMP",
        "Device","ActivityID"
    ]
    return out[cols]


def map_wellness(df: pd.DataFrame, tz: str) -> pd.DataFrame:
    """Map Garmin daily export to our Wellness schema.
    Common columns: date, sleep_duration_min, hrv, resting_hr, stress, body_battery, etc.
    """
    aliases = {
        "date": ["Date", "date"],
        "sleep_min": ["Sleep Duration (min)", "sleep_duration_min"],
        "sleep_score": ["Sleep Score", "sleep_score"],
        "hrv": ["HRV (ms)", "hrv", "avg_hrv"] ,
        "resting_hr": ["Resting Heart Rate", "resting_hr"],
        "stress": ["Stress", "avg_stress"],
        "body_battery": ["Body Battery", "avg_body_battery"],
        "steps": ["Steps", "steps"],
    }

    def pick(df, keys):
        for k in keys:
            if k in df.columns:
                return df[k]
        return pd.Series([None] * len(df))

    out = pd.DataFrame()
    out["Date"] = pd.to_datetime(pick(df, aliases["date"]), errors="coerce").dt.date
    out["SleepHours"] = pd.to_numeric(pick(df, aliases["sleep_min"]), errors="coerce") / 60.0
    out["SleepScore"] = pd.to_numeric(pick(df, aliases["sleep_score"]), errors="coerce")
    out["HRV"] = pd.to_numeric(pick(df, aliases["hrv"]), errors="coerce")
    out["RestingHR"] = pd.to_numeric(pick(df, aliases["resting_hr"]), errors="coerce")
    out["Stress"] = pd.to_numeric(pick(df, aliases["stress"]), errors="coerce")
    out["BodyBattery"] = pd.to_numeric(pick(df, aliases["body_battery"]), errors="coerce")
    out["Steps"] = pd.to_numeric(pick(df, aliases["steps"]), errors="coerce")

    out["RowKey"] = [ _hash_row_key(d) for d in out["Date"] ]

    cols = ["RowKey","Date","SleepHours","SleepScore","HRV","RestingHR","Stress","BodyBattery","Steps"]
    return out[cols]


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Import Garmin CSVs into Activities/Wellness sheets")
    ap.add_argument("--activities", type=str, default=None, help="Path to Garmin activities CSV export")
    ap.add_argument("--wellness", type=str, default=None, help="Path to Garmin wellness (daily) CSV export")
    ap.add_argument("--tz", type=str, default="Australia/Sydney", help="Timezone for timestamps")
    ap.add_argument("--dry-run", action="store_true", help="Parse and preview, but don't write to Excel")
    args = ap.parse_args()

    cfg = load_settings("data/settings.json")
    paths = derive_paths(cfg)
    wb = paths["excel"]

    existing = load_excel_tables(wb)
    have_activities = "Activities" in existing
    have_wellness = "Wellness" in existing

    # Existing RowKeys to avoid duplicates
    existing_act_keys = set()
    existing_wel_keys = set()
    if have_activities:
        try:
            existing_act_keys = set(existing["Activities"].get("RowKey", pd.Series([], dtype=str)).astype(str))
        except Exception:
            existing_act_keys = set()
    if have_wellness:
        try:
            existing_wel_keys = set(existing["Wellness"].get("RowKey", pd.Series([], dtype=str)).astype(str))
        except Exception:
            existing_wel_keys = set()

    to_write = []

    if args.activities:
        act_csv = Path(args.activities)
        if not act_csv.exists():
            raise SystemExit(f"Activities CSV not found: {act_csv}")
        df = pd.read_csv(act_csv)
        mapped = map_activities(df, args.tz)
        if existing_act_keys:
            mapped = mapped[~mapped["RowKey"].astype(str).isin(existing_act_keys)]
        to_write.append(("Activities", mapped))

    if args.wellness:
        wel_csv = Path(args.wellness)
        if not wel_csv.exists():
            raise SystemExit(f"Wellness CSV not found: {wel_csv}")
        df = pd.read_csv(wel_csv)
        mapped = map_wellness(df, args.tz)
        if existing_wel_keys:
            mapped = mapped[~mapped["RowKey"].astype(str).isin(existing_wel_keys)]
        to_write.append(("Wellness", mapped))

    if not to_write:
        raise SystemExit("No inputs provided. Use --activities and/or --wellness with CSV paths.")

    # Preview
    for name, m in to_write:
        print("\n===", name, "===")
        print("Rows parsed:", len(m))
        print(m.head(10).to_string(index=False))

    if args.dry_run:
        print("\nDry run complete. No writes performed.")
        return

    # Append to workbook
    for name, m in to_write:
        if m.empty:
            print(f"No new rows for {name} (after dedup). Skipping.")
            continue
        append_rows_to_sheet(wb, name, m)
        print(f"Appended {len(m)} rows to sheet '{name}'.")

    print("\nDone.")


if __name__ == "__main__":
    main()
