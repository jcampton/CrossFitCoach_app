#!/usr/bin/env python
r"""
Extract per-set strength details from Garmin FIT files (DI-Connect export friendly).

- Scans a folder recursively (e.g., ...\DI-Connect-Uploaded-Files or a Part subfolder)
- Finds strength activities that contain FIT "set" messages
- Writes a single CSV into the SAME folder you scan:
    garmin_strength_sets_<YYYYMMDD_HHMMSS>.csv
"""
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd

try:
    from fitparse import FitFile
except Exception as e:
    raise SystemExit("fitparse not installed. Run:\n  pip install fitparse pandas\n\n" + str(e))

# -------- helpers --------
def fmt_duration(seconds):
    if seconds in (None, ""):
        return ""
    try:
        s = float(seconds)
    except Exception:
        return ""
    neg = s < 0
    s = abs(s)
    m = int(s // 60); rem = s - m*60
    tenths = int(round((rem - int(rem)) * 10)); sec = int(rem)
    if tenths == 10: sec += 1; tenths = 0
    out = f"{m}:{sec:02d}.{tenths}"
    return f"-{out}" if neg else out

def safe_val(v):
    if isinstance(v, bytes):
        try: return v.decode("utf-8","ignore")
        except Exception: return str(v)
    return v

def extract_sets_from_fit(fit_path: Path):
    ff = FitFile(str(fit_path)); ff.parse()
    # sport
    sport = None
    for m in ff.get_messages("sport"):
        vals = {d.name: safe_val(d.value) for d in m}
        sport = vals.get("sport", sport); break
    # session start
    session_start = None
    for m in ff.get_messages("session"):
        vals = {d.name: safe_val(d.value) for d in m}
        if vals.get("start_time"): session_start = vals["start_time"]; break
    # sets
    rows = []; set_idx = 0
    for m in ff.get_messages("set"):
        vals = {d.name: safe_val(d.value) for d in m}
        reps = vals.get("repetitions") if vals.get("repetitions") is not None else vals.get("reps")
        weight = vals.get("weight")
        duration = vals.get("duration") or vals.get("total_elapsed_time")
        rest = vals.get("rest_time")
        title = vals.get("exercise_title") or vals.get("exercise_category") or vals.get("exercise_name")
        ex_name = f"Exercise {title}" if (isinstance(title,(int,float)) or title is None) else str(title)
        try: reps_i = int(reps) if reps not in ("",None) else None
        except Exception: reps_i = None
        try: wkg = float(weight) if weight not in ("",None) else None
        except Exception: wkg = None
        volume = (reps_i*wkg) if (reps_i is not None and wkg is not None) else None
        set_idx += 1
        local_date = ""
        if session_start:
            try: local_date = session_start.strftime("%d/%m/%Y")
            except Exception: local_date = str(session_start)
        rows.append({
            "Set": set_idx,
            "Exercise Name": ex_name,
            "Time": fmt_duration(duration),
            "Rest": fmt_duration(rest),
            "Reps": reps_i if reps_i is not None else "",
            "Weight": (f"{wkg:g} kg" if wkg is not None else ""),
            "Volume": (f"{volume:g} kg" if volume is not None else ""),
            "SessionStart": str(session_start) if session_start else "",
            "LocalDate": local_date,
            "Sport": sport if sport else "",
            "File": fit_path.name,
            "FileBytes": fit_path.stat().st_size
        })
    return rows

def main():
    ap = argparse.ArgumentParser()
    # root is now optional; good for VS Code runs without args
    ap.add_argument("root", nargs="?", default=None, help="Folder to scan (recursively) or a single FIT file")
    args = ap.parse_args()

    # Resolve root path: arg → default DI_CONNECT → cwd
    if args.root:
        root = Path(args.root)
    else:
        # Safe default with forward slashes
        default_candidate = Path("C:/CrossFitCoach/DI_CONNECT/DI-Connect-Uploaded-Files")
        root = default_candidate if default_candidate.exists() else Path.cwd()
        print(f"[INFO] No path arg supplied. Using: {root}")

    if not root.exists():
        raise SystemExit(f"Not found: {root}")

    if root.is_file():
        fit_files = [root]; out_dir = root.parent
    else:
        fit_files = sorted(root.rglob("*.fit")); out_dir = root

    if not fit_files:
        print("No .fit files found."); return

    all_rows = []; skipped = 0
    for fp in fit_files:
        try:
            rows = extract_sets_from_fit(fp)
            if rows: all_rows.extend(rows)
            else: skipped += 1
        except Exception as e:
            print(f"[WARN] {fp.name}: {e}")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"garmin_strength_sets_{ts}.csv"

    if not all_rows:
        pd.DataFrame(columns=[
            "Set","Exercise Name","Time","Rest","Reps","Weight","Volume",
            "SessionStart","LocalDate","Sport","File","FileBytes"
        ]).to_csv(out_path, index=False)
        print(f"No strength sets found. Wrote empty file: {out_path}"); return

    df = pd.DataFrame(all_rows, columns=[
        "Set","Exercise Name","Time","Rest","Reps","Weight","Volume",
        "SessionStart","LocalDate","Sport","File","FileBytes"
    ])
    df.to_csv(out_path, index=False)
    print(f"Wrote {len(df)} rows to:\n  {out_path}")
    print(f"Scanned {len(fit_files)} files • Strength files with sets: {len(df['File'].unique())} • Non-strength or no-sets skipped: {skipped}")

if __name__ == "__main__":
    main()
