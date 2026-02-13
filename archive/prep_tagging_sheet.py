# prep_tagging_sheet.py
# Build a block-level "Tagging Sheet" to name exercises in batches.
# Usage:
#   python prep_tagging_sheet.py "C:\...\garmin_strength_sets_*.csv" --out "C:\...\tagging_sheet.csv"
# You can tweak thresholds with --rest-threshold and --weight-jump.

import argparse, re
from pathlib import Path
import pandas as pd
import numpy as np

def parse_mmss_t(s):
    if not isinstance(s, str) or not s.strip(): return 0.0
    m = re.match(r"^-?(\d+):(\d{2})(?:\.(\d))?$", s.strip())
    if not m: return 0.0
    minutes, seconds = int(m.group(1)), int(m.group(2))
    tenths = int(m.group(3)) if m.group(3) else 0
    return minutes*60 + seconds + tenths*0.1

def kg_text_to_float(x):
    if x is None or (isinstance(x,float) and np.isnan(x)): return np.nan
    s = str(x).lower().replace("kg","").strip()
    try: return float(s)
    except: return np.nan

def add_block_ids(df, rest_threshold=180, weight_jump=25):
    # df: one session (File) sorted by Set
    block_id = 1
    blocks = []
    prev_rest = 0.0
    prev_w = None
    for _, row in df.iterrows():
        w = row.get("weight_kg", np.nan)
        # break if long rest OR big weight jump (optional)
        is_break = (prev_rest >= rest_threshold) or (prev_w is not None and not np.isnan(w) and not np.isnan(prev_w) and abs(w - prev_w) >= weight_jump)
        if is_break:
            block_id += 1
        blocks.append(block_id)
        prev_rest = float(row.get("rest_s", 0.0) or 0.0)
        prev_w = w if not np.isnan(w) else prev_w
    df["BlockID"] = blocks
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input", help="Path or glob to garmin_strength_sets_*.csv")
    ap.add_argument("--out", required=True, help="Output tagging sheet CSV")
    ap.add_argument("--rest-threshold", type=int, default=180, help="Seconds of rest to start a new block (default 180)")
    ap.add_argument("--weight-jump", type=float, default=25.0, help="Weight change to trigger new block (default 25 kg)")
    args = ap.parse_args()

    # Load
    paths = [Path(p) for p in (list(Path().glob(args.input)) if any(ch in args.input for ch in "*?") else [args.input])]
    if not paths: raise SystemExit(f"No files matched: {args.input}")
    frames = []
    for p in paths:
        df = pd.read_csv(p)
        frames.append(df)
    data = pd.concat(frames, ignore_index=True)

    # Parse numerics
    data["work_s"] = data["Time"].apply(parse_mmss_t)
    data["rest_s"] = data["Rest"].apply(parse_mmss_t)
    data["weight_kg"] = data["Weight"].apply(kg_text_to_float)
    vol = data["Volume"].astype(str).str.lower().str.replace("kg","", regex=False).str.strip()
    data["volume_kg"] = pd.to_numeric(vol, errors="coerce")
    missing = data["volume_kg"].isna()
    data.loc[missing, "volume_kg"] = pd.to_numeric(data["Reps"], errors="coerce") * data["weight_kg"]

    # Add BlockID per session
    out_rows = []
    for f, g in data.sort_values(["File","Set"]).groupby("File", sort=False):
        g = add_block_ids(g.copy(), rest_threshold=args.rest_threshold, weight_jump=args.weight_jump)
        # aggregate by block
        agg = g.groupby("BlockID", as_index=False).agg({
            "LocalDate":"first",
            "SessionStart":"first",
            "Set":"count",
            "Reps": lambda s: pd.to_numeric(s, errors="coerce").sum(),
            "weight_kg": ["max","mean"],
            "volume_kg":"sum",
            "work_s":"sum",
            "rest_s":"sum"
        })
        agg.columns = ["BlockID","LocalDate","SessionStart","Sets","TotalReps","TopWeight_kg","AvgWeight_kg","TotalVolume_kg","WorkTime_s","RestTime_s"]
        agg.insert(0, "File", f)
        agg["ExerciseTag"] = ""  # <-- you fill this in Excel
        out_rows.append(agg)

    tag = pd.concat(out_rows, ignore_index=True)
    tag.to_csv(args.out, index=False)
    print(f"Wrote tagging sheet with {len(tag)} blocks:\n  {args.out}\nFill 'ExerciseTag' and save.")
if __name__ == "__main__":
    main()
