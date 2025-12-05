from pathlib import Path
import argparse
import pandas as pd
from fitparse import FitFile

def infer_fit_type(stem: str) -> str:
    parts = stem.split("_")
    return "_".join(parts[1:]).upper() if len(parts) >= 2 else "UNKNOWN"

def fit_to_dataframe(fit_path, min_size_bytes: int = 0) -> pd.DataFrame:
    """
    Parse a .fit to DataFrame.
    - Cast Path → str for fitparse (fixes 'WindowsPath' error).
    - Don't skip small files by default.
    """
    try:
        # If you really want to skip tiny files, set min_size_bytes>0
        if min_size_bytes and fit_path.stat().st_size < min_size_bytes:
            print(f"[WARN] {fit_path.name} is {fit_path.stat().st_size} bytes — skipping by min_size_bytes={min_size_bytes}.")
            return pd.DataFrame()

        # KEY FIX: pass string path, not Path object
        fitfile = FitFile(str(fit_path))

        records = []
        for msg in fitfile.get_messages():
            row = {}
            for field in msg:
                row[field.name] = field.value
            if row:
                records.append(row)

        return pd.DataFrame(records) if records else pd.DataFrame()
    except Exception as e:
        print(f"[WARN] Failed to parse {fit_path.name}: {e}")
        return pd.DataFrame()

def first_existing_with_fits(candidates) -> Path | None:
    """Return the first path that exists and contains at least one .fit (recursively)."""
    for c in candidates:
        try:
            if c.exists():
                fits = list(c.rglob("*.fit"))
                if fits:
                    print(f"[INFO] Auto-discovered FIT_DIR: {c.resolve()} (found {len(fits)} files)")
                    return c
                else:
                    print(f"[INFO] Checked {c.resolve()} — exists but no .fit files.")
        except Exception:
            pass
    return None

def resolve_fit_dir(cli_fit_dir: Path | None) -> Path:
    """Choose fit_dir: CLI arg if given, else auto-discover from common locations."""
    if cli_fit_dir:
        return cli_fit_dir

    here = Path(__file__).parent
    candidates = [
        here / "fit_files",                  # ./pages/fit_files
        here.parent / "fit_files",           # ../fit_files (project root)
        Path.cwd() / "fit_files",            # PWD/fit_files (in case you run from elsewhere)
        Path(r"C:\CrossFitCoach\fit_files"), # common absolute location you showed
    ]
    found = first_existing_with_fits(candidates)
    return found if found else (here / "fit_files")

def resolve_out_dir(cli_out_dir: Path | None, fit_dir: Path, script_dir: Path) -> Path:
    """Choose out_dir: CLI arg if given, else sibling 'csv_outputs' near the fit_dir or script."""
    if cli_out_dir:
        return cli_out_dir
    # Prefer a clean output next to the parent of fit_files (likely project root)
    root_like = fit_dir.parent
    return (root_like / "csv_outputs") if root_like.exists() else (script_dir / "csv_outputs")

def convert_all(fit_dir: Path, out_dir: Path, recursive: bool):
    out_dir.mkdir(parents=True, exist_ok=True)
    by_type_dir = out_dir / "by_type"
    by_type_dir.mkdir(parents=True, exist_ok=True)

    pattern = "**/*.fit" if recursive else "*.fit"
    print(f"[INFO] Searching for .fit files in: {fit_dir.resolve()} (recursive={recursive})")
    fit_files = sorted(fit_dir.glob(pattern))
    print(f"[INFO] Found {len(fit_files)} files")

    if not fit_files:
        print("[HINT] If your files are under C:\\CrossFitCoach\\fit_files, run with:")
        print("       python fit_to_csv.py --fit-dir C:\\CrossFitCoach\\fit_files --out-dir C:\\CrossFitCoach\\csv_outputs --recursive")
        return

    per_file_frames = []
    per_type_frames = {}

    for fit_file in fit_files:
        df = fit_to_dataframe(fit_file)
        # annotate provenance
        df["source_file"] = str(fit_file)
        df["fit_type"] = infer_fit_type(fit_file.stem)

        # write per-file csv (even if empty)
        csv_path = out_dir / f"{fit_file.stem}.csv"
        df.to_csv(csv_path, index=False)

        if not df.empty:
            per_file_frames.append(df)
            ft = df["fit_type"].iat[0]
            per_type_frames.setdefault(ft, []).append(df)

        print(f"[OK] {fit_file.name} → {csv_path.name} ({len(df)} rows)")

    # merged
    merged = pd.concat(per_file_frames, ignore_index=True, sort=True) if per_file_frames else pd.DataFrame()
    merged_path = out_dir / "garmin_merged.csv"
    merged.to_csv(merged_path, index=False)
    print(f"[OK] Wrote merged file: {merged_path} ({len(merged)} rows)")

    # by-type merged
    for ft, frames in per_type_frames.items():
        m = pd.concat(frames, ignore_index=True, sort=True) if frames else pd.DataFrame()
        by_type_path = by_type_dir / f"{ft}.csv"
        m.to_csv(by_type_path, index=False)
        print(f"[OK] Wrote by-type: {by_type_path} ({len(m)} rows)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert Garmin .fit files to CSV (auto-discovers folders).")
    parser.add_argument("--fit-dir", type=Path, default=None, help="Folder with .fit files (optional)")
    parser.add_argument("--out-dir", type=Path, default=None, help="Output folder for CSVs (optional)")
    parser.add_argument("--recursive", action="store_true", help="Search fit-dir recursively")
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    fit_dir = resolve_fit_dir(args.fit_dir)
    out_dir = resolve_out_dir(args.out_dir, fit_dir, script_dir)

    convert_all(fit_dir, out_dir, args.recursive or True)  # default to recursive search
