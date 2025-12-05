# CrossFit AI Coach – Streamlit Apps

This project now has **three Streamlit pages**:

1. **01_Athlete.py** – Athlete training UI (was Coach)
   - Select Program & Session
   - View prescribed work
   - Log actuals (append-only CSV)
   - Guard rails: empty program/session, missing movements, unit fallback, rounding safety
   - Uses a **Submit Session** button to save all entries at once

2. **02_Dashboard.py** – Insights & analytics
   - Aggregates logs by week
   - Shows tonnage (prescribed vs actual) and RPE
   - Includes diagnostics for missing CalcLoad / %1RM coverage
   - Guard rails: empty logs, missing sheets, no programs yet

3. **03_Coach.py** – Program / Workout Builder (was Builder)
   - Create new programs (append to Programs sheet)
   - Create new sessions or clone existing ones
   - Prescribe movements with sets/reps/%1RM or load
   - Live %1RM → CalcLoad preview (if OneRM sheet is populated)
   - Append-only writes to `Programs`, `Sessions` and `SessionMovements` sheets
   - Guard rails: missing MovementIDs, missing OneRMs, blank sessions

---

## File structure
- `data/settings.json` → configuration (paths, sheet names, defaults)
- `data/CrossFit_AI_Coach_Baseline.xlsx` → main Excel workbook
- `SessionsMovements_Log.csv` → append-only log of training actuals
- `Sessions_Closed.csv` → registry of closed sessions

## Running the apps
Use Streamlit to run any page directly:

```bash
streamlit run 01_Athlete.py
streamlit run 02_Dashboard.py
streamlit run 03_Coach.py
```

All three apps share helpers from **coach_io.py**, which contains:
- Path and settings resolution (`derive_paths`, `load_settings`)
- Log helpers (`append_log`, `read_log`, `ensure_log_csv`)
- Excel sheet append (`append_rows_to_sheet`)
- Prescribing utilities (`pick_series`, `parse_pct`, `calc_load`, `round_to_increment`)
- ID utilities (`new_session_id`)

---

## Next Steps
- (Optional) Add `BatchID` to log submissions for grouping.
- Extend Builder/Coach with templates (e.g., 5×5 @75%).
- Dashboard: add “Created vs Completed” analysis.

Enjoy building, logging & coaching 💪
