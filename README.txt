# Substitute Finder - EMRS (Web)

A Flask web version of your desktop app. Drop `teachers_schedule.json` in the same folder,
run the server, and manage substitute assignments in the browser. Exports a `substitutions.csv` too.

## Quickstart

```bash
cd substitute_finder_web
python -m venv .venv && . .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app.py
# open http://localhost:5000
```

- Use **Find Substitute**: choose Day, Absent teacher, Period; shows available teachers with class-fit & load.
- Click **Assign** to record. Re-assigning the same slot overwrites.
- **History** lets you filter by date, teacher, day, period. Download CSV anytime.
- **Settings**: warn-on-repeat, threshold, and off-day note. (Schedule path is displayed.)

Data persists in `substitutions.db` (SQLite) and `substitutions.csv` in the app folder.

