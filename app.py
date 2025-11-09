from __future__ import annotations

import csv
import json
import os
import sqlite3
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, jsonify, render_template, request, redirect, url_for, flash

SCHEDULE_FILE = os.environ.get("SCHEDULE_FILE", "teachers_schedule.json")
DB_FILE = os.environ.get("DB_FILE", "substitutions.db")
CSV_FILE = os.environ.get("CSV_FILE", "substitutions.csv")

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-key")


# ----------------------------- Data & Persistence -----------------------------

class ScheduleError(Exception):
    pass


def load_schedule(path: str = SCHEDULE_FILE) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise ScheduleError(f"Schedule file not found: {path}")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        raise ScheduleError(f"Error reading schedule JSON: {e}")
    if not isinstance(data, dict) or "metadata" not in data or "teachers" not in data:
        raise ScheduleError("Invalid schedule: missing 'metadata' or 'teachers'")
    md = data.get("metadata", {})
    days = md.get("days")
    periods = md.get("periods")
    if not isinstance(days, list) or not isinstance(periods, list):
        raise ScheduleError("Invalid schedule: 'metadata.days' or 'metadata.periods' malformed")
    for p in periods:
        if not isinstance(p, dict) or "code" not in p or "time" not in p:
            raise ScheduleError("Each period must have 'code' and 'time'")
    teachers = data.get("teachers", {})
    if not isinstance(teachers, dict):
        raise ScheduleError("Invalid schedule: 'teachers' must be an object")
    return data


def get_days(schedule: Dict[str, Any]) -> List[str]:
    return list(schedule.get("metadata", {}).get("days", []))


def get_periods(schedule: Dict[str, Any]) -> List[Dict[str, str]]:
    return list(schedule.get("metadata", {}).get("periods", []))


def build_period_index_map(periods: List[Dict[str, str]]) -> Dict[str, int]:
    return {p["code"]: i for i, p in enumerate(periods)}


def index_for_period(period_code: str, period_index_map: Dict[str, int]) -> int:
    if period_code not in period_index_map:
        raise ValueError(f"Unknown period code: {period_code}")
    return period_index_map[period_code]


def init_db(db_path: str = DB_FILE) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            day TEXT NOT NULL,
            period_code TEXT NOT NULL,
            period_time TEXT NOT NULL,
            absent_teacher TEXT NOT NULL,
            assigned_teacher TEXT NOT NULL,
            class_if_known TEXT,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, day, period_code, absent_teacher)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_assignments_assigned_teacher ON assignments(assigned_teacher)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_assignments_date ON assignments(date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_assignments_absent_teacher ON assignments(absent_teacher)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    conn.commit()
    return conn


def export_csv(conn: sqlite3.Connection, path: str = CSV_FILE) -> None:
    cur = conn.execute(
        """
        SELECT date, day, period_code, period_time, absent_teacher, assigned_teacher, class_if_known, notes
        FROM assignments
        ORDER BY date DESC, day, period_code
        """
    )
    rows = cur.fetchall()
    headers = [
        "date",
        "day",
        "period_code",
        "period_time",
        "absent_teacher",
        "assigned_teacher",
        "class_if_known",
        "notes",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)


def was_chosen_on_date(conn: sqlite3.Connection, teacher: str, d: date) -> bool:
    cur = conn.execute("SELECT 1 FROM assignments WHERE assigned_teacher=? AND date=? LIMIT 1", (teacher, d.isoformat()))
    return cur.fetchone() is not None


def prior_assignment_count(conn: sqlite3.Connection, teacher: str) -> int:
    cur = conn.execute("SELECT COUNT(1) FROM assignments WHERE assigned_teacher=?", (teacher,))
    r = cur.fetchone()
    return int(r[0] if r and r[0] is not None else 0)


def recent_assignment_count(conn: sqlite3.Connection, teacher: str, days_back_inclusive: int = 5) -> int:
    today = date.today()
    start = today - timedelta(days=days_back_inclusive)
    cur = conn.execute(
        "SELECT COUNT(1) FROM assignments WHERE assigned_teacher=? AND date>=? AND date<=?",
        (teacher, start.isoformat(), today.isoformat()),
    )
    r = cur.fetchone()
    return int(r[0] if r and r[0] is not None else 0)


def resolve_class_for(schedules: Dict[str, Any], period_index_map: Dict[str, int], absent_teacher: str, day: str, period_code: str) -> Optional[str]:
    try:
        idx = index_for_period(period_code, period_index_map)
    except Exception:
        return None
    teacher_sched = schedules.get("teachers", {}).get(absent_teacher, {})
    arr = teacher_sched.get(day, [])
    if idx < len(arr):
        val = arr[idx]
        return val if isinstance(val, str) and val.strip() else None
    return None


def periods_count_for_day(schedules: Dict[str, Any], teacher: str, day: str) -> int:
    teacher_sched = schedules.get("teachers", {}).get(teacher, {})
    arr = teacher_sched.get(day, [])
    busy = sum(1 for v in arr if isinstance(v, str) and v.strip())
    return busy


def teacher_teaches_class(schedules: Dict[str, Any], teacher: str, class_code: str, days: Optional[List[str]] = None) -> bool:
    if not class_code or not class_code.strip():
        return False
    teacher_sched = schedules.get("teachers", {}).get(teacher, {})
    if not teacher_sched:
        return False
    if days is None:
        days = list(schedules.get("metadata", {}).get("days", []))
    for d in days:
        arr = teacher_sched.get(d, [])
        for v in arr:
            if isinstance(v, str) and v.strip() == class_code:
                return True
    return False


def classes_for_teacher(schedules: Dict[str, Any], teacher: str, days: Optional[List[str]] = None) -> List[str]:
    """Return a de-duplicated list of class codes the teacher teaches.

    If days is provided, only those days are considered. Otherwise, all schedule days are scanned.
    """
    classes: List[str] = []
    teacher_sched = schedules.get("teachers", {}).get(teacher, {})
    if not isinstance(teacher_sched, dict) or not teacher_sched:
        return classes
    if days is None:
        days = list(schedules.get("metadata", {}).get("days", []))
    for d in days:
        arr = teacher_sched.get(d, []) or []
        for v in arr:
            if isinstance(v, str):
                v = v.strip()
                if v and v not in classes:
                    classes.append(v)
    return classes

def available_teachers(schedules: Dict[str, Any], period_index_map: Dict[str, int], day: str, period_code: str, absent_teacher: str, conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    idx = index_for_period(period_code, period_index_map)
    free: List[Dict[str, Any]] = []
    teachers = schedules.get("teachers", {})
    yday = date.today() - timedelta(days=1)
    for t, sched in teachers.items():
        if t == absent_teacher:
            continue
        arr = sched.get(day, [])
        val = arr[idx] if idx < len(arr) else ""
        if val == "":
            info = {
                "teacher": t,
                "chosen_yesterday": was_chosen_on_date(conn, t, yday),
                "prior_count": prior_assignment_count(conn, t),
            }
            free.append(info)
    # sort by chosen_yesterday (False first), prior_count, name
    free.sort(key=lambda x: (x["chosen_yesterday"], x["prior_count"], x["teacher"]))
    return free


# ----------------------------- App Bootstrap ---------------------------------

conn = init_db()
schedules = load_schedule(SCHEDULE_FILE)
days = get_days(schedules)
periods = get_periods(schedules)
period_index_map = build_period_index_map(periods)


# --------------------------------- Views -------------------------------------

@app.get("/")
def index():
    # Defaults
    day = request.args.get("day") or datetime.today().strftime("%A")
    absent = request.args.get("absent") or (sorted(schedules.get("teachers", {}).keys())[:1] or [""])[0]
    selected_period = request.args.get("period")
    avail = []
    # Build period dropdown to only show engaged periods for the selected absent teacher
    engaged_periods: List[Dict[str, Any]] = []
    try:
        day_map = schedules.get("teachers", {}).get(absent, {}) or {}
        arr = day_map.get(day, []) or []
        for i, v in enumerate(arr):
            if i < len(periods) and isinstance(v, str) and v.strip():
                engaged_periods.append(periods[i])
    except Exception:
        engaged_periods = []
    class_code = ""
    absent_classes_today: List[str] = []
    if day and absent and selected_period:
        class_code = resolve_class_for(schedules, period_index_map, absent, day, selected_period) or ""
        # classes the absent teacher handles on the selected day (for quick reference)
        absent_classes_today = classes_for_teacher(schedules, absent, [day])
        base = available_teachers(schedules, period_index_map, day, selected_period, absent, conn)
        # filter off-day teachers (0 periods that day)
        base = [r for r in base if periods_count_for_day(schedules, r["teacher"], day) > 0]
        # attach fit and load
        total_periods = len(periods)
        for r in base:
            busy = periods_count_for_day(schedules, r["teacher"], day)
            fits = teacher_teaches_class(schedules, r["teacher"], class_code, days) if class_code else False
            avail.append({
                **r,
                "fit": ("teaches " + class_code) if fits else (("not teaching " + class_code) if class_code else ""),
                "load": f"{busy}/{total_periods}",
                "teaches": classes_for_teacher(schedules, r["teacher"])  # across all days
            })
        # preferred ordering
        if class_code:
            avail = sorted(avail, key=lambda x: (x["fit"].startswith("teaches"), x["load"], x["chosen_yesterday"], x["prior_count"], x["teacher"]), reverse=True)
        else:
            avail = sorted(avail, key=lambda x: (x["load"], x["chosen_yesterday"], x["prior_count"], x["teacher"]))
    return render_template(
        "index.html",
        days=days,
        periods=periods,
        teachers=sorted(schedules["teachers"].keys()),
        avail=avail,
        selected_day=day,
        selected_absent=absent,
        selected_period=selected_period,
        class_code=class_code,
        absent_classes_today=absent_classes_today,
        periods_for_absent=engaged_periods,
    )


@app.post("/assign")
def assign():
    payload = request.form
    day = payload.get("day", "").strip()
    absent = payload.get("absent", "").strip()
    period_code = payload.get("period", "").strip()
    assigned_teacher = payload.get("assigned", "").strip()
    notes = payload.get("notes", "").strip()
    if not (day and absent and period_code and assigned_teacher):
        flash("Missing required fields", "error")
        return redirect(url_for("index"))

    # optional warning
    cnt = recent_assignment_count(conn, assigned_teacher, 5)
    if cnt >= int(get_setting("warn_threshold", "2")) and get_setting("warn_repeats", "1") == "1":
        flash(f"Warning: {assigned_teacher} has been chosen {cnt} time(s) in the last 5 days.", "warning")

    period_time = next((p["time"] for p in periods if p["code"] == period_code), "")
    class_if_known = resolve_class_for(schedules, period_index_map, absent, day, period_code) or ""
    row = {
        "date": date.today().isoformat(),
        "day": day,
        "period_code": period_code,
        "period_time": period_time,
        "absent_teacher": absent,
        "assigned_teacher": assigned_teacher,
        "class_if_known": class_if_known,
        "notes": notes,
    }
    try:
        cur = conn.execute(
            """
            INSERT INTO assignments (date, day, period_code, period_time, absent_teacher, assigned_teacher, class_if_known, notes)
            VALUES (:date, :day, :period_code, :period_time, :absent_teacher, :assigned_teacher, :class_if_known, :notes)
            """,
            row,
        )
        conn.commit()
        flash("Substitute assigned", "success")
    except sqlite3.IntegrityError:
        # overwrite
        conn.execute(
            "DELETE FROM assignments WHERE date=? AND day=? AND period_code=? AND absent_teacher=?",
            (row["date"], row["day"], row["period_code"], row["absent_teacher"]),
        )
        conn.commit()
        conn.execute(
            """
            INSERT INTO assignments (date, day, period_code, period_time, absent_teacher, assigned_teacher, class_if_known, notes)
            VALUES (:date, :day, :period_code, :period_time, :absent_teacher, :assigned_teacher, :class_if_known, :notes)
            """,
            row,
        )
        conn.commit()
        flash("Existing assignment overwritten", "success")

    try:
        export_csv(conn, CSV_FILE)
    except Exception as e:
        flash(f"CSV export failed: {e}", "error")
    return redirect(url_for("index", day=day, absent=absent, period=period_code))


@app.get("/history")
def history():
    q = {
        "from": request.args.get("from") or "",
        "to": request.args.get("to") or "",
        "teacher": request.args.get("teacher") or "Any",
        "absent": request.args.get("absent") or "Any",
        "day": request.args.get("day") or "Any",
        "period": request.args.get("period") or "Any",
    }
    where = []
    params: List[Any] = []
    if q["from"]:
        where.append("date >= ?"); params.append(q["from"])
    if q["to"]:
        where.append("date <= ?"); params.append(q["to"])
    if q["teacher"] != "Any":
        where.append("assigned_teacher = ?"); params.append(q["teacher"])
    if q["absent"] != "Any":
        where.append("absent_teacher = ?"); params.append(q["absent"])
    if q["day"] != "Any":
        where.append("day = ?"); params.append(q["day"])
    if q["period"] != "Any":
        where.append("period_code = ?"); params.append(q["period"])

    sql = ("SELECT id, date, day, period_code, period_time, absent_teacher, assigned_teacher, class_if_known, notes FROM assignments")
    if where: sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY date DESC, day, period_code"
    cur = conn.execute(sql, params)
    rows = cur.fetchall()

    teachers = sorted(schedules.get("teachers", {}).keys())
    period_codes = [p["code"] for p in periods]

    return render_template("history.html", rows=rows, q=q, teachers=teachers, days=days, periods=periods, period_codes=period_codes)


@app.get("/routine")
def routine():
    # Choose day: query param or today's weekday name
    selected_day = request.args.get("day") or datetime.today().strftime("%A")
    # Build a row per teacher with values per period
    teacher_names = sorted(schedules.get("teachers", {}).keys())
    period_count = len(periods)
    rows: List[Dict[str, Any]] = []
    for t in teacher_names:
        day_map = schedules.get("teachers", {}).get(t, {})
        arr = day_map.get(selected_day, []) or []
        vals = [(arr[i] if i < len(arr) and isinstance(arr[i], str) else "") for i in range(period_count)]
        rows.append({"teacher": t, "vals": vals})
    return render_template("routine.html", days=days, periods=periods, rows=rows, selected_day=selected_day)


@app.get("/export/csv")
def export():
    export_csv(conn, CSV_FILE)
    return redirect(url_for("static_csv"))


def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    cur = conn.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = cur.fetchone()
    return row[0] if row else default


def set_setting(key: str, value: str) -> None:
    conn.execute("REPLACE INTO settings(key, value) VALUES(?, ?)", (key, value))
    conn.commit()


@app.get("/settings")
def settings_view():
    # off days is not strictly enforced here; just a stored value for UI/reference
    warn_repeats = get_setting("warn_repeats", "1")
    warn_threshold = get_setting("warn_threshold", "2")
    off_days = get_setting("off_days", "[]")
    try:
        off_days = json.loads(off_days)
    except Exception:
        off_days = []
    return render_template("settings.html",
                           schedule_path=os.path.abspath(SCHEDULE_FILE),
                           warn_repeats=warn_repeats=="1",
                           warn_threshold=int(warn_threshold or "2"),
                           off_days=", ".join(off_days),
                           days=days, periods=periods)


@app.post("/settings")
def settings_save():
    warn_repeats = request.form.get("warn_repeats") == "on"
    warn_threshold = request.form.get("warn_threshold") or "2"
    off_days = [d.strip() for d in (request.form.get("off_days") or "").split(",") if d.strip()]
    set_setting("warn_repeats", "1" if warn_repeats else "0")
    set_setting("warn_threshold", str(max(1, int(warn_threshold))))

    try:
        json.dumps(off_days)  # validate
        set_setting("off_days", json.dumps(off_days))
    except Exception:
        set_setting("off_days", "[]")
    flash("Settings saved", "success")
    return redirect(url_for("settings_view"))


# ------------------------------ Template Filters ------------------------------

@app.template_filter("badge")
def badge(val: str) -> str:
    if val.lower().startswith("teaches"):
        return "badge badge-ok"
    if val.lower().startswith("not"):
        return "badge badge-warn"
    return ""


# --------------------------------- Static CSV --------------------------------

# Serve CSV via static route
@app.route("/static/" + CSV_FILE)
def static_csv():
    if not os.path.exists(CSV_FILE):
        export_csv(conn, CSV_FILE)
    from flask import send_file
    return send_file(CSV_FILE, as_attachment=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
