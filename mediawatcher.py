#!/usr/bin/env python3
"""
mediawatcher.py — MKV pipeline orchestrator daemon
Watches directories for new/changed MKV files, runs the processing pipeline,
and serves a web UI showing live output and job history.
"""

import os
import sys
import time
import json
import queue
import sqlite3
import hashlib
import logging
import threading
import subprocess
import re
import signal
from datetime import datetime
from pathlib import Path
from collections import deque

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
    from flask import Flask, render_template, Response, jsonify, stream_with_context
    from werkzeug.middleware.dispatcher import DispatcherMiddleware
    from werkzeug.wrappers import Response as WerkzeugResponse
    from waitress import serve as waitress_serve
except ImportError:
    print("Missing dependencies. Run: pip3 install watchdog flask waitress")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Configuration  (override via config.json next to this script)
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "config.json"

DEFAULT_CONFIG = {
    # watch_dirs: list of directory paths OR objects with per-dir pipeline override.
    # Simple form:   "/Volumes/Media/Movies"
    # Override form: {"path": "/Volumes/Media/Movies", "pipeline": ["script1.sh", "script2.sh"]}
    # Falls back to top-level "pipeline" if no per-dir pipeline is set.
    "watch_dirs": [],
    "scripts_dir": str(Path.home() / "scripts"),
    "pipeline": [
        "mkv_original_audio_subtitle_tracks.sh",
        "convert_to_dv81_hdr10only.sh --overwrite",
        "convert_4k_to_1080p.sh"
    ],
    # dry_run_flags: maps each script filename to the flag(s) that activate its
    # dry-run mode. When "dry_run": true, these flags are injected automatically
    # before the file path argument. Scripts not listed here are skipped entirely
    # in dry-run mode (safest default for unknown scripts).
    "dry_run_flags": {
        "mkv_original_audio_subtitle_tracks.sh": "--dry-run",
        "convert_to_dv81_hdr10only.sh":          "--dry-run",
        "convert_4k_to_1080p.sh":                "-n"
    },
    "dry_run": False,
    "web_host": "127.0.0.1",
    "web_port": 5678,
    "web_root": "/",

    "settle_seconds": 5,
    "log_level": "INFO",
    "max_history": 200,
    "env_passthrough": ["HOME", "PATH", "TMDB_API_TOKEN", "TMDB_TOKEN",
                        "JELLYFIN_URL", "JELLYFIN_TOKEN"]
}


def load_config():
    cfg = dict(DEFAULT_CONFIG)
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE) as f:
                cfg.update(json.load(f))
        except Exception as e:
            print(f"[WARN] Could not parse config.json: {e}")
    return cfg


CFG = load_config()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_FILE = SCRIPT_DIR / "logs" / "mediawatcher.log"
LOG_FILE.parent.mkdir(exist_ok=True)

logging.basicConfig(
    level=getattr(logging, CFG["log_level"], logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger("mediawatcher")

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

DB_PATH = SCRIPT_DIR / "db" / "jobs.db"
DB_PATH.parent.mkdir(exist_ok=True)

_db_lock = threading.Lock()


def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path   TEXT NOT NULL,
                file_hash   TEXT,
                status      TEXT NOT NULL DEFAULT 'queued',
                queued_at   TEXT NOT NULL,
                started_at  TEXT,
                finished_at TEXT,
                current_step INTEGER DEFAULT 0,
                total_steps  INTEGER DEFAULT 0,
                error_msg    TEXT
            );
            CREATE TABLE IF NOT EXISTS job_logs (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id   INTEGER NOT NULL REFERENCES jobs(id),
                step     INTEGER NOT NULL DEFAULT 0,
                ts       TEXT NOT NULL,
                line     TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_job_logs_job_id ON job_logs(job_id);
        """)
        # Reset any jobs that were queued or running when the service last stopped —
        # they were interrupted and will never complete on their own.
        affected = conn.execute(
            """UPDATE jobs SET status='interrupted', finished_at=?, error_msg=?
               WHERE status IN ('queued', 'running')""",
            (_now(), "Service was stopped before this job could complete.")
        ).rowcount
        if affected:
            log.warning("Reset %d interrupted job(s) to 'interrupted' status on startup.", affected)


def db_insert_job(file_path, total_steps):
    with _db_lock, get_db() as conn:
        cur = conn.execute(
            "INSERT INTO jobs (file_path, queued_at, status, total_steps) VALUES (?,?,?,?)",
            (str(file_path), _now(), "queued", total_steps)
        )
        return cur.lastrowid


def db_update_job(job_id, **kwargs):
    if not kwargs:
        return
    sets = ", ".join(f"{k}=?" for k in kwargs)
    vals = list(kwargs.values()) + [job_id]
    with _db_lock, get_db() as conn:
        conn.execute(f"UPDATE jobs SET {sets} WHERE id=?", vals)


def db_append_log(job_id, step, line):
    with _db_lock, get_db() as conn:
        conn.execute(
            "INSERT INTO job_logs (job_id, step, ts, line) VALUES (?,?,?,?)",
            (job_id, step, _now(), line)
        )


def db_get_jobs(limit=CFG["max_history"]):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


def db_get_job(job_id):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    return dict(row) if row else None


def db_get_logs(job_id):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT step, ts, line FROM job_logs WHERE job_id=? ORDER BY id",
            (job_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def _now():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


# ---------------------------------------------------------------------------
# Live output bus  (SSE broadcast)
# ---------------------------------------------------------------------------

class OutputBus:
    """Fan-out: pipeline writes lines here, SSE subscribers read them."""

    def __init__(self):
        self._subs = []  # type: list
        self._lock = threading.Lock()
        # Ring buffer so late-joiners get recent context
        self._recent = deque(maxlen=500)

    def subscribe(self):
        q = queue.Queue()
        with self._lock:
            # Send recent history to new subscriber
            for item in self._recent:
                q.put(item)
            self._subs.append(q)
        return q

    def unsubscribe(self, q):
        with self._lock:
            try:
                self._subs.remove(q)
            except ValueError:
                pass

    def publish(self, event: dict):
        data = json.dumps(event)
        with self._lock:
            self._recent.append(data)
            for q in self._subs:
                try:
                    q.put_nowait(data)
                except queue.Full:
                    pass


BUS = OutputBus()

# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

# Files currently in-flight (path → job_id)
_in_flight = {}  # type: dict
_in_flight_lock = threading.Lock()

# Pending settle timers (path → timer)
_settle_timers = {}  # type: dict
_settle_lock = threading.Lock()

# FIFO job queue
JOB_QUEUE: queue.Queue = queue.Queue()

# Jobs cancelled before the worker picked them up
_cancelled: set = set()
_cancelled_lock = threading.Lock()


def _resolve_pipeline(file_path: Path):
    """Return the pipeline list for the given file, using per-dir override if configured."""
    watch_dirs = CFG.get("watch_dirs", [])
    for entry in watch_dirs:
        if isinstance(entry, dict):
            dp = Path(entry["path"]).expanduser().resolve()
            if "pipeline" in entry:
                try:
                    if file_path.resolve().is_relative_to(dp):
                        return entry["pipeline"]
                except AttributeError:
                    # Python < 3.9: manual check
                    try:
                        file_path.resolve().relative_to(dp)
                        return entry["pipeline"]
                    except ValueError:
                        pass
    return CFG.get("pipeline", [])


def _build_env():
    env = {}
    for key in CFG["env_passthrough"]:
        if key in os.environ:
            env[key] = os.environ[key]
    # Ensure a sensible PATH that includes common tool locations
    if "PATH" not in env:
        env["PATH"] = "/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin"
    return env


def run_pipeline(job_id: int, file_path: Path):
    scripts_dir = Path(CFG["scripts_dir"])
    pipeline = _resolve_pipeline(file_path)
    dry_run = bool(CFG.get("dry_run", False))
    dry_run_flags: dict = CFG.get("dry_run_flags", {})
    env = _build_env()

    db_update_job(job_id, status="running", started_at=_now())

    def emit(line: str, step: int = 0, kind: str = "log"):
        db_append_log(job_id, step, line)
        BUS.publish({
            "type": kind,
            "job_id": job_id,
            "step": step,
            "ts": _now(),
            "line": line,
            "file": str(file_path)
        })
        log.debug("[job %d step %d] %s", job_id, step, line)

    emit(f"{'='*60}", kind="separator")
    emit(f"▶ Starting pipeline for: {file_path}", kind="info")
    emit(f"  Job ID : {job_id}")
    emit(f"  Steps  : {len(pipeline)}")
    emit(f"  Mode   : {'DRY RUN — no files will be modified' if dry_run else 'LIVE'}", kind="info" if not dry_run else "ok")
    emit(f"  Time   : {_now()}")
    emit(f"{'='*60}")

    for step_idx, script_entry in enumerate(pipeline, start=1):
        parts = script_entry.split()
        script_name = parts[0]
        extra_args = parts[1:]

        # ── Dry-run flag injection ──────────────────────────────────────────
        if dry_run:
            if script_name in dry_run_flags:
                flag_str = dry_run_flags[script_name]
                # Flags may be a space-separated string e.g. "--dry-run --verbose"
                dry_flags = flag_str.split()
                # Insert after any existing extra_args (flags before file path)
                extra_args = extra_args + [f for f in dry_flags if f not in extra_args]
            else:
                emit(
                    f"⚠ Skipping step {step_idx} ({script_name}) in dry-run mode "
                    f"— no dry_run_flags entry defined for this script.",
                    step=step_idx, kind="error"
                )
                continue
        # ───────────────────────────────────────────────────────────────────

        script_path = scripts_dir / script_name
        if not script_path.exists():
            msg = f"✗ Script not found: {script_path}"
            emit(msg, step=step_idx, kind="error")
            db_update_job(job_id, status="failed", finished_at=_now(),
                          error_msg=msg, current_step=step_idx)
            BUS.publish({"type": "job_done", "job_id": job_id,
                         "status": "failed", "file": str(file_path)})
            return

        db_update_job(job_id, current_step=step_idx)
        emit(f"\n── Step {step_idx}/{len(pipeline)}: {script_name} ──",
             step=step_idx, kind="step_start")

        cmd = ["bash", str(script_path)] + extra_args + [str(file_path)]
        emit(f"  $ {' '.join(cmd)}", step=step_idx)

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=env,
                text=True,
                bufsize=1
            )

            ansi_escape = re.compile(r'\x1b\[[0-9;]*[mGKHF]')
            for raw_line in iter(proc.stdout.readline, ""):
                line = ansi_escape.sub("", raw_line.rstrip("\n"))
                emit(line, step=step_idx)

            proc.wait()

        except Exception as exc:
            msg = f"✗ Failed to launch {script_name}: {exc}"
            emit(msg, step=step_idx, kind="error")
            db_update_job(job_id, status="failed", finished_at=_now(),
                          error_msg=msg, current_step=step_idx)
            BUS.publish({"type": "job_done", "job_id": job_id,
                         "status": "failed", "file": str(file_path)})
            return

        if proc.returncode != 0:
            msg = (f"✗ {script_name} exited with code {proc.returncode} "
                   f"— aborting pipeline for this file.")
            emit(msg, step=step_idx, kind="error")
            db_update_job(job_id, status="failed", finished_at=_now(),
                          error_msg=msg, current_step=step_idx)
            BUS.publish({"type": "job_done", "job_id": job_id,
                         "status": "failed", "file": str(file_path)})
            return

        emit(f"  ✓ {script_name} completed successfully.", step=step_idx, kind="ok")

    emit(f"\n{'='*60}")
    emit(f"✓ Pipeline complete for: {file_path}", kind="ok")
    emit(f"  Finished: {_now()}")
    emit(f"{'='*60}")

    db_update_job(job_id, status="done", finished_at=_now(),
                  current_step=len(pipeline))
    BUS.publish({"type": "job_done", "job_id": job_id,
                 "status": "done", "file": str(file_path)})


def worker_thread():
    """Drain JOB_QUEUE sequentially."""
    while True:
        item = JOB_QUEUE.get()
        if item is None:
            break
        job_id, file_path = item
        with _cancelled_lock:
            if job_id in _cancelled:
                _cancelled.discard(job_id)
                log.info("Skipping cancelled job #%d", job_id)
                with _in_flight_lock:
                    _in_flight.pop(str(file_path), None)
                JOB_QUEUE.task_done()
                continue
        try:
            run_pipeline(job_id, file_path)
        except Exception as exc:
            log.exception("Unexpected error in pipeline for %s", file_path)
            db_update_job(job_id, status="failed", finished_at=_now(),
                          error_msg=str(exc))
        finally:
            with _in_flight_lock:
                _in_flight.pop(str(file_path), None)
            JOB_QUEUE.task_done()


def enqueue_file(file_path: Path):
    path_str = str(file_path)
    with _in_flight_lock:
        if path_str in _in_flight:
            log.info("Already queued/running: %s — skipping duplicate", file_path.name)
            return

    # Also check the DB for any queued or running job for this file —
    # guards against duplicates that slip through before in-flight is set
    with get_db() as conn:
        row = conn.execute(
            "SELECT id FROM jobs WHERE file_path=? AND status IN ('queued','running')",
            (path_str,)
        ).fetchone()
    if row:
        log.info("Job #%d already queued/running for: %s — skipping", row[0], file_path.name)
        return

    job_id = db_insert_job(file_path, total_steps=len(_resolve_pipeline(file_path)))
    with _in_flight_lock:
        _in_flight[path_str] = job_id

    log.info("Enqueuing job %d for: %s", job_id, file_path.name)
    BUS.publish({"type": "job_queued", "job_id": job_id, "file": path_str, "ts": _now()})
    JOB_QUEUE.put((job_id, file_path))


# ---------------------------------------------------------------------------
# FSEvents watcher
# ---------------------------------------------------------------------------

class MKVHandler(FileSystemEventHandler):
    """Debounce FS events and enqueue settled MKV files."""

    def __init__(self, settle_secs: float):
        self.settle = settle_secs

    def _schedule(self, path_str: str):
        p = Path(path_str)
        if p.suffix.lower() != ".mkv":
            return
        if ".tmp" in p.name:
            return
        if not p.exists():
            return
        # Suppress events for files currently being processed by the pipeline
        with _in_flight_lock:
            if path_str in _in_flight:
                log.debug("Ignoring event for in-flight file: %s", p.name)
                return

        with _settle_lock:
            existing = _settle_timers.pop(path_str, None)
            if existing:
                existing.cancel()
            t = threading.Timer(self.settle, self._fire, args=(path_str,))
            _settle_timers[path_str] = t
            t.start()
            log.debug("Settle timer started for %s (%.1fs)", p.name, self.settle)

    def _fire(self, path_str: str):
        with _settle_lock:
            _settle_timers.pop(path_str, None)
        p = Path(path_str)
        if not p.exists():
            return

        # For large files, confirm the size has stopped changing before enqueuing.
        # Sample twice with a short gap — if still growing, reschedule the timer.
        try:
            size1 = p.stat().st_size
        except OSError:
            return
        time.sleep(2)
        try:
            size2 = p.stat().st_size
        except OSError:
            return

        if size2 != size1:
            log.info("File still growing (%s): %d → %d bytes, rescheduling…",
                     p.name, size1, size2)
            with _settle_lock:
                t = threading.Timer(self.settle, self._fire, args=(path_str,))
                _settle_timers[path_str] = t
                t.start()
            return

        log.info("Detected settled MKV: %s", p.name)
        enqueue_file(p)

    def on_created(self, event):
        if not event.is_directory:
            self._schedule(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            self._schedule(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            self._schedule(event.dest_path)


WATCH_RECHECK_SECS = 30


def _reconcile_watches(observer, handler, watches: dict, missing: set):
    """Schedule watch dirs that have appeared; reschedule ones whose volume was remounted.

    External volumes (e.g. a USB drive) often mount after the service starts at
    boot, and an FSEvents stream on an unmounted path goes dead, so this runs
    periodically rather than once at startup.
    """
    for entry in CFG.get("watch_dirs", []):
        d = entry["path"] if isinstance(entry, dict) else entry
        pipeline = entry.get("pipeline") if isinstance(entry, dict) else None
        dp = Path(d).expanduser()
        key = str(dp)
        try:
            dev = dp.stat().st_dev if dp.is_dir() else None
        except OSError:
            dev = None

        current = watches.get(key)
        if current and current[1] != dev:
            try:
                observer.unschedule(current[0])
            except Exception:
                pass
            del watches[key]
            log.warning("Watch dir unmounted or replaced, dropping watch: %s", dp)

        if dev is None:
            if key not in missing:
                log.warning("Watch dir does not exist (will keep checking every %ds): %s",
                            WATCH_RECHECK_SECS, dp)
                missing.add(key)
            continue

        if key not in watches:
            watches[key] = (observer.schedule(handler, key, recursive=True), dev)
            missing.discard(key)
            if pipeline:
                log.info("Watching: %s (custom pipeline: %d steps)", dp, len(pipeline))
            else:
                log.info("Watching: %s (default pipeline)", dp)


def start_watchers():
    observer = Observer()
    handler = MKVHandler(settle_secs=CFG["settle_seconds"])

    if not CFG.get("watch_dirs", []):
        log.warning("No watch_dirs configured — watcher is idle.")
        log.warning("Add directories to config.json and restart.")
        observer.start()
        return observer

    watches = {}
    missing = set()
    _reconcile_watches(observer, handler, watches, missing)
    observer.start()

    def recheck_loop():
        while observer.is_alive():
            time.sleep(WATCH_RECHECK_SECS)
            try:
                _reconcile_watches(observer, handler, watches, missing)
            except Exception:
                log.exception("Error re-checking watch dirs")

    threading.Thread(target=recheck_loop, daemon=True, name="watch-recheck").start()
    return observer


# ---------------------------------------------------------------------------
# Flask web UI
# ---------------------------------------------------------------------------

app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["SECRET_KEY"] = os.urandom(24)
app.config["APPLICATION_ROOT"] = CFG.get("web_root", "/")
app.config["PREFERRED_URL_SCHEME"] = "http"


@app.route("/")
def index():
    jobs = db_get_jobs(50)
    pipeline = CFG["pipeline"]
    watch_dirs = CFG.get("watch_dirs", [])
    return render_template("index.html",
                           jobs=jobs,
                           pipeline=pipeline,
                           watch_dirs=watch_dirs,
                           scripts_dir=CFG["scripts_dir"],
                           dry_run=CFG.get("dry_run", False))


@app.route("/job/<int:job_id>")
def job_detail(job_id):
    job = db_get_job(job_id)
    if not job:
        return "Job not found", 404
    logs = db_get_logs(job_id)
    pipeline = _resolve_pipeline(Path(job["file_path"]))
    return render_template("job.html", job=job, logs=logs, pipeline=pipeline)


@app.route("/api/jobs")
def api_jobs():
    return jsonify(db_get_jobs(50))


@app.route("/api/job/<int:job_id>/logs")
def api_job_logs(job_id):
    return jsonify(db_get_logs(job_id))


@app.route("/api/queue_size")
def api_queue_size():
    return jsonify({"queued": JOB_QUEUE.qsize(),
                    "in_flight": len(_in_flight)})


@app.route("/api/job/<int:job_id>/cancel", methods=["POST"])
def api_cancel(job_id):
    job = db_get_job(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    if job["status"] != "queued":
        return jsonify({"error": "Only queued jobs can be cancelled"}), 400
    with _cancelled_lock:
        _cancelled.add(job_id)
    with _in_flight_lock:
        _in_flight.pop(job["file_path"], None)
    db_update_job(job_id, status="cancelled", finished_at=_now(),
                  error_msg="Cancelled manually.")
    log.info("Job #%d cancelled by user", job_id)
    return jsonify({"ok": True})


@app.route("/api/job/<int:job_id>/rerun", methods=["POST"])
def api_rerun(job_id):
    job = db_get_job(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    file_path = Path(job["file_path"])
    if not file_path.exists():
        return jsonify({"error": f"File no longer exists: {file_path}"}), 400
    enqueue_file(file_path)
    return jsonify({"ok": True, "file": str(file_path)})


@app.route("/stream")
def stream():
    """SSE endpoint — streams live pipeline output to the browser."""
    def generate():
        q = BUS.subscribe()
        try:
            # Keep-alive heartbeat every 15s
            last = time.time()
            while True:
                try:
                    data = q.get(timeout=15)
                    yield f"data: {data}\n\n"
                    last = time.time()
                except queue.Empty:
                    yield ": heartbeat\n\n"
        except GeneratorExit:
            pass
        finally:
            BUS.unsubscribe(q)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no"
        }
    )


@app.route("/config")
def config_page():
    env_values = {
        key: os.environ.get(key) for key in CFG.get("env_passthrough", [])
    }
    return render_template("config.html", cfg=CFG, config_path=str(CONFIG_FILE),
                           dry_run=CFG.get("dry_run", False),
                           env_values=env_values)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    init_db()

    # Worker thread (serial pipeline execution)
    worker = threading.Thread(target=worker_thread, daemon=True, name="pipeline-worker")
    worker.start()

    # FS watcher
    observer = start_watchers()

    host = CFG["web_host"]
    port = CFG["web_port"]
    log.info("Web UI at http://%s:%d", host, port)

    # Graceful shutdown
    def shutdown(sig, frame):
        log.info("Shutting down…")
        observer.stop()
        JOB_QUEUE.put(None)  # poison pill
        observer.join()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Mount app at web_root if set (e.g. "/mediawatcher")
    web_root = CFG.get("web_root", "/").rstrip("/")
    if web_root and web_root != "":
        def not_found(environ, start_response):
            return WerkzeugResponse("Not found", status=404)(environ, start_response)
        wsgi_app = DispatcherMiddleware(not_found, {web_root: app})
        log.info("Mounted at %s", web_root)
    else:
        wsgi_app = app

    # Waitress WSGI server — handles SSE streams and regular requests concurrently
    # without the connection-blocking issues of Flask's built-in dev server.
    log.info("Starting Waitress WSGI server…")
    waitress_serve(wsgi_app, host=host, port=port, threads=16, channel_timeout=3600)


if __name__ == "__main__":
    main()
