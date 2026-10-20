# 🛃 MediaWatcher

A macOS background service that monitors directories for new MKV files and
automatically runs a configurable processing pipeline against each one.
Includes a responsive web dashboard for live output monitoring and job history.

---

## What it does

1. **Watches** one or more directories (recursively) for new or changed `.mkv` files using macOS FSEvents
2. **Waits** for the file to finish being written — samples the file size repeatedly until it stops growing, so large transfers over a network complete fully before processing begins
3. **Queues** each settled file and runs its pipeline **serially** — one file at a time, one script at a time
4. **Aborts** the pipeline for that file on any script failure and moves on to the next queued file
5. **Ignores** temporary files (anything with `.tmp` in the filename) and files being modified by the pipeline itself
6. **Resets** any interrupted jobs to a clear status on startup
7. **Serves a web dashboard** with live streaming output, job history, and controls

---

## Web dashboard

Accessible at `http://<host>:<port>` (default `http://127.0.0.1:5678`), or behind a reverse proxy at a subpath (e.g. `https://myserver.local/mediawatcher`).

- **Live output panel** — streams stdout/stderr from the running script in real time via SSE, with ANSI colour codes stripped. Draggable resize handle on desktop.
- **Recent jobs** — scrollable table of all jobs with status, step progress, duration, and per-job log links
- **Job detail page** — full log output for any job, with pipeline step progress in the sidebar
- **Re-run button** — re-enqueue any completed, failed, interrupted or cancelled job
- **Cancel button** — remove a queued job before it starts
- **Config page** — view all active configuration values including environment variable values
- **Responsive layout** — works on desktop and mobile browsers; hamburger menu on mobile

### Screenshots
<img width="1499" height="961" alt="Screenshot 2026-04-15 at 20 21 32" src="https://github.com/user-attachments/assets/01b79385-c746-4d05-9dbb-7d87089ee475" />

<img width="1494" height="959" alt="Screenshot 2026-04-15 at 20 21 43" src="https://github.com/user-attachments/assets/539476e9-3c3e-4158-ad53-84237c345356" />

<img width="1496" height="935" alt="Screenshot 2026-04-15 at 20 21 57" src="https://github.com/user-attachments/assets/2b0e6ba3-adfd-4b7d-aea0-9def480dabc0" />

---

## Installation

### Prerequisites

- macOS 12+ (FSEvents required)
- Python 3.8+
- Your pipeline shell scripts in a directory (see `scripts_dir` in config)

### Create your config

The service needs a `config.json` next to `mediawatcher.py`. It isn't tracked in git, so copy the example and edit it before installing:

```bash
cp config.example.json config.json
```

Then set `watch_dirs`, `scripts_dir` and `pipeline` for your setup (see [Configuration](#configuration)).

### Quick install

```bash
cd /path/to/mediawatcher
chmod +x install.sh mediawatcher.py
./install.sh
```

The installer will:
- Create a Python virtualenv at `.venv/` and install `watchdog`, `flask`, and `waitress`
- Patch `com.user.mediawatcher.plist` with the correct paths and copy it to `~/Library/LaunchAgents/`
- Load and start the service immediately

### Manual install

```bash
# Create virtualenv and install dependencies
python3 -m venv .venv
.venv/bin/pip install watchdog flask waitress

# Run in the foreground for testing
.venv/bin/python3 mediawatcher.py

# Install as a launchd service
cp com.user.mediawatcher.plist ~/Library/LaunchAgents/
# Edit the plist to set PYTHON3_PATH and INSTALL_DIR, then:
launchctl load ~/Library/LaunchAgents/com.user.mediawatcher.plist
```

### Setting environment variables

Environment variables are not inherited from your shell by launchd. Set them
explicitly in the plist under `EnvironmentVariables`:

```xml
<key>EnvironmentVariables</key>
<dict>
  <key>PATH</key>
  <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
  <key>TMDB_TOKEN</key>
  <string>your_token_here</string>
</dict>
```

After editing the plist, reload it:

```bash
launchctl unload ~/Library/LaunchAgents/com.user.mediawatcher.plist
launchctl load   ~/Library/LaunchAgents/com.user.mediawatcher.plist
```

---

## Configuration

Edit `config.json` next to `mediawatcher.py` (created from `config.example.json` — see [Create your config](#create-your-config)) and restart the service to apply changes.

### Full example

```json
{
  "watch_dirs": [
    {
      "path": "/Volumes/Media/Movies",
      "pipeline": [
        "mkv_original_english_audio_subtitle_tracks.sh",
        "convert_to_dv81_hdr10only.sh --overwrite",
        "convert_4k_to_1080p.sh"
      ]
    },
    {
      "path": "/Volumes/Media/TV",
      "pipeline": [
        "mkv_original_english_audio_subtitle_tracks.sh",
        "convert_to_dv81_hdr10only.sh --overwrite"
      ]
    }
  ],
  "scripts_dir": "/Volumes/Media/scripts",
  "pipeline": [
    "mkv_original_english_audio_subtitle_tracks.sh",
    "convert_to_dv81_hdr10only.sh --overwrite",
    "convert_4k_to_1080p.sh"
  ],
  "dry_run": false,
  "dry_run_flags": {
    "mkv_original_english_audio_subtitle_tracks.sh": "--dry-run",
    "convert_to_dv81_hdr10only.sh":                  "--dry-run",
    "convert_4k_to_1080p.sh":                        "-n"
  },
  "web_host": "0.0.0.0",
  "web_port": 5678,
  "web_root": "/",
  "settle_seconds": 5,
  "log_level": "INFO",
  "max_history": 200,
  "env_passthrough": [
    "HOME",
    "PATH",
    "TMDB_TOKEN"
  ]
}
```

### Configuration reference

| Key | Default | Description |
|-----|---------|-------------|
| `watch_dirs` | `[]` | Directories to monitor. See below for formats. |
| `scripts_dir` | `~/scripts` | Directory where pipeline shell scripts are located |
| `pipeline` | `[]` | Default pipeline — used for any watch dir without its own pipeline |
| `dry_run` | `false` | When `true`, injects each script's dry-run flag instead of running live |
| `dry_run_flags` | `{}` | Maps script filename → its dry-run flag (e.g. `"--dry-run"` or `"-n"`) |
| `web_host` | `127.0.0.1` | Bind address for the web UI. Use `0.0.0.0` for LAN access. |
| `web_port` | `5678` | Port for the web UI |
| `web_root` | `/` | Subpath to serve the UI from (e.g. `/mediawatcher` for reverse proxy use) |
| `settle_seconds` | `5` | Seconds to wait after the last file size change before enqueuing |
| `log_level` | `INFO` | Log verbosity: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `max_history` | `200` | Maximum number of jobs to retain in the database |
| `env_passthrough` | `[]` | Environment variable names to forward from the service to each script |

### watch_dirs format

Each entry can be a plain path string (uses the default pipeline) or an object
with a directory-specific pipeline. Both forms can be mixed freely.

```json
"watch_dirs": [
  "/Volumes/Media/Inbox",
  {
    "path": "/Volumes/Media/Movies",
    "pipeline": [
      "mkv_original_english_audio_subtitle_tracks.sh",
      "convert_to_dv81_hdr10only.sh --overwrite",
      "convert_4k_to_1080p.sh"
    ]
  }
]
```

The plain string form falls back to the top-level `pipeline`.

### Dry-run mode

Set `"dry_run": true` in `config.json` and restart. Each script in the pipeline
will be called with its dry-run flag so you can observe what the service would
do without modifying any files.

Scripts without an entry in `dry_run_flags` are **skipped entirely** in dry-run
mode with a warning — they will not be called at all. This is the safest
default for unknown scripts.

A yellow DRY RUN MODE banner appears in the web UI sidebar whenever this is active.

---

## How pipeline scripts are executed

Each pipeline entry is a string: the script filename followed by any flags,
space-separated. The first token is the script name (resolved against
`scripts_dir`). Remaining tokens are passed as arguments **before** the file
path. The file path is always the final argument.

Given `scripts_dir = "/Volumes/Media/scripts"` and the pipeline entry:

```json
"convert_to_dv81_hdr10only.sh --overwrite"
```

MediaWatcher executes:

```bash
bash /Volumes/Media/scripts/convert_to_dv81_hdr10only.sh \
  --overwrite \
  /path/to/file.mkv
```

In dry-run mode, the dry-run flag is appended after any existing flags:

```bash
bash /Volumes/Media/scripts/convert_to_dv81_hdr10only.sh \
  --overwrite \
  --dry-run \
  /path/to/file.mkv
```

### File detection behaviour

- Only `.mkv` files trigger the pipeline
- Files with `.tmp` anywhere in the filename are ignored (handles in-progress remux temp files)
- Files currently being processed by the pipeline are ignored — no re-triggering while in-flight
- After the settle timer fires, the file size is sampled twice with a 2-second gap. If still
  growing, the timer reschedules itself. This repeats until stable, correctly handling very
  large files being copied over a network without any manual configuration.

### Pipeline execution behaviour

- Jobs are processed **one at a time** in FIFO order
- Within a job, scripts run **one at a time** in order
- If any script exits with a non-zero exit code, the pipeline for that file is
  aborted immediately and the job is marked as `failed`
- The next queued job starts regardless of the previous job's outcome
- All stdout and stderr from each script is captured, ANSI escape codes stripped,
  streamed live to the dashboard, and stored in the SQLite database

### Job statuses

| Status | Meaning |
|--------|---------|
| `queued` | Waiting in the queue to be processed |
| `running` | Currently being processed |
| `done` | All pipeline steps completed successfully |
| `failed` | A script exited with a non-zero code — pipeline aborted |
| `interrupted` | Service was stopped while the job was queued or running |
| `cancelled` | Manually cancelled via the web UI before it started |

---

## Service management

```bash
# Restart after editing config.json
launchctl stop  com.user.mediawatcher
launchctl start com.user.mediawatcher

# Reload plist after editing it (e.g. to add env vars)
launchctl unload ~/Library/LaunchAgents/com.user.mediawatcher.plist
launchctl load   ~/Library/LaunchAgents/com.user.mediawatcher.plist

# Check service status — shows PID and last exit code
# PID of "-" with exit code "1" means it crashed
launchctl list | grep mediawatcher

# View logs
tail -f /path/to/mediawatcher/logs/mediawatcher.log
tail -f /path/to/mediawatcher/logs/stderr.log

# Remove service entirely
launchctl unload ~/Library/LaunchAgents/com.user.mediawatcher.plist
rm ~/Library/LaunchAgents/com.user.mediawatcher.plist
```

---

## Reverse proxy setup (Caddy)

To serve the dashboard at a subpath, set `web_root` in `config.json`:

```json
"web_host": "0.0.0.0",
"web_root": "/mediawatcher"
```

Then in your Caddyfile:

```
reverse_proxy /mediawatcher/* 192.168.0.222:5678
```

All internal URLs (navigation, API calls, SSE stream, static assets) are
generated relative to `web_root` automatically — no path rewriting needed.

---

## Architecture

```
FSEvents (watchdog / macOS kernel)
   │   created / modified / moved event for *.mkv
   ▼
MKVHandler._schedule()
   │   skip: non-.mkv, .tmp in name, already in-flight
   ▼
Settle timer (threading.Timer, settle_seconds)
   │   fires → sample file size twice, 2s apart
   │   still growing → reschedule
   ▼
enqueue_file()
   │   dedup check: in-flight dict + DB query
   ▼
JOB_QUEUE (FIFO, threading.Queue)
   ▼
worker_thread() — single serial worker
   │   check _cancelled set → skip if cancelled
   ▼
run_pipeline()
   │   resolve pipeline for file's watch dir
   │   for each script:
   │     inject dry-run flag if dry_run=true
   │     subprocess.Popen → stream stdout+stderr line by line
   │     strip ANSI escape codes
   │     emit to SSE OutputBus + write to SQLite job_logs
   │     non-zero exit → abort, mark job failed
   ▼
OutputBus (fan-out SSE broadcast)
   │   ring buffer of last 500 lines for late-joining browsers
   ▼
Waitress WSGI server (16 threads)
   ├─ GET  /                        Dashboard
   ├─ GET  /job/<id>                Job detail + full logs
   ├─ GET  /stream                  SSE live output (persistent)
   ├─ GET  /config                  Config viewer
   ├─ GET  /api/jobs                JSON job list
   ├─ GET  /api/job/<id>/logs       JSON job logs
   ├─ GET  /api/queue_size          Queue depth
   ├─ POST /api/job/<id>/rerun      Re-enqueue a finished job
   └─ POST /api/job/<id>/cancel     Cancel a queued job
```

---

## File layout

```
mediawatcher/
├── mediawatcher.py              # Main daemon (watcher + queue + web server)
├── config.example.json          # Example configuration (copy to config.json)
├── config.json                  # Your configuration (not tracked in git)
├── install.sh                   # One-shot installer
├── com.user.mediawatcher.plist  # launchd service definition
├── README.md                    # This file
├── templates/
│   ├── index.html               # Dashboard
│   ├── job.html                 # Job detail / live log
│   ├── config.html              # Config viewer
│   └── _jobs_table.html         # Jobs table partial (server + JS rendered)
├── static/
│   ├── css/style.css            # Styles (dark theme, responsive)
│   └── js/app.js                # SSE client, live log, job table, resize handle
├── logs/                        # Created on first run
│   ├── mediawatcher.log         # Structured application log
│   ├── stdout.log               # launchd stdout capture
│   └── stderr.log               # launchd stderr capture
└── db/
    └── jobs.db                  # SQLite — jobs + job_logs tables
```

---

## Disclaimer

This service was built for personal use in a specific home media server environment. It is shared as-is, with no guarantee that it will work correctly in any other environment, setup, or configuration.

The code was produced with the assistance of AI and has not been independently audited or scrutinised for correctness, security, or output quality.

The author accepts no liability for any damage caused to your environment, machine, files, or data as a result of using this software. This includes but is not limited to data loss, file corruption, or unintended modifications to your media files.

**Usage is entirely at your own risk.**
