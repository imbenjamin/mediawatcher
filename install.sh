#!/usr/bin/env bash
# install.sh — sets up mediawatcher as a macOS launchd service
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLIST_SRC="$SCRIPT_DIR/com.user.mediawatcher.plist"
PLIST_DST="$HOME/Library/LaunchAgents/com.user.mediawatcher.plist"
PYTHON3="$(which python3)"
VENV="$SCRIPT_DIR/.venv"

echo "════════════════════════════════════════"
echo "  MediaWatcher Installer"
echo "════════════════════════════════════════"
echo "  Install dir : $SCRIPT_DIR"
echo "  Python3     : $PYTHON3"
echo ""

# ── 1. Python venv + dependencies ──────────────────────────────────────────
echo "→ Creating Python virtual environment…"
"$PYTHON3" -m venv "$VENV"
"$VENV/bin/pip" install --quiet --upgrade pip
"$VENV/bin/pip" install --quiet watchdog flask waitress

VENV_PYTHON="$VENV/bin/python3"
echo "  ✓ Dependencies installed"

# ── 2. Patch the plist ──────────────────────────────────────────────────────
echo "→ Writing launchd plist…"
sed \
  -e "s|PYTHON3_PATH|$VENV_PYTHON|g" \
  -e "s|INSTALL_DIR|$SCRIPT_DIR|g" \
  "$PLIST_SRC" > "$PLIST_DST"
echo "  ✓ Plist written to $PLIST_DST"

# ── 3. Create logs dir ──────────────────────────────────────────────────────
mkdir -p "$SCRIPT_DIR/logs" "$SCRIPT_DIR/db"

# ── 4. Prompt for config ────────────────────────────────────────────────────
if ! grep -q '"/Volumes' "$SCRIPT_DIR/config.json" 2>/dev/null; then
  echo ""
  echo "⚠️  No watch_dirs configured yet."
  echo "   Edit config.json and add your media directories:"
  echo "   $SCRIPT_DIR/config.json"
fi

# ── 5. Load service ─────────────────────────────────────────────────────────
echo ""
echo "→ Loading launchd service…"
# Unload first if already loaded (ignore errors)
launchctl unload "$PLIST_DST" 2>/dev/null || true
launchctl load "$PLIST_DST"
echo "  ✓ Service loaded"

sleep 1

# ── 6. Status check ─────────────────────────────────────────────────────────
echo ""
echo "→ Service status:"
launchctl list | grep mediawatcher || echo "  (not found — check logs)"

echo ""
echo "════════════════════════════════════════"
echo "  ✓ Installation complete"
echo ""
echo "  Web UI  : http://127.0.0.1:5678"
echo "  Logs    : $SCRIPT_DIR/logs/"
echo ""
echo "  Stop    : launchctl stop com.user.mediawatcher"
echo "  Start   : launchctl start com.user.mediawatcher"
echo "  Unload  : launchctl unload $PLIST_DST"
echo "════════════════════════════════════════"
