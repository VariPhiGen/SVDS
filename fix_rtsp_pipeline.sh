#!/bin/bash
# Make hailo-apps' RTSP source work on Raspberry Pi 5 (HAILO_RECOVERY.md section J).
#
# hailo-apps builds:  rtspsrc ! rtph265depay ! h265parse ! decodebin ! ... ! video/x-raw,format=I420
# On a Pi 5, decodebin picks the hardware decoder v4l2slh265dec, which outputs
# NV12_128C8. That can't negotiate to I420, so the pipeline fails and RTSP
# detection never starts. Adding videoconvert after decodebin fixes it.
#
# Safe to run more than once. Patches every hailo-apps copy found in venvs
# under this repo. Rerun after anything that reinstalls hailo-apps.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REL="site-packages/hailo_apps/hailo_app_python/core/gstreamer/gstreamer_helper_pipelines.py"
OLD='"decodebin ! queue leaky=downstream'
NEW='"decodebin ! videoconvert ! queue leaky=downstream'

shopt -s nullglob
FILES=("$REPO_DIR"/*/lib/python3*/"$REL")
if [[ ${#FILES[@]} -eq 0 ]]; then
  echo "❌ hailo-apps not found in a venv under $REPO_DIR (run ./install.sh first)."
  exit 1
fi

status=0
for f in "${FILES[@]}"; do
  if grep -qF "$NEW" "$f"; then
    echo "✅ RTSP fix already applied: $f"
  elif grep -qF "$OLD" "$f"; then
    [[ -f "$f.orig" ]] || cp "$f" "$f.orig"
    sed -i "s|$OLD|$NEW|" "$f"
    if grep -qF "$NEW" "$f"; then
      echo "✅ RTSP fix applied (backup: $f.orig): $f"
    else
      echo "❌ Patch did not apply: $f"; status=1
    fi
  else
    echo "⚠️  RTSP source line not recognised (hailo-apps changed?), check by hand: $f"
    status=1
  fi
done
exit $status
