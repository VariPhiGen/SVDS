#!/bin/bash

# SVDS Detection Service Setup Script (self-elevating with sudo)

# Re-run the script with sudo if not already root
if [[ $EUID -ne 0 ]]; then
    echo "⚠️  This script needs root privileges. Re-running with sudo..."
    exec sudo bash "$0" "$@"
fi

set -euo pipefail

SERVICE_NAME="svds-detection"
REBOOT_SERVICE_NAME="svds-reboot"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DETECTION_SCRIPT="$SCRIPT_DIR/run_detection.sh"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
REBOOT_SERVICE_FILE="/etc/systemd/system/${REBOOT_SERVICE_NAME}.service"
REBOOT_SCRIPT="$SCRIPT_DIR/reboot_system.sh"

# 👉 Set your username manually here
RUN_USER="svds9"

echo "👉 Setting up services in $SCRIPT_DIR for user $RUN_USER"

# Check detection script exists
if [[ ! -f "$DETECTION_SCRIPT" ]]; then
    echo "❌ Error: run_detection.sh not found in $SCRIPT_DIR"
    exit 1
fi
chmod +x "$DETECTION_SCRIPT"

# Reboot script
cat > "$REBOOT_SCRIPT" << 'EOF'
#!/bin/bash
sleep 86400   # 24 hours
systemctl stop svds-detection
sleep 5
reboot
EOF
chmod +x "$REBOOT_SCRIPT"

# Detection service
cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=SVDS Detection Pipeline
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$RUN_USER
WorkingDirectory=$SCRIPT_DIR
ExecStart=$DETECTION_SCRIPT
Restart=always
RestartSec=30
StandardOutput=null
StandardError=null

[Install]
WantedBy=multi-user.target
EOF

# Reboot service
cat > "$REBOOT_SERVICE_FILE" <<EOF
[Unit]
Description=SVDS 24-Hour Reboot Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=$SCRIPT_DIR
ExecStart=$REBOOT_SCRIPT
Restart=always
RestartSec=60
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Reload and enable
systemctl daemon-reload
systemctl enable "$SERVICE_NAME" "$REBOOT_SERVICE_NAME"
systemctl restart "$SERVICE_NAME" "$REBOOT_SERVICE_NAME"

echo "✅ Services created and started"
echo "  - $SERVICE_NAME (detection pipeline)"
echo "  - $REBOOT_SERVICE_NAME (24h reboot)"
