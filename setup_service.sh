#!/bin/bash

# Simple SVDS Detection Service Setup Script (no logging)
# This script creates a systemd service to run run_detection.sh continuously
# with automatic 24-hour reboot to prevent system issues

set -e

SERVICE_NAME="svds-detection"
REBOOT_SERVICE_NAME="svds-reboot"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DETECTION_SCRIPT="$SCRIPT_DIR/run_detection.sh"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
REBOOT_SERVICE_FILE="/etc/systemd/system/${REBOOT_SERVICE_NAME}.service"
REBOOT_SCRIPT="$SCRIPT_DIR/reboot_system.sh"
USER="$(whoami)"

echo "Setting up SVDS Detection Service with 24-hour reboot..."

# Check if running as root
if [[ $EUID -ne 0 ]]; then
    echo "Error: This script must be run as root or with sudo"
    echo "Usage: sudo ./setup_service.sh"
    exit 1
fi

# Check if detection script exists
if [[ ! -f "$DETECTION_SCRIPT" ]]; then
    echo "Error: run_detection.sh not found in $SCRIPT_DIR"
    exit 1
fi

# Make detection script executable
chmod +x "$DETECTION_SCRIPT"

# Create reboot script
echo "Creating reboot script: $REBOOT_SCRIPT"
cat > "$REBOOT_SCRIPT" << 'REBOOT_EOF'
#!/bin/bash
# SVDS System Reboot Script (no logs)
# Reboots the system after 24 hours to prevent memory leaks and system issues

REBOOT_INTERVAL=86400  # 24 hours in seconds

sleep $REBOOT_INTERVAL

# Stop the detection service gracefully
systemctl stop svds-detection
sleep 5

# Reboot the system
reboot
REBOOT_EOF

chmod +x "$REBOOT_SCRIPT"

# Create systemd service file for detection
echo "Creating detection service file: $SERVICE_FILE"
cat > "$SERVICE_FILE" << SERVICE_EOF
[Unit]
Description=SVDS Detection Pipeline
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$SCRIPT_DIR
ExecStart=$DETECTION_SCRIPT
Restart=always
RestartSec=30
StandardOutput=null
StandardError=null

[Install]
WantedBy=multi-user.target
SERVICE_EOF

# Create systemd service file for reboot
echo "Creating reboot service file: $REBOOT_SERVICE_FILE"
cat > "$REBOOT_SERVICE_FILE" << REBOOT_SERVICE_EOF
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
StandardOutput=null
StandardError=null

[Install]
WantedBy=multi-user.target
REBOOT_SERVICE_EOF

# Reload systemd daemon
echo "Reloading systemd daemon..."
systemctl daemon-reload

# Enable and start services
echo "Enabling detection service..."
systemctl enable "$SERVICE_NAME"

echo "Enabling reboot service..."
systemctl enable "$REBOOT_SERVICE_NAME"

echo "Starting detection service..."
systemctl start "$SERVICE_NAME"

echo "Starting reboot service..."
systemctl start "$REBOOT_SERVICE_NAME"

echo "Service setup complete!"
echo ""
echo "Services created:"
echo "  - $SERVICE_NAME: Main detection pipeline"
echo "  - $REBOOT_SERVICE_NAME: 24-hour reboot service"
echo ""
echo "Service management commands:"
echo "  sudo systemctl start $SERVICE_NAME       # Start detection service"
echo "  sudo systemctl stop $SERVICE_NAME        # Stop detection service"
echo "  sudo systemctl restart $SERVICE_NAME     # Restart detection service"
echo "  sudo systemctl status $SERVICE_NAME      # Check detection status"
echo "  sudo systemctl status $REBOOT_SERVICE_NAME # Check reboot service status"
