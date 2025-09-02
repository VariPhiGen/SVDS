#!/bin/bash
set -e

### ==============================
### Variables (edit as needed)
### ==============================
ETH_IP="IP_PLACEHOLDER"
TABLE_ID="200"
TABLE_NAME="eth0table"

WIFI_METRIC=100
ETH_METRIC=600

# List of external IPs/subnets that must go through eth0
FORCED_IPS=("IP_PLACEHOLDER")

### ==============================
### Detect connection names
### ==============================
ETH_CONN=$(nmcli -t -f NAME,DEVICE connection show --active | grep ":eth0" | cut -d: -f1)
WIFI_CONN=$(nmcli -t -f NAME,DEVICE connection show --active | grep ":wlan0" | cut -d: -f1)

if [ -z "$ETH_CONN" ]; then
  echo "No active eth0 connection found. Exiting."
  exit 1
fi
if [ -z "$WIFI_CONN" ]; then
  echo "No active Wi-Fi connection found. Skipping Wi-Fi metric setup."
fi

echo "Detected Ethernet connection: $ETH_CONN"
[ -n "$WIFI_CONN" ] && echo "Detected Wi-Fi connection: $WIFI_CONN"

### ==============================
### Configure Ethernet (no DNS, no default gateway)
### ==============================
sudo nmcli connection modify "$ETH_CONN" ipv4.addresses "$ETH_IP"
sudo nmcli connection modify "$ETH_CONN" ipv4.method manual
sudo nmcli connection modify "$ETH_CONN" ipv4.route-metric "$ETH_METRIC"
sudo nmcli connection up "$ETH_CONN"

### ==============================
### Configure Wi-Fi metric
### ==============================
if [ -n "$WIFI_CONN" ]; then
  sudo nmcli connection modify "$WIFI_CONN" ipv4.route-metric "$WIFI_METRIC"
  sudo nmcli connection up "$WIFI_CONN"
fi

### ==============================
### Add custom routing table if not already
### ==============================
if ! grep -q "$TABLE_NAME" /etc/iproute2/rt_tables; then
  echo "$TABLE_ID $TABLE_NAME" | sudo tee -a /etc/iproute2/rt_tables
  echo "Added $TABLE_NAME to rt_tables"
fi

### ==============================
### Policy routing for eth0 source IP
### ==============================
# Clear old rules safely
sudo nmcli connection modify "$ETH_CONN" -ipv4.routing-rules "" 2>/dev/null || true
sudo nmcli connection modify "$ETH_CONN" +ipv4.routing-rules "priority 100 from ${ETH_IP%/*} table $TABLE_NAME" || true

### ==============================
### Add static routes for forced external IPs
### ==============================
# Clear old routes safely
sudo nmcli connection modify "$ETH_CONN" -ipv4.routes "" 2>/dev/null || true
for IP in "${FORCED_IPS[@]}"; do
  sudo nmcli connection modify "$ETH_CONN" +ipv4.routes "$IP" || true
  echo "Added forced route: $IP -> eth0"
done

### ==============================
### Apply changes (only up, no down)
### ==============================
sudo nmcli connection up "$ETH_CONN"
[ -n "$WIFI_CONN" ] && sudo nmcli connection up "$WIFI_CONN"

### ==============================
### Verification
### ==============================
echo "==== ip rule ===="
ip rule
echo "==== ip route show table $TABLE_NAME ===="
ip route show table $TABLE_NAME
