#!/bin/bash
# hailo_doctor.sh - read-only health check for a Hailo PCIe accelerator on Raspberry Pi 5.
# It changes nothing; it prints what is wrong and the exact command to fix it.
# Usage:  sudo ./hailo_doctor.sh            (sudo needed to read PCIe registers / dmesg)
#         sudo ./hailo_doctor.sh --no-run   (skip the inference test)

RUN_TEST=1; [ "$1" = "--no-run" ] && RUN_TEST=0
ok()   { echo -e "  \033[32m[OK]\033[0m   $*"; }
bad()  { echo -e "  \033[31m[FAIL]\033[0m $*"; FAILED=1; }
warn() { echo -e "  \033[33m[WARN]\033[0m $*"; }
fix()  { echo -e "         fix: $*"; }
FAILED=0
[ "$(id -u)" -ne 0 ] && { echo "Run with sudo: sudo $0"; exit 1; }
KREL=$(uname -r)
echo "Kernel: $KREL   page size: $(getconf PAGESIZE)   boot: $(uptime -s)"

echo "1. PCIe enumeration"
DEV=$(lspci -D -d 1e60: | awk '{print $1}' | head -1)
if [ -z "$DEV" ]; then
    bad "No Hailo device (vendor 1e60) on PCIe"
    fix "power off, reseat M.2 card / ribbon cable; check 'dtparam=pciex1' in /boot/firmware/config.txt"
    exit 1
fi
ok "Hailo at $DEV: $(lspci -s $DEV | cut -d' ' -f2-)"
UP=$(basename "$(dirname "$(readlink -f /sys/bus/pci/devices/$DEV)")")
LNK=$(lspci -vvv -s $DEV | grep -m1 "LnkSta:" | sed 's/^\s*//')
echo "         link: $LNK   (config.txt: $(grep -E '^(pcie_gen3|dtparam=pciex1_gen)' /boot/firmware/config.txt | tr '\n' ' '))"

echo "2. Kernel module matches running kernel"
MODVER=$(modinfo -F vermagic hailo_pci 2>/dev/null | awk '{print $1}')
if [ -z "$MODVER" ]; then
    bad "hailo_pci module not installed for $KREL"
    fix "see HAILO_RECOVERY.md section A (rebuild driver)"
elif [ "$MODVER" != "$KREL" ]; then
    bad "hailo_pci built for $MODVER, running $KREL"
    fix "see HAILO_RECOVERY.md section A (rebuild driver)"
else
    ok "hailo_pci $(modinfo -F version hailo_pci) built for $KREL"
fi
command -v dkms >/dev/null && ok "dkms installed (driver rebuilds on kernel updates)" \
    || warn "dkms not installed - next kernel update will break the driver (HAILO_RECOVERY.md section D)"

echo "3. Driver bound / device node"
DRV=$(basename "$(readlink /sys/bus/pci/devices/$DEV/driver 2>/dev/null)" 2>/dev/null)
[ "$DRV" = "hailo" ] && ok "driver 'hailo' bound" || bad "no driver bound to $DEV"
[ -c /dev/hailo0 ] && ok "/dev/hailo0 exists" || bad "/dev/hailo0 missing"
PROBE_ERR=$(dmesg | grep -E "hailo.*(Failed|failed|error)" | grep -v "Modules linked" | tail -3)
[ -n "$PROBE_ERR" ] && { warn "driver errors in dmesg:"; echo "$PROBE_ERR" | sed 's/^/           /'; }

echo "4. PCIe config space intact (BARs, Max Payload Size)"
HW_BAR_HI=$(setpci -s $DEV BASE_ADDRESS_1.L)
HW_BAR_LO=$(setpci -s $DEV BASE_ADDRESS_0.L)
KBUS=$(lspci -b -v -s $DEV | grep -m1 "Memory at" | awk '{print $3}')
if [ "$HW_BAR_HI" = "00000000" ] && [ "$((16#${HW_BAR_LO} & 0xfffffff0))" = "0" ]; then
    bad "BAR0 in hardware is 0 but kernel assigned bus address 0x$KBUS -> device was reset after boot"
    fix "see HAILO_RECOVERY.md section B"
else
    ok "BAR0 hardware = 0x${HW_BAR_HI}${HW_BAR_LO} (kernel bus addr 0x$KBUS)"
fi
MPS_DEV=$(( (16#$(setpci -s $DEV CAP_EXP+8.W) >> 5) & 7 ))
MPS_UP=$((  (16#$(setpci -s $UP  CAP_EXP+8.W) >> 5) & 7 ))
if [ "$MPS_DEV" != "$MPS_UP" ]; then
    bad "Max Payload mismatch: device $((128<<MPS_DEV))B, root port $((128<<MPS_UP))B (causes 0 FPS / malformed TLPs)"
    fix "see HAILO_RECOVERY.md section B (reboot)"
else
    ok "Max Payload Size $((128<<MPS_DEV))B on both ends"
fi
AER=$(lspci -vvv -s $DEV | grep -m1 "UESta:" | grep -oE "[A-Za-z]+\+" | tr '\n' ' ')
[ -n "$AER" ] && warn "device AER uncorrectable errors latched: $AER" || ok "no uncorrectable PCIe errors latched"

echo "5. Firmware / runtime"
cd /tmp
ID=$(timeout 15 hailortcli fw-control identify 2>&1 | tr -d '\000')
if echo "$ID" | grep -q "Firmware Version"; then
    ok "$(echo "$ID" | grep -m1 'Firmware Version' | sed 's/^\s*//')"
    ARCH=$(echo "$ID" | grep -m1 "Device Architecture" | awk '{print $3}')
    ok "Device Architecture: $ARCH  -> use $( [ "$ARCH" = "HAILO8L" ] && echo 'resources/models/hailo8l (run download_hailo8l_models.sh)' || echo 'resources/models/hailo8' ) HEFs"
else
    bad "hailortcli fw-control identify failed"
fi
echo "         versions: $(dpkg-query -W -f='${Package}=${Version} ' hailort hailort-pcie-driver hailo-tappas-core 2>/dev/null)"

if [ $RUN_TEST -eq 1 ] && [ -c /dev/hailo0 ]; then
    echo "6. Inference test"
    HEF=$(ls /usr/share/hailo-models/resnet_v1_50_h8l.hef 2>/dev/null)
    [ "$ARCH" = "HAILO8" ] && HEF=$(ls /usr/share/hailo-models/resnet_v1_50_h8.hef 2>/dev/null || echo "$HEF")
    if [ -n "$HEF" ]; then
        FPS=$(timeout 40 hailortcli run "$HEF" -t 5 2>&1 | tr '\r' '\n' | grep -E "^\s+FPS:" | tail -1 | awk '{print $2}')
        if [ -n "$FPS" ] && awk "BEGIN{exit !($FPS > 0)}"; then
            ok "$(basename $HEF): $FPS FPS"
        else
            bad "inference produced ${FPS:-no} FPS (DMA not working)"
            fix "see HAILO_RECOVERY.md section C"
        fi
    else
        warn "no test HEF in /usr/share/hailo-models (install hailo-models)"
    fi
fi

echo
[ $FAILED -eq 0 ] && echo -e "\033[32mHailo looks healthy.\033[0m" || echo -e "\033[31mProblems found - see fixes above.\033[0m"
exit $FAILED
