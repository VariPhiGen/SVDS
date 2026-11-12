#!/bin/bash
set -euo pipefail

# —————————————————————————————
# 0. Parse flags
# —————————————————————————————
NO_INSTALLATION=false
PYHAILORT_PATH=""
PYTAPPAS_PATH=""
DOWNLOAD_ALL="default"  # Default to false unless --all is specified
while [[ $# -gt 0 ]]; do
  case "$1" in
    -n|--no-installation)
      NO_INSTALLATION=true
      shift
      ;;
    -h|--pyhailort)
      PYHAILORT_PATH="$2"
      shift 2
      ;;
    -p|--pytappas)
      PYTAPPAS_PATH="$2"
      shift 2
      ;;
    --all)
      DOWNLOAD_ALL="all"
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;

  esac
done

# —————————————————————————————
# 1. Read config
# —————————————————————————————
CONFIG_FILE="config.yaml"
if [[ -n "$CONFIG_FILE" ]]; then
  echo "📄 Loading configuration from $CONFIG_FILE"
    while IFS= read -r line; do
    # 1) Remove any inline comment (everything from the first unescaped “#” to end of line)
    line="${line%%#*}"

    # 2) Skip blank lines
    [[ -z "${line//[[:space:]]/}" ]] && continue

    # 3) Split on the *first* colon
    key=${line%%:*}
    val=${line#*:}

    # 4) Trim whitespace and surrounding quotes
    key=$(echo "$key" | xargs)
    val=$(echo "$val" | sed -e 's/^[[:space:]]*"\{0,1\}//' \
                            -e 's/"\{0,1\}[[:space:]]*$//')


    case "$key" in
      server_url)          server_url="$val"          ;;
      storage_dir)         storage_dir="$val"         ;;
      hailort_version)     hailort_version="$val"     ;;
      tappas_version)      tappas_version="$val"      ;;
      virtual_env_name)    virtual_env_name="$val"    ;;
      hailo_apps_infra_repo_url)   hailo_apps_infra_repo_url="$val"   ;;
      hailo_apps_infra_branch_tag) hailo_apps_infra_branch_tag="$val" ;;
      hailo_apps_infra_path)  hailo_apps_infra_path="$val"  ;;
      tappas_variant) tappas_variant="$val" ;;
      # you can add more mappings here…
    esac
  done < <(grep -E '^[[:space:]]*[a-z_]+:' "$CONFIG_FILE")
fi
# —————————————————————————————
# 2. Fallback to old defaults
# —————————————————————————————
: "${server_url:=http://dev-public.hailo.ai/2025_01}"
: "${storage_dir:=hailo_temp_resources}"
: "${hailort_version:=4.20.0}"
: "${tappas_version:=3.31.0}"
: "${virtual_env_name:="hailo_venv"}"
: "${hailo_apps_infra_repo_url:=https://github.com/VariPhiGen/hailo-apps-infra.git}"
: "${hailo_apps_infra_branch_tag:=main}"
: "${hailo_apps_infra_path:="auto"}"  # or "auto" for latest
: "${tappas_variant:="hailo-tappas-core"}"  # or "x86_64"

# Ensure all required variables are set
if [[ "$hailo_apps_infra_branch_tag" == "auto" ]] && [[ "$hailo_apps_infra_path" == "auto" ]]; then
  echo "❌ Please set 'hailo_apps_infra_repo_url', 'hailo_apps_infra_branch_tag', and 'hailo_apps_infra_path' in the config."
  echo "Using hailo_apps_infra_branch_tag = dev because auto was set."
  hailo_apps_infra_branch_tag="main"
fi

# Now use those
BASE_URL="$server_url"
DOWNLOAD_DIR="$storage_dir"
HAILORT_VERSION="$hailort_version"
TAPPAS_CORE_VERSION="$tappas_version"
VENV_NAME="$virtual_env_name"
HAILO_INFRA_PATH="$hailo_apps_infra_path"
TAPPAS_PIP_PKG="$tappas_variant"

# —————————————————————————————
# Rest of your existing checks & installs…
# —————————————————————————————

echo "Using config from $CONFIG_FILE (or defaults):"
echo "  BASE_URL           = $BASE_URL"
echo "  DOWNLOAD_DIR       = $DOWNLOAD_DIR"
echo "  HAILORT_VERSION    = $HAILORT_VERSION"
echo "  TAPPAS_CORE_VERSION= $TAPPAS_CORE_VERSION"
echo "  VENV_NAME          = $VENV_NAME"
echo "  Hailo-Apps-Infra   = $hailo_apps_infra_repo_url @ $hailo_apps_infra_branch_tag"
echo "  TAPPAS_PIP_PKG     = $TAPPAS_PIP_PKG"
echo "  TAPPAS_VARIANT     = $tappas_variant"
echo "  HAILO_INFRA_PATH   = $HAILO_INFRA_PATH"
echo "  CONFIG_FILE        = $CONFIG_FILE"


###——— HELPERS —————————————————————————————————————————————————————
detect_system_pkg_version() {
  dpkg -l | grep "$1" | awk '$1=="ii" { print $3; exit }'
}


detect_pip_pkg_version() {
  if pip show "$1" >/dev/null 2>&1; then
    pip show "$1" \
      | awk -F': ' '/^Version: /{print $2; exit}'
  else
    echo ""
  fi
}

check_system_pkg() {
  pkg="$1"
  ver=$(detect_system_pkg_version "$pkg")
  if [[ -z "$ver" ]]; then
    echo "❌ System package '$pkg' not found."
    echo "    Please install it before proceeding."
    exit 1
  else
    echo "✅ $pkg (system) version: $ver"
  fi
}

###——— SYSTEM PKG CHECKS —————————————————————————————————————————————
echo
echo "📋 Checking required system packages…"
check_system_pkg hailort

echo
echo "📋 Checking for HailoRT system version"
HRT_VER=$(detect_system_pkg_version hailort)
echo "📋 Checking for hailo-tappas vs hailo-tappas-core…"
HT1=$(detect_system_pkg_version hailo-tappas)
echo $HT1
HT2=$(detect_system_pkg_version hailo-tappas-core)
echo $HT2
HTC_VER="none"

if [[ -n "$HT2" ]]; then
  echo "✅ hailo-tappas-core version: $HT2"
  TAPPAS_PIP_PKG="hailo-tappas-core-python-binding"
  HTC_VER="$HT2"
elif [[ -n "$HT1" ]]; then
  echo "✅ hailo-tappas version: $HT1"
  HTC_VER="$HT1"
  TAPPAS_PIP_PKG="hailo-tappas"
else
  echo "❌ Neither hailo-tappas nor hailo-tappas-core is installed."
  exit 1
fi


###——— PIP PKG CHECKS —————————————————————————————————————————————————
echo
echo "📋 Checking host-Python pip packages…"
INSTALL_PYHAILORT=false
INSTALL_TAPPAS_CORE=false

# hailort
host_py=$(detect_pip_pkg_version hailort)
if [[ -z "$host_py" ]]; then
  echo "⚠️  pip 'hailort' missing; will install in venv."
  INSTALL_PYHAILORT=true
else
  echo "✅ pip 'hailort' version: $host_py"
fi

# tappas binding pkg (RPi vs x86)
host_tc=$(detect_pip_pkg_version "$TAPPAS_PIP_PKG")
if [[ -z "$host_tc" ]]; then
  echo "⚠️  pip '$TAPPAS_PIP_PKG' missing; will install in venv."
  INSTALL_TAPPAS_CORE=true
else
  echo "✅ pip '$TAPPAS_PIP_PKG' version: $host_tc"
fi

if [[ "$NO_INSTALLATION" == true ]]; then
  echo "⚠️  Skipping installation due to --no-installation flag."
  INSTALL_PYHAILORT=false
  INSTALL_TAPPAS_CORE=false
else
  echo "📦 Will install missing pip packages in virtualenv."
fi

if [[ -n "$PYHAILORT_PATH" ]]; then
  echo "📦 Using custom hailort path: $PYHAILORT_PATH"
  INSTALL_PYHAILORT=true
fi
if [[ -n "$PYTAPPAS_PATH" ]]; then
  echo "📦 Using custom tappas path: $PYTAPPAS_PATH"
  INSTALL_TAPPAS_CORE=true
fi

###——— HAILO PCIe DRIVER (DKMS + missing-file repair) ————————————————————
# See HAILO_RECOVERY.md section D. If the package's files under /usr/src or
# /lib/firmware/hailo are missing, its postinst runs `make uninstall` and then
# fails at `pushd`, leaving no hailo_pci module. Restore the files from the
# .deb first, then reinstall so the postinst registers the driver with DKMS.
NEED_REBOOT=false
ensure_hailo_pcie_driver() {
  local pkg=hailort-pcie-driver
  if ! dpkg -s "$pkg" >/dev/null 2>&1; then
    echo "⚠️  $pkg not installed; skipping driver check."
    return 0
  fi

  local reinstall=false
  if ! command -v dkms >/dev/null 2>&1; then
    echo "📦 Installing dkms so the Hailo driver is rebuilt on kernel updates…"
    sudo apt install -y dkms
    reinstall=true
  fi

  if [[ ! -d /usr/src/hailort-pcie-driver/linux/pcie ]] \
     || dpkg -V "$pkg" 2>/dev/null | grep -q '^missing'; then
    echo "🔧 $pkg files are missing; restoring them from the .deb…"
    local ver deb tmp
    ver=$(dpkg-query -W -f='${Version}' "$pkg")
    sudo apt install --download-only --reinstall -y "$pkg"
    deb=$(ls /var/cache/apt/archives/${pkg}_*"${ver#*:}"*_all.deb 2>/dev/null | head -n1)
    if [[ -z "$deb" ]]; then
      echo "❌ Could not find the downloaded $pkg $ver .deb in /var/cache/apt/archives."
      exit 1
    fi
    tmp=$(mktemp -d)
    dpkg-deb -x "$deb" "$tmp"
    sudo cp -a "$tmp/usr/src/hailort-pcie-driver" /usr/src/
    sudo mkdir -p /lib/firmware/hailo
    sudo cp -a "$tmp/lib/firmware/hailo/." /lib/firmware/hailo/
    rm -rf "$tmp"
    if dpkg -V "$pkg" 2>/dev/null | grep -q '^missing'; then
      echo "❌ $pkg still has missing files after restore:"
      dpkg -V "$pkg" | grep '^missing'
      exit 1
    fi
    reinstall=true
  fi

  if ! modinfo -k "$(uname -r)" hailo_pci >/dev/null 2>&1; then
    echo "⚠️  hailo_pci module not installed for $(uname -r)."
    reinstall=true
  fi

  if $reinstall; then
    echo "📦 Reinstalling $pkg (builds hailo_pci via DKMS)…"
    sudo apt install --reinstall -y "$pkg"
    NEED_REBOOT=true
  else
    echo "✅ $pkg intact, dkms present, hailo_pci built for $(uname -r)"
  fi
}

echo
echo "📋 Checking Hailo PCIe driver…"
if [[ "$NO_INSTALLATION" == true ]]; then
  echo "⚠️  Skipping driver check due to --no-installation flag."
else
  ensure_hailo_pcie_driver
fi

sudo apt install python3-gi python3-gi-cairo


###——— VENV SETUP —————————————————————————————————————————————————————
arch=$(grep -q "Raspberry Pi" /proc/device-tree/model 2>/dev/null && echo "rpi" || uname -m)
echo "$arch"

echo
if [[ -d "$VENV_NAME" ]]; then
  echo "✅ Virtualenv '$VENV_NAME' exists. Activating…"
  source "$VENV_NAME/bin/activate"
else
  echo "🔧 Creating virtualenv '$VENV_NAME'…"
  python3 -m venv --system-site-packages "$VENV_NAME"
  echo "✅ Created. Activating…"
  source "$VENV_NAME/bin/activate"
fi


###——— INSTALL MISSING PIP PACKAGES —————————————————————————————————————
echo
echo "📦 Installing missing pip packages…"


# pyhailort
if $INSTALL_PYHAILORT; then
  if [[ -n "$PYHAILORT_PATH" ]]; then
    echo "📦 Installing 'hailort' from local path: $PYHAILORT_PATH"
    pip install "$PYHAILORT_PATH"
  else
    echo "📦 Installing 'hailort' via helper script"
    ./hailo_python_installation.sh --only-hailort
  fi
fi

# pytappas (tappas-core or tappas binding)
if $INSTALL_TAPPAS_CORE; then
  if [[ -n "$PYTAPPAS_PATH" ]]; then
    echo "📦 Installing '$TAPPAS_PIP_PKG' from local path: $PYTAPPAS_PATH"
    pip install "$PYTAPPAS_PATH"
  else
    echo "📦 Installing '$TAPPAS_PIP_PKG' via helper script"
    ./hailo_python_installation.sh --only-tappas
  fi
fi

# If neither was missing, you can still echo:
if ! $INSTALL_PYHAILORT && ! $INSTALL_TAPPAS_CORE; then
  echo "✅ All pip packages are already installed."
fi



###——— ENV FILE —————————————————————————————————————————————————————
ENV_FILE=".env"
ENV_PATH="$(pwd)/$ENV_FILE"
export HAILO_ENV_FILE="${ENV_PATH}"
echo $HAILO_ENV_FILE

CONFIG_PATH="$(pwd)/$CONFIG_FILE"

# Step 1: Create the .env file if it doesn't exist
if [[ ! -f "$ENV_PATH" ]]; then
    echo "🔧 Creating .env file at $ENV_PATH"
    touch "$ENV_PATH"
    chmod 666 "$ENV_PATH"  # rw-rw-r-- for user/group
else
    echo "✅ .env already exists at $ENV_PATH"
    chmod 666 "$ENV_PATH"
fi


###——— MODULE INSTALLS ———————————————————————————————————————————————————
echo
echo "📦 Upgrading pip/setuptools/wheel…"
pip install --upgrade pip setuptools wheel
pip install "py>=1.8.0"


pip install -r requirements.txt --break-system-packages
echo $"📦 Installing Hailo-Apps-Infra… $HAILO_INFRA_PATH"
if [[ "$HAILO_INFRA_PATH" != "auto" ]]; then
  echo "📦 Installing Hailo-Apps-Infra from $HAILO_INFRA_PATH…"
  pip install -e "$HAILO_INFRA_PATH"
else
    echo "📦 Installing hailo-apps-infra from Git ($hailo_apps_infra_branch_tag)…"
    pip install "git+${hailo_apps_infra_repo_url}@${hailo_apps_infra_branch_tag}#egg=hailo-apps"
fi


echo "📦 Installing shared runtime deps…"


###——— POST-INSTALL ———————————————————————————————————————————————————
echo
echo "⚙️  Running post-install…"
hailo-post-install \
    --dotenv "$ENV_PATH" \
    --config "$CONFIG_PATH" \
    --group "$DOWNLOAD_ALL"

###——— RTSP SOURCE FIX (Pi 5 hardware H.265 decoder) ——————————————————————
# hailo-apps' RTSP pipeline can't negotiate the Pi 5 decoder's NV12_128C8
# output to I420, so RTSP input fails. See HAILO_RECOVERY.md section J.
echo
echo "🔧 Patching hailo-apps RTSP source pipeline…"
./fix_rtsp_pipeline.sh || echo "⚠️  RTSP fix not applied; RTSP input may fail (see HAILO_RECOVERY.md section J)"

###——— YOLO POST-PROCESS FALLBACK ————————————————————————————————————————
# hailo-apps' C++ post-processes don't compile against TAPPAS 5.x headers
# (HailoTensor::vstream_info was removed), so hailo-post-install can leave
# libyolo_hailortpp_postprocess.so missing and detection.py segfaults.
# TAPPAS ships the same YOLO post-process (same filter_letterbox entry point).
echo
echo "🔗 Checking YOLO post-process library…"
SO_DIR="$(readlink -f resources 2>/dev/null || echo /usr/local/hailo/resources)/so"
YOLO_SO="$SO_DIR/libyolo_hailortpp_postprocess.so"
TAPPAS_YOLO_SO="/usr/lib/$(uname -m)-linux-gnu/hailo/tappas/post_processes/libyolo_hailortpp_post.so"
if [[ -e "$YOLO_SO" ]]; then
  echo "✅ $YOLO_SO present"
elif [[ -f "$TAPPAS_YOLO_SO" ]]; then
  mkdir -p "$SO_DIR" 2>/dev/null || sudo mkdir -p "$SO_DIR"
  ln -sf "$TAPPAS_YOLO_SO" "$YOLO_SO" 2>/dev/null || sudo ln -sf "$TAPPAS_YOLO_SO" "$YOLO_SO"
  echo "✅ Linked $YOLO_SO -> $TAPPAS_YOLO_SO"
else
  echo "⚠️  $YOLO_SO missing and TAPPAS YOLO post-process not found; detection will fail (see HAILO_RECOVERY.md section H)"
fi


###——— FINISHED —————————————————————————————————————————————————————
cat <<EOF

🎉  All done!

To setup your environment:
    source setup_env.sh

EOF

if [[ "$NEED_REBOOT" == true ]]; then
  echo "🔁 The Hailo driver was (re)installed. Reboot before using the Hailo:"
  echo "    sudo reboot"
  echo "   Then check it with: sudo ./hailo_doctor.sh"
fi