# Hailo on Raspberry Pi 5: recovery runbook

Written from the 2026-11-11 bring-up on Pi OS Trixie, kernel `6.18.60+rpt-rpi-2712`, HailoRT and `hailort-pcie-driver` 4.23.0.

**Always start with the check script.** It is read-only:

```bash
cd ~/SVDS && sudo ./hailo_doctor.sh
```

Then go to the section the script points you to. Run all `hailortcli` commands from `/tmp`, because the tool writes `hailort.log` into the current directory.

> **Do not run `fix_hailo_drivers.sh`.** It builds driver **v4.20.0**, which does not match the 4.23 HailoRT runtime.
> **Do not create `/dev/hailo0` by hand with `mknod`.** The driver creates it only after a successful probe.

---

## A. `hailo_pci` missing, or built for a different kernel

**Symptoms:**
- `modinfo hailo_pci` says "not found", or its `vermagic` is not the same as `uname -r`.

**Cause:** an `apt upgrade` installed a new kernel, and DKMS is not installed to rebuild the driver for it.

```bash
sudo apt install -y build-essential linux-headers-$(uname -r)
```
```bash
cd /usr/src/hailort-pcie-driver/linux/pcie && sudo make clean && sudo make all -j$(nproc) && sudo make install && sudo depmod -a
```
```bash
sudo reboot
```

Reboot rather than running `modprobe`, so the device is set up once, cleanly, at boot. That avoids section B. Afterwards, do section D so this doesn't happen again.

## B. "Probing: Failed reading device BARs, device may be disconnected"

**Symptoms:**
- `lspci` lists the Hailo, but there is no `Kernel driver in use: hailo` and no `/dev/hailo0`.
- The doctor script reports "BAR0 in hardware is 0" or "Max Payload mismatch".

**Cause:** the Hailo lost its PCIe configuration after the kernel set it up at boot: its BARs (address registers) and Max Payload Size (MPS) went back to their defaults. The driver reads the vendor ID through BAR0 and gets nothing back. Nothing in the kernel logs a reset, so it happens at the link or hardware level.

**Fix: reboot.**

```bash
sudo reboot
```

On the first Pi, one clean reboot with the correct driver installed fixed both the probe failure and the 0-FPS DMA problem.

**Avoid "rescan" workarounds.** Removing the device and rescanning the bus (`echo 1 > .../remove; echo 1 > /sys/bus/pci/rescan`) gets `/dev/hailo0` back, but it does **not** restore MPS. Inference then runs at 0 FPS: first with Malformed TLP and Completion Timeout errors, and even after correcting MPS by hand, DMA stayed stuck. Neither a function-level reset nor reloading the driver cleared it. Only a reboot did.

**If B keeps coming back after reboots**, the PCIe link is marginal:
1. Power off, then reseat the M.2 card and the FPC ribbon cable.
2. Drop the link to Gen 2. Raspberry Pi does not certify Gen 3, and a Hailo-8L needs very little bandwidth.
   ```bash
   sudo sed -i 's/^pcie_gen3=1/#pcie_gen3=1\ndtparam=pciex1_gen=2/' /boot/firmware/config.txt && sudo reboot
   ```
3. Check the power supply: `vcgencmd get_throttled` should print `0x0`. Use the official 27 W supply.

## C. Driver loads and identifies, but inference gives 0 FPS / HAILO_TIMEOUT

1. Check for PCIe errors on the device:
   ```bash
   sudo lspci -vvv -d 1e60: | grep -E "UESta|DevSta"
   ```
   `MalfTLP+` or `CmpltTO+` means config space was lost or MPS doesn't match: do **section B** (reboot).
2. Check the descriptor page setting:
   ```bash
   grep force_desc /etc/modprobe.d/hailo_pci.conf
   ```
   It must be `force_desc_page_size=4096`. 16384 makes HEF configuration fail.
3. Make sure the HEF matches the chip; see section E.

## D. Prevent section A after kernel updates

```bash
sudo apt install -y dkms
```
```bash
sudo dpkg-reconfigure hailort-pcie-driver
```

The package's install script registers the driver with DKMS, so it is rebuilt automatically for every new kernel.

`./install.sh` now does all of this automatically: it installs `dkms`, restores missing package files as below, reinstalls the driver if needed, and tells you to reboot.

If `dpkg-reconfigure` prints `Failed. Exited with status 1` and `/usr/src/hailort-pcie-driver` does not exist, the package's files are gone. Confirm with `dpkg -V hailort-pcie-driver`; on the second Pi the firmware in `/lib/firmware/hailo` was missing too. The package's own remove script deletes both folders (`rm -rf`), and both its remove and install scripts `cd` into the source folder, so a plain `apt install --reinstall` fails too.

Typical signs: `/var/log/hailort-pcie-driver.deb.log` shows `pushd: /usr/src/hailort-pcie-driver/linux/pcie: No such file or directory`, and `hailo_doctor.sh` goes from `[OK]` to `[FAIL] hailo_pci module not installed`, because the install script ran `make uninstall` before it failed.

First put the package's own files back from the `.deb`, then reinstall; the reinstall also builds through DKMS:

```bash
sudo apt install --download-only --reinstall -y hailort-pcie-driver
mkdir -p ~/hailo_deb && dpkg-deb -x /var/cache/apt/archives/hailort-pcie-driver_4.23.0_all.deb ~/hailo_deb
sudo cp -a ~/hailo_deb/usr/src/hailort-pcie-driver /usr/src/
sudo mkdir -p /lib/firmware/hailo && sudo cp -a ~/hailo_deb/lib/firmware/hailo/. /lib/firmware/hailo/
dpkg -V hailort-pcie-driver        # should now print nothing
sudo apt install --reinstall -y hailort-pcie-driver
```
```bash
dkms status
modinfo -F vermagic hailo_pci
```

Expected: `hailo_pci/4.23.0, <kernel>, aarch64: installed`, and a vermagic matching `uname -r`. Then reboot. Change `4.23.0` if `dpkg-query -W hailort-pcie-driver` shows a different version.

## E. Hailo-8 or Hailo-8L: pick the right models

`lspci` shows "Hailo-8" for **both** chips, because they share PCI ID `1e60:2864`. To see which one you have:

```bash
cd /tmp && hailortcli fw-control identify | grep Architecture
```

| Architecture | Models to use |
|---|---|
| `HAILO8L` | `resources/models/hailo8l/`. Run `./download_hailo8l_models.sh` first, because the folder ships empty. |
| `HAILO8` | `resources/models/hailo8/`. 8L models also run on a Hailo-8, but slower. |

Check which chip a HEF was compiled for:

```bash
hailortcli parse-hef FILE.hef | grep Architecture
```

Then set `default_arguments.hef_path` in `configuration.json` to that model's **absolute** path. The template points into another user's home directory.

Reference speeds on the first Pi (HAILO8L, Gen 3 x1):

| Model | FPS |
|---|---|
| yolov8s | 54 |
| yolov11s | 40 |
| yolov8m | 28 |
| resnet_v1_50 | 47 |

## F. Known harmless noise

On kernel 6.18, `dmesg` shows `WARNING ... find_vma` traces from `hailo_vdma_buffer_map`. Driver 4.23 calls `find_vma` without taking the memory-map lock that kernel 6.18 now checks for. Inference works anyway.

## G. SVDS Python environment: `ImportError: cannot import name 'NoBrokersAvailable'`

`kafka-python` 3.x removed `NoBrokersAvailable`. `requirements.txt` only says `>=2.0.0`, so pip installs 3.x. Pin the last 2.x release:

```bash
cd ~/SVDS && source setup_env.sh && pip install "kafka-python==2.3.3"
```

Tested on the first Pi on 2026-11-11.

## H. Detection segfaults: `Could not load lib /usr/local/hailo/resources/so/libyolo_hailortpp_postprocess.so`

**Cause:** hailo_apps' C++ post-processing libraries don't compile against TAPPAS 5.1 headers (`HailoTensor` has no member `vstream_info`). The build fails silently during install, and `/usr/local/hailo/resources/so/` stays empty.

**Fix:** TAPPAS ships the same YOLO post-processor, with the same `filter_letterbox` entry point. Point the expected filename at it:

```bash
ln -s /usr/lib/aarch64-linux-gnu/hailo/tappas/post_processes/libyolo_hailortpp_post.so /usr/local/hailo/resources/so/libyolo_hailortpp_postprocess.so
```

This covers YOLO detection only. The pose, segmentation, depth and face (scrfd) pipelines would need the same treatment with their TAPPAS equivalents; untested.

## I. Running and stopping detection

- **On a video file:**
  ```bash
  PYTHONUNBUFFERED=1 python basic_pipelines/detection.py --input resources/videos/example.mp4 --disable-sync --disable-display --show-fps
  ```
  It loops the file. Expect about 54 FPS with yolov11s on a Hailo-8L.
- **Unconfigured Kafka and a missing radar are not fatal.** They log `NoBrokersAvailable` and `could not open port /dev/ttyACMr` and retry.
- **Stopping:** the app may ignore SIGTERM. Ctrl+C works interactively; otherwise use `pkill -9 -f detection.py`.
- **Without `PYTHONUNBUFFERED=1`**, SVDS's own output is buffered and you only see GStreamer warnings.
- **`GStreamer-CRITICAL: write map requested on non-writable buffer` is harmless.** It comes from `hailofilter` (the YOLO post-process step), once per frame. `hailofilter` asks for write access to every frame, but `hailocropper` shares the frame with its bypass branch to `hailoaggregator`, so GStreamer refuses and prints this. The YOLO post-process only reads the Hailo output tensors, never the pixels, so detections are unaffected (checked on 2026-11-12 by removing elements one by one with `gst-launch-1.0`, and by saving annotated frames). `hailofilter` has no property to turn it off. `detection.py` and `detection_display.py` call `throttle_gst_warnings()` from `helper_utils.py`, so it prints at most once every 10 seconds with a `(+N repeats hidden)` count. To change that, pass a different `interval` (seconds) to `throttle_gst_warnings()`. Any **other** `GStreamer-CRITICAL` is still printed in full and should be looked at.

## J. RTSP camera: detection fails to start, file input works

**Symptoms:** `detection.py --input "rtsp://..."` exits or never shows FPS lines, even though the camera plays in `ffprobe`/VLC. Files work.

**Cause:** hailo-apps builds the RTSP source as `rtspsrc ! rtph265depay ! h265parse ! decodebin ! ... ! video/x-raw, format=I420`. On a Pi 5, `decodebin` picks the hardware decoder `v4l2slh265dec`, which outputs `NV12_128C8`. That cannot negotiate to `I420`, so the pipeline fails and `rtspsrc` reports `Could not send message`.

**Fix:** add `videoconvert` after `decodebin`. `./install.sh` does this automatically; on an existing install run:

```bash
cd ~/SVDS && git pull && ./fix_rtsp_pipeline.sh
```

It edits `gstreamer_helper_pipelines.py` inside the venv's `hailo_apps` package, keeps a `.orig` backup, and is safe to run again. Rerun it after anything that reinstalls hailo-apps (`pip install`, `./install.sh` does it for you).

**Checks:**

1. The camera sends H.265. The pipeline only accepts H.265, so `codec_name` must be `hevc`; if it is `h264`, change the camera's video encoding to H.265 in its web page:
   ```bash
   ffprobe -v error -rtsp_transport tcp -select_streams v:0 -show_entries stream=codec_name,width,height,avg_frame_rate -of default=nw=1 "RTSP_URL"
   ```
2. Whether this Pi needs the fix: `NV12_128C8` in the output means yes.
   ```bash
   cd /tmp && gst-launch-1.0 -v rtspsrc location="RTSP_URL" ! rtph265depay ! h265parse ! decodebin ! fakesink num-buffers=30 2>&1 | grep -oE 'decodebin0/[a-z0-9_]+|NV12_128C8' | sort -u
   ```
3. Run detection. Expect FPS close to the camera's frame rate (15 on the first test camera) with `Droprate: 0.00`:
   ```bash
   cd ~/SVDS && source setup_env.sh && PYTHONUNBUFFERED=1 python basic_pipelines/detection.py --input "RTSP_URL" --disable-sync --disable-display --show-fps
   ```

**URL tips:** always put the URL in double quotes (`&` breaks the command otherwise). Encode special characters in the password: `@` as `%40`, `#` as `%23`, `:` as `%3A`, `/` as `%2F`. Dahua-style cameras use `/cam/realmonitor?channel=1&subtype=0` (`subtype=1` is the lighter sub-stream); Hikvision uses `/Streaming/Channels/101`. For `./run_detection.sh`, put the URL in `camera_details.RTSP_URL` in `configuration.json`.

## Order of operations on a fresh Pi

1. `source setup_system.sh` and `./install.sh`
2. `sudo ./hailo_doctor.sh`, then fix whatever it flags (usually section A, then reboot)
3. Section D (DKMS). `install.sh` already does this; if it reported a driver reinstall, reboot and rerun the doctor script
4. Section E (models and `configuration.json`)
5. Sections G and H (the kafka-python pin and the YOLO post-process symlink) are handled by `./install.sh` from this commit on. On a Pi installed earlier, run `git pull && ./install.sh` again, or apply them by hand
   The RTSP fix (section J) is also applied by `./install.sh`; on an older install run `./fix_rtsp_pipeline.sh`
6. Test on a file before using RTSP:
   ```bash
   source setup_env.sh && python basic_pipelines/detection.py --input resources/videos/example.mp4 --disable-sync --disable-display
   ```
7. Then test the camera (section J, checks 1 and 3)
