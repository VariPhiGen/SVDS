"""Draw the speed zone and lane line on a camera frame and save them.

Pressing 's' writes:
  - zones_and_lines.json and org_img.png in the current folder (as before)
  - the zone and line straight into configuration.json, under
    activities_data.traffic_overspeeding_distancewise (zones.zone1 and
    parameters.lines.lane1), with a backup in configuration.json.bak

Examples:
  python basic_pipelines/zone_selector_SVDS.py                      # camera from configuration.json
  python basic_pipelines/zone_selector_SVDS.py --source "rtsp://..." --real-distance 15
  python basic_pipelines/zone_selector_SVDS.py --source video.mp4 --no-config-update
"""
import argparse
import json
import os
import re
import shutil
from pathlib import Path

import cv2
import numpy as np

ACTIVITY = "traffic_overspeeding_distancewise"
DEFAULT_CONFIG = Path(__file__).resolve().parent.parent / "configuration.json"


def mask_credentials(url):
    """rtsp://user:pass@host -> rtsp://***:***@host, so saved files don't hold passwords."""
    return re.sub(r"(://)[^/@]+@", r"\1***:***@", url)


def update_config(config_path, zone_points, line_points, zone_name="zone1",
                  lane_name="lane1", real_distance=None, source=None):
    """Write the zone and line into configuration.json. Returns True on success."""
    config_path = Path(config_path)
    with open(config_path) as f:
        config = json.load(f)

    activity = config.setdefault("activities_data", {}).setdefault(ACTIVITY, {})
    activity.setdefault("zones", {})[zone_name] = [list(p) for p in zone_points]
    params = activity.setdefault("parameters", {})
    params.setdefault("lines", {})[lane_name] = [list(p) for p in line_points]
    if real_distance is not None:
        params["real_distance"] = real_distance

    # Keep the config as it was before this run's first save.
    backup = config_path.with_name(config_path.name + ".bak")
    if not getattr(update_config, "_backed_up", False):
        shutil.copy2(config_path, backup)
        update_config._backed_up = True

    # Write to a temp file then rename, so a crash can't leave half a config.
    tmp = config_path.with_name(config_path.name + ".tmp")
    with open(tmp, "w") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
        f.write("\n")
    os.replace(tmp, config_path)

    print(f"✅ Updated {config_path} (backup: {backup})")
    print(f"   zones.{zone_name} = {activity['zones'][zone_name]}")
    print(f"   lines.{lane_name} = {params['lines'][lane_name]}")
    print(f"   real_distance = {params.get('real_distance')} m"
          + ("" if real_distance is not None else "  <- not changed, pass --real-distance to set it"))

    cam_url = config.get("camera_details", {}).get("RTSP_URL")
    if source and cam_url and source != cam_url:
        print("⚠️  This source is not the camera_details.RTSP_URL in the config. The points only "
              "fit if SVDS runs on a stream with the same resolution.")
    return True


class ZoneDrawer:
    def __init__(self, source, config_path=None, zone_name="zone1", lane_name="lane1",
                 real_distance=None):
        self.source = source
        self.config_path = config_path
        self.zone_name = zone_name
        self.lane_name = lane_name
        self.real_distance = real_distance
        self.cap = cv2.VideoCapture(self.source, cv2.CAP_FFMPEG)

        if not self.cap.isOpened():
            raise ValueError("Cannot open video source")

        ret, frame = self.cap.read()
        if not ret:
            raise ValueError("Couldn't read from video source")

        self.first_frame = frame
        self.original_height, self.original_width = frame.shape[:2]
        print(f"Frame size: {self.original_width}x{self.original_height}")
        self.resize_width = 480
        self.resize_height = int(self.original_height * (self.resize_width / self.original_width))
        self.scale_x = self.original_width / self.resize_width
        self.scale_y = self.original_height / self.resize_height

        self.saved_polygon = []
        self.saved_line = []
        self.current_polygon = []
        self.current_line = []
        self.mode = 'polygon'

        self.window_name = 'Zone & Line Drawer'
        cv2.namedWindow(self.window_name)
        cv2.setMouseCallback(self.window_name, self.mouse_callback)
        print("Click the zone corners, press 'f'. Click 2 line points, press 'f'. "
              "Then 's' to save, 'q' to quit.")

    def mouse_callback(self, event, x, y, flags, param):
        if event == cv2.EVENT_LBUTTONDOWN:
            if self.mode == 'polygon':
                self.current_polygon.append((x, y))
            elif self.mode == 'line' and len(self.current_line) < 2:
                self.current_line.append((x, y))

    def save_zones(self, path="zones_and_lines.json"):
        # Prepare zone and line data
        zone_points = [(int(x * self.scale_x), int(y * self.scale_y)) for x, y in self.saved_polygon]
        line_points = [(int(x * self.scale_x), int(y * self.scale_y)) for x, y in self.saved_line]

        if len(zone_points) < 3 or len(line_points) != 2:
            print("❌ Nothing saved: finish a zone (3+ points, 'f') and a line (2 points, 'f') first.")
            return

        data = {
            "source": mask_credentials(self.source),
            "zone": {
                "name": "box",
                "points": zone_points
            },
            "line": {
                "name": "line",
                "points": line_points
            }
        }

        # Save JSON without spaces
        with open(path, 'w') as f:
            json.dump(data, f, separators=(',', ':'))
        print(f"✅ Saved JSON to {path}")

        # Draw on the frame that was clicked on
        frame = self.first_frame.copy()
        cv2.polylines(frame, [np.array(zone_points)], isClosed=True, color=(0, 255, 0), thickness=2)
        cv2.line(frame, line_points[0], line_points[1], (0, 0, 255), 2)
        cv2.imwrite("org_img.png", frame)
        print("✅ Saved image with polygon and line to org_img.png")

        if self.config_path:
            try:
                update_config(self.config_path, zone_points, line_points, self.zone_name,
                              self.lane_name, self.real_distance, self.source)
            except Exception as e:
                print(f"❌ Could not update {self.config_path}: {e}")

    def run(self):
        frame = self.first_frame
        while True:
            display_frame = cv2.resize(frame, (self.resize_width, self.resize_height))

            if self.saved_polygon:
                cv2.polylines(display_frame, [np.array(self.saved_polygon)], isClosed=True, color=(0, 255, 0), thickness=2)

            if self.saved_line and len(self.saved_line) == 2:
                cv2.line(display_frame, self.saved_line[0], self.saved_line[1], (0, 0, 255), 2)

            if self.current_polygon:
                cv2.polylines(display_frame, [np.array(self.current_polygon)], isClosed=False, color=(0, 255, 255), thickness=1)

            if len(self.current_line) == 1:
                cv2.circle(display_frame, self.current_line[0], 3, (0, 0, 255), -1)
            elif len(self.current_line) == 2:
                cv2.line(display_frame, self.current_line[0], self.current_line[1], (0, 0, 255), 1)

            cv2.imshow(self.window_name, display_frame)
            key = cv2.waitKey(1) & 0xFF

            if key == ord('q'):
                break
            elif key == ord('s'):
                self.save_zones()
            elif key == ord('f'):
                if self.mode == 'polygon' and self.current_polygon:
                    self.saved_polygon = self.current_polygon.copy()
                    self.current_polygon.clear()
                    print(f"✅ Polygon saved with 'f': {self.saved_polygon}")
                    self.mode = 'line'
                    print("Switched to LINE mode")
                elif self.mode == 'line' and len(self.current_line) == 2:
                    self.saved_line = self.current_line.copy()
                    self.current_line.clear()
                    print(f"✅ Line saved with 'f': {self.saved_line}")
                    self.mode = 'polygon'
                    print("Switched to POLYGON mode")
            elif key == ord('l'):
                self.mode = 'line'
                print("Switched to LINE mode")
            elif key == ord('c'):
                self.mode = 'polygon'
                print("Switched to POLYGON mode")

        self.cap.release()
        cv2.destroyAllWindows()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Draw the speed zone and lane line, save to configuration.json")
    parser.add_argument("--source", type=str, default=None,
                        help="RTSP URL or video file (default: camera_details.RTSP_URL from the config)")
    parser.add_argument("--config", type=str, default=str(DEFAULT_CONFIG),
                        help=f"configuration file to update (default: {DEFAULT_CONFIG})")
    parser.add_argument("--real-distance", type=float, default=None,
                        help="metres on the road between the 2 line points (also written to the config)")
    parser.add_argument("--zone-name", default="zone1", help="zone key in the config (default: zone1)")
    parser.add_argument("--lane-name", default="lane1", help="line key in the config (default: lane1)")
    parser.add_argument("--no-config-update", action="store_true",
                        help="only write zones_and_lines.json, leave configuration.json alone")
    args = parser.parse_args()

    source = args.source
    if source is None:
        with open(args.config) as f:
            source = json.load(f).get("camera_details", {}).get("RTSP_URL")
        if not source:
            parser.error(f"no --source given and no camera_details.RTSP_URL in {args.config}")
        print(f"Using camera from {args.config}: {mask_credentials(source)}")

    drawer = ZoneDrawer(source,
                        config_path=None if args.no_config_update else args.config,
                        zone_name=args.zone_name, lane_name=args.lane_name,
                        real_distance=args.real_distance)
    drawer.run()
