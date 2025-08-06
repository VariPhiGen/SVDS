import os
import uuid
import cv2
import numpy as np
from datetime import datetime
from collections import deque


class VideoClipRecorder:
    def __init__(self, maxlen=150, fps=20, prefix: str = "clips"):
        """
        Initialize video recorder for frame buffering and video generation.
        
        Args:
            maxlen: Maximum number of frames to buffer
            fps: Frames per second for video encoding
            prefix: Prefix for video filenames (used by external upload handlers)
        """
        self.prefix = prefix
        self.frame_buffer = deque(maxlen=maxlen)
        self.fps = fps
        
        print(f"Initiated VideoClipRecorder with buffer size {maxlen}")

    def add_frame(self, frame: np.ndarray):
        """Add a frame to the RAM-backed buffer."""
        try:
            self.frame_buffer.append(frame.copy())
        except Exception as e:
            print(f"[ERROR] Failed to add frame to buffer: {e}")

    def generate_video_bytes(self) -> bytes | None:
        """Encode the buffered frames as an MP4 video in memory and return bytes."""
        if not self.frame_buffer:
            return None

        try:
            frame_buffer_copy = [frame.copy() for frame in list(self.frame_buffer)]
            height, width, _ = frame_buffer_copy[0].shape
            fourcc = cv2.VideoWriter_fourcc(*'h264')
            temp_path = f"/tmp/{uuid.uuid4()}_New.mp4"

            writer = cv2.VideoWriter(temp_path, fourcc, self.fps, (width, height))
            for frame in frame_buffer_copy:
                writer.write(frame)
            writer.release()

            with open(temp_path, 'rb') as f:
                video_bytes = f.read()

            os.remove(temp_path)
            return video_bytes

        except Exception as e:
            print(f"[ERROR] Failed to generate video bytes: {e}")
            return None

    def save_images(self, org_img, save_dir: str, suffix: str):
        """Save a decoded OpenCV image (`numpy.ndarray`, dtype uint8, BGR)."""
        try:
            os.makedirs(save_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            org_path = os.path.join(save_dir, f"org_img_{timestamp}_{suffix}.png")

            if not isinstance(org_img, np.ndarray):
                raise TypeError(f"Expected numpy.ndarray, got {type(org_img)}")

            if org_img.dtype != np.uint8:
                org_img = org_img.astype(np.uint8)

            if not org_img.flags['C_CONTIGUOUS']:
                org_img = np.ascontiguousarray(org_img)

            success = cv2.imwrite(org_path, org_img)
            if not success:
                raise IOError(f"cv2.imwrite failed for image at {org_path}")

            return org_path

        except Exception as e:
            print(f"[ERROR] Failed to save image: {e}")
            return ""
