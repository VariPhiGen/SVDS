import serial
import time
from collections import deque
from threading import Thread, Lock
from typing import Optional, Dict, Any, List, Tuple


class RadarHandler:
    """Handles radar communication and speed data processing."""
    
    def __init__(self):
        """Initialize radar handler."""
        self.radar_data = deque(maxlen=5)  # Dictionary to store radar data for each tracked object
        self.radar_port = None  # Serial port for radar
        self.radar_baudrate = 9600  # Default baudrate
        self.radar_thread = None
        self.radar_running = False
        self.radar_lock = Lock()  # Lock for thread-safe radar data access
        self.count_radar = 0  # Count radar until it will become 0 again
        self.rank1_radar_speeds = []
        self.rank2_radar_speeds = []
        self.rank3_radar_speeds = []
        self.ser = None
        self.is_calibrating={}
        
    def init_radar(self, port: str, baudrate: int = 9600,max_age=10, max_diff_rais=15,calibration_required=2):
        """
        Initialize radar connection parameters.
        
        Args:
            port: Serial port for radar connection
            baudrate: Baud rate for serial communication
        """
        self.radar_port = port
        self.radar_baudrate = baudrate
        self.max_age = max_age
        self.max_diff_rais = max_diff_rais
        self.calibration_required = calibration_required  # Will be set from config
        self.class_calibration_count = {}  # Track calibration count per class
        self.ser = None
        self._connect()
        
    def start_radar(self):
        """Start the radar reading thread."""
        if not self.radar_running and self.radar_port:
            self.radar_running = True
            self.radar_thread = Thread(target=self._radar_read_loop, daemon=True)
            self.radar_thread.start()

    def stop_calbirating(self,obj_class):
        self.is_calibrating[obj_class]=False
            
    def stop_radar(self):
        """Stop the radar reading thread."""
        self.radar_running = False
        if self.radar_thread:
            self.radar_thread.join()
    
    def _connect(self) -> None:
        """Establish serial connection to radar."""
        try:
            self.ser = serial.Serial(
                port=self.radar_port,
                baudrate=self.radar_baudrate,
                timeout=0.1,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE
            )
        except Exception as e:
            raise RuntimeError(f"Failed to connect to radar: {e}")
            
    def set_error_logger(self, error_logger):
        """Set the error logger function for sending errors to Kafka."""
        self.error_logger = error_logger
    
    def _process_speed_data(self, data: bytes) -> Optional[Dict[str, Any]]:
        """
        Process raw radar speed data.
        
        Args:
            data: Raw bytes from radar
            
        Returns:
            Processed speed data dictionary or None
        """
        if not data:
            return None
            
        try:
            # Convert bytes to list of hex strings for debugging
            hex_data = [hex(x) for x in data]
            # Debug data removed - can be added back if needed
            
            # Process each 4-byte chunk
            for i in range(len(data) - 3):
                # Check for target speed pattern: 0xFC 0xFA sum 0x00
                if data[i] == 0xFC and data[i+1] == 0xFA and data[i+3] == 0x00:
                    speed_raw = data[i+2]
                    if 0x0F <= speed_raw <= 0xFA:  # Valid speed range
                        speed_kmh = speed_raw
                        direction = 'Approaching'
                        return {
                            'speed': speed_kmh,
                            'direction': direction,
                            'type': 'Primary Target'
                        }
                
                # Check for leading target speed pattern: 0xFB 0xFD sum 0x00
                elif data[i] == 0xFB and data[i+1] == 0xFD and data[i+3] == 0x00:
                    speed_raw = data[i+2]
                    if 0x00 <= speed_raw <= 0xFA:  # Valid speed range
                        speed_kmh = speed_raw
                        direction = 'Receding'
                        
                        return {
                            'speed': speed_kmh,
                            'direction': direction,
                            'type': 'Leading Target'
                        }
            
            return None
        except Exception as e:
            if hasattr(self, 'error_logger') and self.error_logger:
                self.error_logger(f"Error processing speed data: {e}")
            return None

    def get_speed(self) -> Optional[Dict[str, Any]]:
        """
        Get current speed reading from radar.
        
        Returns:
            Speed data dictionary or None
        """
        if not self.ser:
            try:
                self._connect()
            except Exception as e:
                if hasattr(self, 'error_logger') and self.error_logger:
                    self.error_logger(f"Radar reconnection failed: {e}")
                return None

        try:
            # Read 4 bytes at a time (protocol frame size)
            self.ser.reset_input_buffer()
            data = self.ser.read(4)
            if len(data) == 4:
                return self._process_speed_data(data)
        except Exception as e:
            if hasattr(self, 'error_logger') and self.error_logger:
                self.error_logger(f"Radar read error: {e}")
            self.ser = None  # Force reconnection on next attempt
        return None
    
    def _radar_read_loop(self):
        """Main radar reading loop running in a separate thread."""
        previous_reading = None
        time.sleep(1)
        try:
            while self.radar_running:
                try:
                    speed_data = self.get_speed()
                    if speed_data is not None:
                        speed = speed_data['speed']
                        direction = speed_data['direction']
                        target_type = speed_data['type']   
                        
                        # Parse the radar data
                        try:
                            if previous_reading is not None:
                                if abs(speed - previous_reading) > 4:
                                    self.count_radar = 0
                            
                            previous_reading = speed
                                
                            if speed != 0:
                                self.count_radar += 1
                                if self.count_radar == 1:
                                    current_time = time.time()
                                    self.rank1_radar_speeds = [(ts, speed) for ts, speed in self.rank1_radar_speeds if (current_time - ts) < self.max_age]
                                    self.rank1_radar_speeds.append((time.time(), speed))
                                    print(self.rank1_radar_speeds)
                                elif self.count_radar == 2:
                                    current_time = time.time()
                                    self.rank2_radar_speeds = [(ts, speed) for ts, speed in self.rank1_radar_speeds if (current_time - ts) < self.max_age]
                                    self.rank2_radar_speeds.append((time.time(), speed))
                                    print(self.rank2_radar_speeds)
                                elif self.count_radar == 3:
                                    current_time = time.time()
                                    self.rank3_radar_speeds = [(ts, speed) for ts, speed in self.rank1_radar_speeds if (current_time - ts) < self.max_age]
                                    self.rank3_radar_speeds.append((time.time(), speed))
                                    print(self.rank3_radar_speeds)
                                    # Debug info removed - can be added back if needed
                                # Debug info removed - can be added back if needed
                                print("Actual Radar Running Speed",speed, self.count_radar)
                            else:
                                self.count_radar = 0
                        except (ValueError, IndexError) as e:
                            if hasattr(self, 'error_logger') and self.error_logger:
                                self.error_logger(f"Error parsing radar data: {e}")
                            continue
                            
                except serial.SerialException as e:
                    if hasattr(self, 'error_logger') and self.error_logger:
                        self.error_logger(f"Serial communication error: {e}")
                    break
                        
        except serial.SerialException as e:
            if hasattr(self, 'error_logger') and self.error_logger:
                self.error_logger(f"Failed to open radar serial port: {e}")
        
    def get_radar_speed(self, ai_speed,threshold, obj_class):
        """
        Optimized radar speed matching with early exit and cleaner logic
        """
        with self.radar_lock:
            # Early exit if no radar data available
            if not any([self.rank1_radar_speeds, self.rank2_radar_speeds, self.rank3_radar_speeds]):
                return None
            
            # Filter speeds above threshold once
            min_speed = threshold - 5
            
            # Process calibration mode
            if self.is_calibrating[obj_class]:
                return self._handle_calibration_mode(ai_speed, obj_class, min_speed)
            
            # Normal mode: try each rank in order
            return self._get_best_match_from_ranks(ai_speed, min_speed)

    def _handle_calibration_mode(self, ai_speed, obj_class, min_speed):
        """
        Handle calibration mode with early exit
        """
        # Filter rank1 speeds once
        valid_speeds = [(ts, speed) for ts, speed in self.rank1_radar_speeds if speed > min_speed]
        
        if not valid_speeds:
            return None
        
        # Find best match
        best_match = min(valid_speeds, key=lambda x: abs(x[1] - ai_speed))
        
        # Update calibration count
        if self.class_calibration_count[obj_class] <= self.calibration_required:
            self.class_calibration_count[obj_class] += 1
            print(f"Calibration for {obj_class}: {self.class_calibration_count[obj_class]}/{self.calibration_required} done.")
        else:
            self.stop_calbirating(obj_class)
        
        # Remove used speed and return
        self.rank1_radar_speeds.remove(best_match)
        return best_match[1]

    def _get_best_match_from_ranks(self, ai_speed, min_speed):
        """
        Get best match from available ranks with early exit
        """
        # Define ranks to check in order of priority
        rank_configs = [
            (self.rank1_radar_speeds, 'rank1'),
            (self.rank2_radar_speeds, 'rank2'), 
            (self.rank3_radar_speeds, 'rank3')
        ]
        
        for radar_speeds, rank_name in rank_configs:
            # Filter speeds above threshold
            valid_speeds = [(ts, speed) for ts, speed in radar_speeds if speed > min_speed]
            
            if valid_speeds:
                # Get earliest timestamp (best match)
                best_match = min(valid_speeds, key=lambda x: x[0])
                
                # Remove used speed
                radar_speeds.remove(best_match)
                
                return best_match[1]
        
        # No valid speeds found in any rank
        return None
        