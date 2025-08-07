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
        # Use deques for better performance with time-series data
        self.rank1_radar_speeds = deque()
        self.rank2_radar_speeds = deque()
        self.rank3_radar_speeds = deque()
        self.ser = None
        self.is_calibrating = {}
        
    def init_radar(self, port: str, baudrate: int = 9600, max_age=10, max_diff_rais=15, calibration_required=2):
        """
        Initialize radar connection parameters.
        
        Args:
            port: Serial port for radar connection
            baudrate: Baud rate for serial communication
            max_age: Maximum age of speed readings in seconds
            max_diff_rais: Maximum difference between consecutive readings
            calibration_required: Number of calibration readings required
        """
        self.radar_port = port
        self.radar_baudrate = baudrate
        self.max_age = max_age
        self.max_diff_rais = max_diff_rais
        self.calibration_required = calibration_required
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
                        
                        # Parse the radar data with thread safety
                        try:
                            
                            # Reset counter if speed difference is too large
                            if previous_reading is not None and abs(speed - previous_reading) > self.max_diff_rais:
                                self.count_radar = 0
                            
                            previous_reading = speed
                            
                            if speed != 0:
                                self.count_radar += 1
                                current_time = time.time()
                                
                                # Process based on count
                                if self.count_radar == 1:
                                    # First reading goes to rank1
                                    self._add_speed_to_rank(speed, self.rank1_radar_speeds, current_time)
                                    print(f"Rank1 speeds: {list(self.rank1_radar_speeds)}")
                                elif self.count_radar % 2 == 0:
                                    # Even readings go to rank2
                                    self._add_speed_to_rank(speed, self.rank2_radar_speeds, current_time)
                                    # Process rank2 to rank3 transfer
                                    self._process_rank2_to_rank3()
                                
                                print(f"Actual Radar Running Speed: {speed}, Count: {self.count_radar}")
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
        
    def get_radar_data(self, ai_speed,threshold, obj_class):
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
        # Filter rank1 speeds once - more efficient with list comprehension
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
            # Filter speeds above threshold - more efficient with list comprehension
            valid_speeds = [(ts, speed) for ts, speed in radar_speeds if speed > min_speed]
            
            if valid_speeds:
                # Get earliest timestamp (best match)
                best_match = min(valid_speeds, key=lambda x: x[0])
                
                # Remove used speed
                radar_speeds.remove(best_match)
                
                return best_match[1]
        
        # No valid speeds found in any rank
        return None
        
    def _cleanup_old_speeds(self, speed_deque: deque, current_time: float) -> None:
        """
        Remove old speed entries from a deque based on max_age.
        
        Args:
            speed_deque: Deque containing (timestamp, speed) tuples
            current_time: Current timestamp
        """
        while speed_deque and (current_time - speed_deque[0][0]) >= self.max_age:
            speed_deque.popleft()
    
    def _add_speed_to_rank(self, speed: int, rank_deque: deque, current_time: float) -> None:
        """
        Add speed to a rank deque with cleanup.
        
        Args:
            speed: Speed value to add
            rank_deque: Target deque to add to
            current_time: Current timestamp
        """
        self._cleanup_old_speeds(rank_deque, current_time)
        rank_deque.append((current_time, speed))
    
    def _process_rank2_to_rank3(self) -> None:
        """
        Process rank2 speeds and move oldest to rank3 when rank2 has 2 entries.
        """
        if len(self.rank2_radar_speeds) >= 2:
            # Move oldest speed from rank2 to rank3
            oldest_speed = self.rank2_radar_speeds.popleft()
            self.rank3_radar_speeds.append(oldest_speed)
            
            # Keep only the most recent entry in rank3
            if len(self.rank3_radar_speeds) > 1:
                self.rank3_radar_speeds.popleft()
        