import io
import json
import time
import uuid
import queue
import boto3
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from typing import Optional, Dict, Any, List
from video_clipper import VideoClipRecorder


class KafkaHandler:
    """Handles Kafka message production and S3 uploads with dual broker and dual S3 redundancy."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Kafka handler with configuration.
        
        Args:
            config: Configuration dictionary containing Kafka and AWS settings
        """
        self.config = config
        self.kafka_pipeline = None
        self.s3_clients = {}
        self.recorder = None
        self.last_error_time = 0
        self.error_interval = 300
        
        # Dual broker redundancy settings
        self.brokers = self._get_broker_list()
        self.current_broker_index = 0
        self.broker_failover_timeout = config.get("kafka_variables", {}).get("broker_failover_timeout", 30)
        self.last_broker_failure = 0
        self.broker_health = {broker: True for broker in self.brokers}
        
        # Dual S3 redundancy settings
        self.s3_configs = self._get_s3_configs()
        self.current_s3_index = 0
        self.s3_failover_timeout = config.get("kafka_variables", {}).get("AWS_S3", {}).get("s3_failover_timeout", 30)
        self.last_s3_failure = 0
        self.s3_health = {name: True for name in self.s3_configs.keys()}
        
        print(f"DEBUG: Initialized with {len(self.brokers)} brokers and {len(self.s3_configs)} S3 buckets")
        
        self._setup_aws_s3()
        self._setup_video_recorder()
        
    def _get_broker_list(self) -> List[str]:
        """Get list of brokers from configuration."""
        kafka_config = self.config.get("kafka_variables", {})
        
        bootstrap_servers = kafka_config.get("bootstrap_servers")
        if isinstance(bootstrap_servers, list):
            return bootstrap_servers
        
        primary = kafka_config.get("primary_broker")
        secondary = kafka_config.get("secondary_broker")
        
        brokers = []
        if primary:
            brokers.append(primary)
        if secondary:
            brokers.append(secondary)
        
        return brokers if brokers else ["localhost:9092"]
        
    def _get_s3_configs(self) -> Dict[str, Dict[str, Any]]:
        """Get S3 configurations from config."""
        aws_config = self.config.get("kafka_variables", {}).get("AWS_S3", {})
        
        s3_configs = {}
        
        # Check for new dual S3 format
        if "primary" in aws_config:
            s3_configs["primary"] = aws_config["primary"]
        if "secondary" in aws_config:
            s3_configs["secondary"] = aws_config["secondary"]
        
        # Fallback to old single S3 format
        if not s3_configs and "BUCKET_NAME" in aws_config:
            s3_configs["primary"] = aws_config
        
        return s3_configs
        
    def _setup_aws_s3(self):
        """Initialize AWS S3 clients for all configured buckets."""
        for name, config in self.s3_configs.items():
            try:
                client = boto3.client(
                    "s3",
                    aws_access_key_id=config.get("aws_access_key_id"),
                    aws_secret_access_key=config.get("aws_secret_access_key"),
                    region_name=config.get("region_name")
                )
                self.s3_clients[name] = client
                print(f"DEBUG: Initialized S3 client for {name}: {config.get('BUCKET_NAME')}")
            except Exception as e:
                print(f"DEBUG: Failed to initialize S3 client for {name}: {e}")
                self.s3_health[name] = False
        
    def _setup_video_recorder(self):
        """Initialize video recorder for frame buffering."""
        self.recorder = VideoClipRecorder(
            maxlen=100,
            fps=20,
            prefix="clips"
        )
        
    def _test_s3_connectivity(self, s3_name: str) -> bool:
        """Test if an S3 bucket is reachable."""
        try:
            client = self.s3_clients.get(s3_name)
            if not client:
                return False
            
            config = self.s3_configs[s3_name]
            bucket_name = config.get("BUCKET_NAME")
            
            # Test by listing objects (limited to 1)
            client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
            return True
        except Exception:
            return False
            
    def _get_next_healthy_s3(self) -> Optional[str]:
        """Get the next healthy S3 bucket in round-robin fashion."""
        current_time = time.time()
        
        # Check if enough time has passed to retry failed S3 buckets
        if current_time - self.last_s3_failure > self.s3_failover_timeout:
            for s3_name in self.s3_configs.keys():
                if not self.s3_health[s3_name]:
                    self.s3_health[s3_name] = self._test_s3_connectivity(s3_name)
        
        # Find healthy S3 buckets
        healthy_s3 = [name for name in self.s3_configs.keys() if self.s3_health[name]]
        
        if not healthy_s3:
            return None
            
        # Round-robin selection
        s3_name = healthy_s3[self.current_s3_index % len(healthy_s3)]
        self.current_s3_index += 1
        return s3_name
        
    def _test_broker_connectivity(self, broker: str) -> bool:
        """Test if a broker is reachable."""
        try:
            test_producer = KafkaProducer(
                bootstrap_servers=[broker],
                request_timeout_ms=5000,
                max_block_ms=2000
            )
            test_producer.close()
            return True
        except Exception:
            return False
            
    def _get_next_healthy_broker(self) -> Optional[str]:
        """Get the next healthy broker in round-robin fashion."""
        current_time = time.time()
        
        if current_time - self.last_broker_failure > self.broker_failover_timeout:
            for broker in self.brokers:
                if not self.broker_health[broker]:
                    self.broker_health[broker] = self._test_broker_connectivity(broker)
        
        healthy_brokers = [broker for broker in self.brokers if self.broker_health[broker]]
        
        if not healthy_brokers:
            return None
            
        broker = healthy_brokers[self.current_broker_index % len(healthy_brokers)]
        self.current_broker_index += 1
        return broker
        
    def _create_kafka_producer(self) -> Optional[KafkaProducer]:
        """Create and configure Kafka producer with dual broker redundancy."""
        broker = self._get_next_healthy_broker()
        if not broker:
            print("DEBUG: No healthy brokers available")
            return None
            
        try:
            producer = KafkaProducer(
                bootstrap_servers=[broker],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                
                acks='1',
                retries=3,
                retry_backoff_ms=500,
                compression_type='gzip',
                batch_size=1048576,
                buffer_memory=67108864,
                max_request_size=1048576,
                request_timeout_ms=5000,
                max_block_ms=1000,
                linger_ms=1000
            )
            
            producer.metrics()
            print(f"DEBUG: Connected to broker: {broker}")
            return producer
            
        except Exception as e:
            print(f"DEBUG: Failed to create Kafka producer with broker {broker}: {e}")
            self.broker_health[broker] = False
            self.last_broker_failure = time.time()
            return None
            
    def _handle_broker_failure(self):
        """Handle broker failure and switch to next available broker."""
        if self.kafka_pipeline:
            try:
                self.kafka_pipeline.close()
            except:
                pass
            finally:
                self.kafka_pipeline = None
                
        self.kafka_pipeline = self._create_kafka_producer()
        
    def upload_to_s3(self, file_bytes: bytes, file_type: str = "image", retries: int = 2, delay: int = 1) -> Optional[str]:
        """Upload file bytes to S3 with dual bucket redundancy."""
        upload_retries = self.config.get("kafka_variables", {}).get("AWS_S3", {}).get("upload_retries", 3)
        
        # Set content type and filename based on file type
        if file_type == "video":
            unique_filename = f"clips{uuid.uuid4()}.mp4"
            content_type = "video/mp4"
        else:
            unique_filename = f"{uuid.uuid4()}.jpg"
            content_type = "image/jpg"
        
        # Try each S3 bucket with retries
        for s3_name in self.s3_configs.keys():
            if not self.s3_health[s3_name]:
                continue
                
            client = self.s3_clients.get(s3_name)
            config = self.s3_configs[s3_name]
            
            if not client or not config:
                continue
            
            for attempt in range(upload_retries):
                try:
                    if file_type == "video":
                        # Use upload_fileobj for video files
                        client.upload_fileobj(
                            io.BytesIO(file_bytes),
                            Bucket=config.get("BUCKET_NAME"),
                            Key=unique_filename,
                            ExtraArgs={"ContentType": content_type}
                        )
                        # Return full S3 URL for videos
                        region = config.get("region_name")
                        bucket_name = config.get("BUCKET_NAME")
                        s3_url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{unique_filename}"
                        print(f"DEBUG: Successfully uploaded video to {s3_name}: {s3_url}")
                        return s3_url
                    else:
                        # Use put_object for images
                        client.put_object(
                            Bucket=config.get("BUCKET_NAME"),
                            Key=unique_filename,
                            Body=file_bytes,
                            ContentType=content_type
                        )
                        print(f"DEBUG: Successfully uploaded {file_type} to {s3_name}: {config.get('BUCKET_NAME')}")
                        return unique_filename
                        
                except Exception as e:
                    print(f"DEBUG: S3 {file_type} upload attempt {attempt + 1} to {s3_name} failed: {e}")
                    if attempt < upload_retries - 1:
                        time.sleep(delay)
            
            # Mark S3 as unhealthy if all attempts failed
            self.s3_health[s3_name] = False
            self.last_s3_failure = time.time()
                    
        return None
        
    def process_events_queue(self, events_queue: queue.Queue, topic: str) -> bool:
        """Process events from queue and send to Kafka with dual broker and S3 redundancy."""
        try:
            message = events_queue.get_nowait()
            if message is None or topic == "None":
                return True
                
            # Upload files to S3 with redundancy
            image_bytes = message.get("org_img")
            image_s3_url = self.upload_to_s3(image_bytes, "image") if image_bytes else None
            
            snap_shot_bytes = message.get("snap_shot")
            snap_shot_s3_url = self.upload_to_s3(snap_shot_bytes, "snapshot") if snap_shot_bytes else None
            
            video_bytes = message.get("video")
            video_s3_url = self.upload_to_s3(video_bytes, "video") if video_bytes and len(video_bytes) > 0 else None
                
            # Update message with S3 URLs
            success = False
            if image_s3_url and snap_shot_s3_url:
                message["org_img"] = image_s3_url
                message["snap_shot"] = snap_shot_s3_url
                message["video"] = video_s3_url if video_s3_url else None
                
                # Send to Kafka with dual broker redundancy
                if self.kafka_pipeline:
                    try:
                        self.kafka_pipeline.send(topic, message)
                        self.kafka_pipeline.flush()
                        success = True
                    except (KafkaError, NoBrokersAvailable) as e:
                        print(f"DEBUG: Kafka send failed: {e}")
                        self._handle_broker_failure()
                        if self.kafka_pipeline:
                            try:
                                self.kafka_pipeline.send(topic, message)
                                self.kafka_pipeline.flush()
                                success = True
                            except:
                                pass
            
            return success
                
        except queue.Empty:
            return True
        except Exception as e:
            print(f"DEBUG: Events queue processing error: {e}")
            return False
            
    def process_analytics_queue(self, analytics_queue: queue.Queue, topic: str) -> bool:
        """Process analytics from queue and send to Kafka with dual broker redundancy."""
        try:
            message = analytics_queue.get_nowait()
            if message is None or topic == "None":
                return True
                
            if self.kafka_pipeline:
                try:
                    self.kafka_pipeline.send(topic, message)
                    self.kafka_pipeline.flush()
                    return True
                except (KafkaError, NoBrokersAvailable) as e:
                    print(f"DEBUG: Kafka analytics send failed: {e}")
                    self._handle_broker_failure()
                    if self.kafka_pipeline:
                        try:
                            self.kafka_pipeline.send(topic, message)
                            self.kafka_pipeline.flush()
                            return True
                        except:
                            pass
            
            return False
                
        except queue.Empty:
            return True
        except Exception as e:
            print(f"DEBUG: Analytics queue processing error: {e}")
            return False
            
    def send_error_log(self, error_message: str, error_details: str = None, sensor_id: str = None):
        """Send error log to Kafka with dual broker redundancy."""
        current_time = time.time()
        
        if current_time - self.last_error_time < self.error_interval:
            return
            
        try:
            if self.kafka_pipeline is None:
                return
                
            log_message = {
                "timestamp": datetime.now().isoformat(),
                "level": "ERROR",
                "message": error_message,
                "sensor_id": sensor_id,
                "details": error_details,
                "rate_limited": True
            }
            
            log_topic = self.config.get("kafka_variables", {}).get("log_topic", "log_topic")
            
            try:
                self.kafka_pipeline.send(log_topic, log_message)
                self.last_error_time = current_time
            except (KafkaError, NoBrokersAvailable):
                self._handle_broker_failure()
                if self.kafka_pipeline:
                    try:
                        self.kafka_pipeline.send(log_topic, log_message)
                        self.last_error_time = current_time
                    except:
                        pass
                        
        except Exception as e:
            print(f"DEBUG: Kafka error logging failed: {e}")
            
    def run_kafka_loop(self, events_queue: queue.Queue, analytics_queue: queue.Queue):
        """Main Kafka processing loop with dual broker and S3 redundancy."""
        kafka_config = self.config.get("kafka_variables", {})
        send_events_pipeline = kafka_config.get("send_events_pipeline")
        send_analytics_pipeline = kafka_config.get("send_analytics_pipeline")
        
        queues_and_topics = [
            (events_queue, send_events_pipeline),
            (analytics_queue, send_analytics_pipeline)
        ]
        
        consecutive_empty_cycles = 0
        
        print(f"DEBUG: Starting Kafka loop with dual brokers: {self.brokers}")
        print(f"DEBUG: S3 buckets: {[config.get('BUCKET_NAME') for config in self.s3_configs.values()]}")
        
        while True:
            try:
                if self.kafka_pipeline is None:
                    self.kafka_pipeline = self._create_kafka_producer()
                    if self.kafka_pipeline is None:
                        print("DEBUG: No healthy brokers available, retrying in 10 seconds")
                        time.sleep(10)
                        continue
                    time.sleep(5)
                
                messages_processed = 0
                for queue_obj, topic in queues_and_topics:
                    if topic == send_events_pipeline:
                        if self.process_events_queue(queue_obj, topic):
                            messages_processed += 1
                    else:
                        if self.process_analytics_queue(queue_obj, topic):
                            messages_processed += 1
                
                if messages_processed == 0:
                    consecutive_empty_cycles += 1
                    sleep_time = min(2 * consecutive_empty_cycles, 10)
                    time.sleep(sleep_time)
                else:
                    consecutive_empty_cycles = 0
                    time.sleep(0.1)
                    
            except (KafkaError, NoBrokersAvailable) as e:
                print(f"DEBUG: Kafka connection error: {e}")
                self._handle_broker_failure()
                time.sleep(10)
            except Exception as e:
                print(f"DEBUG: Unexpected error in Kafka loop: {e}")
                time.sleep(10) 
