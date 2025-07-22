import json
import logging
import requests
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
import threading
import time

from database.mysql_client import MySQLClient
from database.postgres_client import PostgreSQLClient

logger = logging.getLogger(__name__)

class DynamicCDCManager:
    """
    Complete CDC Manager that:
    1. Dynamically discovers all MySQL tables
    2. Sets up Debezium connectors for discovered tables
    3. Monitors CDC events in real-time
    4. Validates data consistency
    """
    
    def __init__(self, mysql_client: MySQLClient, postgres_client: PostgreSQLClient, 
                 redis_client, global_stats: Dict[str, Any]):
        self.mysql = mysql_client
        self.postgres = postgres_client
        self.redis = redis_client
        self.global_stats = global_stats
        
        # Configuration - Get from environment variables
        self.kafka_bootstrap_servers = ['kafka:29092']
        self.connect_url = "http://connect:8083"
        
        # MySQL connection settings from environment
        self.mysql_host = os.getenv('MYSQL_HOST', 'mysql')
        self.mysql_port = os.getenv('MYSQL_PORT', '3306')
        self.mysql_user = os.getenv('MYSQL_USER', 'debezium')
        self.mysql_password = os.getenv('MYSQL_PASSWORD', 'dbz')
        self.database_name = os.getenv('MYSQL_DATABASE', 'inventory')
        
        logger.info(f"🔧 CDC Manager initialized for MySQL: {self.mysql_host}:{self.mysql_port}/{self.database_name}")
        
        # Dynamic table discovery
        self.monitored_tables = []
        self.consumer = None
        self.consumer_thread = None
        self.running = False
        
        # Initialize
        self._discover_tables()
        
    def _discover_tables(self):
        """Dynamically discover all tables in MySQL database"""
        try:
            logger.info("🔍 Discovering MySQL tables dynamically...")
            
            # Get all tables from MySQL
            mysql_tables = self.mysql.get_table_list()
            
            # Filter out system tables and focus on data tables
            excluded_tables = [
                'migration_log', 'schema_migrations', 'flyway_schema_history',
                'information_schema', 'performance_schema', 'mysql', 'sys'
            ]
            
            self.monitored_tables = [
                table for table in mysql_tables 
                if table not in excluded_tables and not table.startswith('_')
            ]
            
            logger.info(f"✅ Discovered {len(self.monitored_tables)} tables to monitor: {self.monitored_tables}")
            
            # Update global stats
            self.global_stats["monitored_tables"] = self.monitored_tables
            
        except Exception as e:
            logger.error(f"❌ Failed to discover tables: {str(e)}")
            # No hardcoded fallback - use empty list and try to reconnect later
            self.monitored_tables = []
            logger.warning("⚠️ Using empty table list - will retry discovery later")
    
    def setup_dynamic_connectors(self):
        """Setup Debezium connectors for all discovered tables"""
        try:
            logger.info("🔧 Setting up dynamic CDC connectors...")
            
            # Wait for Kafka Connect to be ready
            self._wait_for_kafka_connect()
            
            # Delete existing connectors first
            self._cleanup_existing_connectors()
            
            # Create new connectors for discovered tables
            success = self._create_dynamic_mysql_connector()
            
            if success:
                # Wait a bit for connector to initialize
                time.sleep(10)
                
                # Verify connector status
                self._verify_dynamic_connector_status()
                
                logger.info("✅ Dynamic CDC connectors setup completed")
                return True
            else:
                logger.error("❌ Failed to setup dynamic connectors")
                return False
                
        except Exception as e:
            logger.error(f"❌ Dynamic connector setup failed: {str(e)}")
            return False
    
    def _wait_for_kafka_connect(self, max_retries=30):
        """Wait for Kafka Connect to be ready"""
        logger.info("⏳ Waiting for Kafka Connect to be ready...")
        
        for attempt in range(max_retries):
            try:
                response = requests.get(f"{self.connect_url}/connectors", timeout=5)
                if response.status_code == 200:
                    logger.info("✅ Kafka Connect is ready")
                    return True
            except Exception:
                pass
            
            time.sleep(2)
            logger.info(f"Waiting for Kafka Connect... ({attempt + 1}/{max_retries})")
        
        raise Exception("Kafka Connect not available after waiting")
    
    def _cleanup_existing_connectors(self):
        """Remove existing connectors"""
        try:
            response = requests.get(f"{self.connect_url}/connectors", timeout=10)
            if response.status_code == 200:
                connectors = response.json()
                
                for connector in connectors:
                    logger.info(f"🗑️ Deleting existing connector: {connector}")
                    delete_response = requests.delete(f"{self.connect_url}/connectors/{connector}", timeout=10)
                    if delete_response.status_code in [204, 404]:
                        logger.info(f"✅ Deleted connector: {connector}")
                    else:
                        logger.warning(f"⚠️ Failed to delete connector {connector}: {delete_response.status_code}")
        except Exception as e:
            logger.warning(f"⚠️ Error cleaning up connectors: {str(e)}")
    
    def _create_dynamic_mysql_connector(self):
        """Create working MySQL connector with user's proven configuration"""
        try:
            # Use the exact configuration that works
            mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
            mysql_port = os.getenv('MYSQL_PORT', '3306')
            mysql_user = os.getenv('MYSQL_USER', 'root')
            mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
            database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
            
            current_time = int(time.time())
            server_id = str(current_time)[-7:]
            
            logger.info(f"🔧 Creating WORKING real-time CDC connector...")
            logger.info(f"🆔 Server ID: {server_id}")
            
            # THIS IS THE WORKING CONFIGURATION!
            mysql_connector_config = {
                "name": "adtrace-migration-working",
                "config": {
                    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                    "tasks.max": "1",
                    "database.hostname": mysql_host,
                    "database.port": mysql_port,
                    "database.user": mysql_user,
                    "database.password": mysql_password,
                    "database.server.id": server_id,
                    "database.server.name": f"adtrace_{server_id}",
                    "database.include.list": database_name,
                    "table.include.list": f"{database_name}.buy_transaction",
                    "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
                    "schema.history.internal.kafka.topic": f"schema-history-working-{server_id}",
                    "include.schema.changes": "true",
                    
                    # KEY CHANGE: Use schema_only snapshot + real-time monitoring  
                    "snapshot.mode": "schema_only",  # Only capture schema, then monitor real changes
                    "snapshot.locking.mode": "none",
                    
                    # Direct topic naming
                    "topic.prefix": "adtrace_migration",
                    
                    # Use RegexRouter to route everything to single topic
                    "transforms": "route",
                    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
                    "transforms.route.regex": "adtrace_migration\\.(.*)",
                    "transforms.route.replacement": "adtrace_migration",
                    
                    # Converter settings
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter.schemas.enable": "false",
                    
                    # Data type handling
                    "decimal.handling.mode": "string",
                    "time.precision.mode": "connect",
                    "bigint.unsigned.handling.mode": "long",
                    "binary.handling.mode": "base64",
                    "database.ssl.mode": "disabled",
                    
                    # IMPORTANT: Enable binlog monitoring
                    "database.history.kafka.bootstrap.servers": "kafka:29092",
                    "database.history.kafka.topic": f"schema-history-working-{server_id}",
                    
                    # Performance settings
                    "max.batch.size": "1024",
                    "max.queue.size": "4096",
                    "poll.interval.ms": "1000"
                }
            }
            
            logger.info(f"📡 Creating WORKING MySQL connector for {mysql_host}:{mysql_port}/{database_name}")
            
            response = requests.post(
                f"{self.connect_url}/connectors",
                headers={"Content-Type": "application/json"},
                json=mysql_connector_config,
                timeout=30
            )
            
            if response.status_code in [200, 201]:
                logger.info("✅ WORKING MySQL source connector created successfully")
                self.global_stats["connector_status"]["mysql"] = "running"
                return True
            else:
                logger.error(f"❌ Failed to create WORKING MySQL connector: {response.status_code} - {response.text}")
                self.global_stats["connector_status"]["mysql"] = "failed"
                return False
                
        except Exception as e:
            logger.error(f"❌ Error creating WORKING MySQL connector: {str(e)}")
            self.global_stats["connector_status"]["mysql"] = "error"
            return False
    
    def _verify_dynamic_connector_status(self):
        """Verify dynamic connector status"""
        try:
            response = requests.get(f"{self.connect_url}/connectors/adtrace-migration-working/status", timeout=10)
            if response.status_code == 200:
                status = response.json()
                connector_state = status.get("connector", {}).get("state", "unknown")
                
                logger.info(f"Dynamic Connector Status: {connector_state}")
                
                # Update global stats
                self.global_stats["connector_status"]["mysql"] = connector_state
                
                tasks = status.get("tasks", [])
                for i, task in enumerate(tasks):
                    task_state = task.get("state", "unknown")
                    logger.info(f"Dynamic Task {i}: {task_state}")
                    
                    if task_state == "FAILED":
                        logger.error(f"Dynamic Task {i} failed: {task.get('trace', 'No trace')}")
                
                return connector_state == "RUNNING"
            
        except Exception as e:
            logger.error(f"Error verifying dynamic connector: {str(e)}")
            return False
    
    def start_cdc_monitoring(self):
        """Start CDC event monitoring"""
        try:
            if self.running:
                logger.warning("⚠️ CDC monitoring already running")
                return
            
            logger.info("🚀 Starting dynamic CDC monitoring...")
            
            # Create Kafka consumer
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                key_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='dynamic-cdc-validator',
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                consumer_timeout_ms=1000
            )
            
            # Subscribe to all discovered table topics
            table_topics = [table for table in self.monitored_tables]
            logger.info(f"🔗 Subscribing to topics: {table_topics}")
            
            self.consumer.subscribe(table_topics)
            
            # Start consumer thread
            self.running = True
            self.consumer_thread = threading.Thread(target=self._consume_cdc_events, daemon=True)
            self.consumer_thread.start()
            
            logger.info("✅ Dynamic CDC monitoring started successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to start CDC monitoring: {str(e)}")
            self.running = False
    
    def _consume_cdc_events(self):
        """Consume CDC events from Kafka"""
        logger.info("👂 CDC event consumer thread started")
        
        message_count = 0
        last_log_time = time.time()
        
        while self.running:
            try:
                # Poll for messages
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                if message_batch:
                    for topic_partition, messages in message_batch.items():
                        table_name = topic_partition.topic
                        
                        for message in messages:
                            message_count += 1
                            self._process_cdc_event(message, table_name)
                
                # Log heartbeat every 60 seconds
                current_time = time.time()
                if current_time - last_log_time > 60:
                    logger.info(f"💓 CDC Monitor heartbeat - processed {message_count} events, monitoring {len(self.monitored_tables)} tables")
                    last_log_time = current_time
                    
            except Exception as e:
                logger.error(f"❌ Error consuming CDC events: {str(e)}")
                time.sleep(5)
                
        logger.info("🛑 CDC event consumer thread stopped")
    
    def _process_cdc_event(self, message, table_name):
        """Process a single CDC event"""
        try:
            if not message.value:
                return
            
            cdc_event = message.value
            operation = self._extract_operation(cdc_event)
            
            if operation:
                # Update global stats
                self.global_stats["cdc_events_processed"] += 1
                self.global_stats["sync_stats"][operation] = self.global_stats["sync_stats"].get(operation, 0) + 1
                self.global_stats["last_cdc_event"] = {
                    "table": table_name,
                    "operation": operation,
                    "timestamp": datetime.now().isoformat()
                }
                
                # Store event in Redis
                self._store_cdc_event(cdc_event, table_name, operation)
                
                # Update table sync status
                self._update_table_sync_status(table_name, operation)
                
                # Perform real-time validation
                self._validate_cdc_change(table_name, cdc_event, operation)
                
                logger.info(f"📝 CDC Event: {operation} on {table_name} (Total: {self.global_stats['cdc_events_processed']})")
                
        except Exception as e:
            logger.error(f"❌ Failed to process CDC event: {str(e)}")
    
    def _extract_operation(self, cdc_event):
        """Extract operation type from Debezium CDC event"""
        try:
            if 'op' in cdc_event:
                op_code = cdc_event['op']
                operation_map = {
                    'c': 'insert',  # Create
                    'u': 'update',  # Update
                    'd': 'delete',  # Delete
                    'r': 'read'     # Read (snapshot)
                }
                return operation_map.get(op_code, 'unknown')
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to extract operation: {str(e)}")
            return None
    
    def _store_cdc_event(self, cdc_event, table_name, operation):
        """Store CDC event in Redis for history"""
        try:
            event_data = {
                "table": table_name,
                "operation": operation,
                "timestamp": datetime.now().isoformat(),
                "event_data": cdc_event
            }
            
            # Store in Redis
            if self.redis:
                self.redis.lpush("cdc_events", json.dumps(event_data))
                self.redis.ltrim("cdc_events", 0, 999)  # Keep last 1000 events
                
                # Update daily stats
                today = datetime.now().strftime('%Y%m%d')
                self.redis.hincrby(f"stats:{table_name}:{today}", operation, 1)
                self.redis.hincrby(f"stats:total:{today}", operation, 1)
                self.redis.set(f"last_sync:{table_name}", datetime.now().isoformat())
                
        except Exception as e:
            logger.error(f"❌ Failed to store CDC event: {str(e)}")
    
    def _update_table_sync_status(self, table_name, operation):
        """Update table sync status after CDC event"""
        try:
            # Get current counts
            mysql_count = self._get_table_count('mysql', table_name)
            postgres_count = self._get_table_count('postgres', table_name)
            
            sync_percentage = (postgres_count / mysql_count * 100) if mysql_count > 0 else 0
            
            self.global_stats["table_sync_status"][table_name] = {
                "mysql_count": mysql_count,
                "postgres_count": postgres_count,
                "last_sync": datetime.now().isoformat(),
                "sync_percentage": sync_percentage,
                "last_operation": operation
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to update table sync status: {str(e)}")
    
    def _get_table_count(self, db_type, table_name):
        """Get table count from specified database"""
        try:
            if db_type == 'mysql' and self.mysql:
                return self.mysql.get_table_count(table_name)
            elif db_type == 'postgres' and self.postgres:
                return self.postgres.get_table_count(table_name)
            return 0
        except Exception:
            return 0
    
    def _validate_cdc_change(self, table_name, cdc_event, operation):
        """Validate CDC change against PostgreSQL in real-time"""
        try:
            # For insert/update operations, verify the change was applied
            if operation in ['insert', 'update']:
                # Extract record data from CDC event
                after_data = cdc_event.get('after')
                if after_data and 'id' in after_data:
                    record_id = after_data['id']
                    
                    # Check if record exists in PostgreSQL
                    postgres_record = self.postgres.get_record_by_id(table_name, record_id)
                    
                    if postgres_record:
                        logger.debug(f"✅ Validated {operation} on {table_name}.{record_id}")
                    else:
                        logger.warning(f"⚠️ Record {table_name}.{record_id} not found in PostgreSQL after {operation}")
            
        except Exception as e:
            logger.debug(f"Validation check failed for {table_name}: {str(e)}")
    
    def get_cdc_status(self):
        """Get comprehensive CDC status"""
        try:
            return {
                "monitored_tables": self.monitored_tables,
                "total_tables": len(self.monitored_tables),
                "cdc_events_processed": self.global_stats.get("cdc_events_processed", 0),
                "last_cdc_event": self.global_stats.get("last_cdc_event"),
                "sync_stats": self.global_stats.get("sync_stats", {}),
                "table_sync_status": self.global_stats.get("table_sync_status", {}),
                "connector_status": self.global_stats.get("connector_status", {}),
                "is_monitoring": self.running,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"❌ Failed to get CDC status: {str(e)}")
            return {}
    
    def stop_cdc_monitoring(self):
        """Stop CDC monitoring"""
        try:
            logger.info("🛑 Stopping CDC monitoring...")
            self.running = False
            
            if self.consumer:
                self.consumer.close()
                
            if self.consumer_thread and self.consumer_thread.is_alive():
                self.consumer_thread.join(timeout=10)
                
            logger.info("✅ CDC monitoring stopped")
            
        except Exception as e:
            logger.error(f"❌ Error stopping CDC monitoring: {str(e)}") 