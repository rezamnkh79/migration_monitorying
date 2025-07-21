import json
import logging
import requests
import threading
import time
import os
from datetime import datetime
from typing import Dict, List, Set, Any, Optional
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic

from database.mysql_client import MySQLClient
from database.postgres_client import PostgreSQLClient

logger = logging.getLogger(__name__)

class DynamicTableMonitor:
    """
    Advanced Dynamic Table Monitor that:
    1. Continuously monitors MySQL for new tables
    2. Automatically discovers schema changes
    3. Updates CDC connectors dynamically
    4. Handles table creation/deletion in real-time
    5. No hardcoded table names or regex patterns
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
        self.database_name = self._get_database_name()
        
        # MySQL connection settings from environment
        self.mysql_host = os.getenv('MYSQL_HOST', 'mysql')
        self.mysql_port = os.getenv('MYSQL_PORT', '3306')
        self.mysql_user = os.getenv('MYSQL_USER', 'debezium')
        self.mysql_password = os.getenv('MYSQL_PASSWORD', 'dbz')
        
        logger.info(f"🔧 Table Monitor initialized for MySQL: {self.mysql_host}:{self.mysql_port}/{self.database_name}")
        
        # Dynamic monitoring state
        self.current_tables: Set[str] = set()
        self.table_schemas: Dict[str, List[str]] = {}
        self.consumer = None
        self.consumer_thread = None
        self.monitor_thread = None
        self.running = False
        self.monitoring_tables = False
        
        # Discovery settings
        self.discovery_interval = 30  # Check for new tables every 30 seconds
        self.excluded_patterns = [
            'information_schema', 'performance_schema', 'mysql', 'sys',
            'migration_log', 'schema_migrations', 'flyway_schema_history'
        ]
        
        logger.info("Dynamic Table Monitor initialized")
        
    def _get_database_name(self):
        """Get current database name dynamically"""
        try:
            if self.mysql:
                result = self.mysql.execute_query("SELECT DATABASE()")
                if result and len(result) > 0:
                    db_name = result[0].get('DATABASE()')
                    if db_name:
                        logger.info(f"Detected database: {db_name}")
                        return db_name
            
            # Fallback to environment variable
            return os.getenv('MYSQL_DATABASE', 'inventory')
        except Exception as e:
            logger.warning(f"Could not detect database name: {e}")
            return 'inventory'
    
    def start_monitoring(self):
        """Start comprehensive table monitoring"""
        try:
            logger.info("Starting Dynamic Table Monitoring System...")
            
            # Initial table discovery
            self._discover_current_tables()
            
            # Start table discovery monitoring
            self.monitoring_tables = True
            self.monitor_thread = threading.Thread(target=self._table_discovery_loop, daemon=True)
            self.monitor_thread.start()
            
            # Setup initial CDC connectors
            self._setup_initial_connectors()
            
            # Start CDC event monitoring
            self._start_cdc_monitoring()
            
            logger.info("Dynamic Table Monitoring System started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start table monitoring: {str(e)}")
            raise
    
    def _discover_current_tables(self):
        """Discover all current tables in MySQL"""
        try:
            logger.info("Discovering current MySQL tables...")
            
            mysql_tables = self.mysql.get_table_list() if self.mysql else []
            
            # Filter out system tables and excluded patterns
            discovered_tables = set()
            for table in mysql_tables:
                if self._should_monitor_table(table):
                    discovered_tables.add(table)
                    # Get table schema for monitoring
                    self.table_schemas[table] = self._get_table_columns(table)
            
            # Update current tables
            old_tables = self.current_tables.copy()
            self.current_tables = discovered_tables
            
            # Detect changes
            new_tables = self.current_tables - old_tables
            removed_tables = old_tables - self.current_tables
            
            if new_tables:
                logger.info(f"New tables discovered: {list(new_tables)}")
            if removed_tables:
                logger.info(f"Tables removed: {list(removed_tables)}")
            
            # Update global stats
            self.global_stats["monitored_tables"] = list(self.current_tables)
            self.global_stats["total_tables"] = len(self.current_tables)
            
            logger.info(f"Currently monitoring {len(self.current_tables)} tables: {list(self.current_tables)}")
            
            return new_tables, removed_tables
            
        except Exception as e:
            logger.error(f"Table discovery failed: {str(e)}")
            return set(), set()
    
    def _should_monitor_table(self, table_name: str) -> bool:
        """Determine if a table should be monitored"""
        # Check against excluded patterns
        table_lower = table_name.lower()
        for pattern in self.excluded_patterns:
            if pattern in table_lower:
                return False
        
        # Exclude temporary tables
        if table_name.startswith('_') or table_name.startswith('tmp_'):
            return False
        
        # Exclude backup tables
        if '_backup' in table_lower or '_bak' in table_lower:
            return False
        
        return True
    
    def _get_table_columns(self, table_name: str) -> List[str]:
        """Get column names for a table"""
        try:
            schema = self.mysql.get_table_schema(table_name)
            return [col.get('Field', '') for col in schema]
        except Exception as e:
            logger.warning(f"Could not get schema for {table_name}: {e}")
            return []
    
    def _table_discovery_loop(self):
        """Continuous loop to monitor for table changes"""
        logger.info("Table discovery monitoring started")
        
        while self.monitoring_tables:
            try:
                # Wait before next check
                time.sleep(self.discovery_interval)
                
                # Discover tables
                new_tables, removed_tables = self._discover_current_tables()
                
                # Handle new tables
                if new_tables:
                    logger.info(f"Handling {len(new_tables)} new tables...")
                    self._handle_new_tables(new_tables)
                
                # Handle removed tables
                if removed_tables:
                    logger.info(f"Handling {len(removed_tables)} removed tables...")
                    self._handle_removed_tables(removed_tables)
                
                # Check for schema changes in existing tables
                self._check_schema_changes()
                
            except Exception as e:
                logger.error(f"Error in table discovery loop: {str(e)}")
                time.sleep(10)  # Wait longer on error
        
        logger.info("Table discovery monitoring stopped")
    
    def _handle_new_tables(self, new_tables: Set[str]):
        """Handle newly discovered tables"""
        try:
            # Update connector to include new tables
            self._update_connector_with_new_tables(new_tables)
            
            # Initialize sync status for new tables
            for table in new_tables:
                self._initialize_table_sync_status(table)
            
            # Update Kafka consumer to subscribe to new topics
            self._update_kafka_subscription()
            
            logger.info(f"Successfully added {len(new_tables)} new tables to monitoring")
            
        except Exception as e:
            logger.error(f"Failed to handle new tables: {str(e)}")
    
    def _handle_removed_tables(self, removed_tables: Set[str]):
        """Handle tables that were removed"""
        try:
            # Remove from sync status
            for table in removed_tables:
                if table in self.global_stats.get("table_sync_status", {}):
                    del self.global_stats["table_sync_status"][table]
                
                # Remove schema info
                if table in self.table_schemas:
                    del self.table_schemas[table]
            
            # Update connector configuration
            self._update_connector_configuration()
            
            logger.info(f"Successfully removed {len(removed_tables)} tables from monitoring")
            
        except Exception as e:
            logger.error(f"Failed to handle removed tables: {str(e)}")
    
    def _check_schema_changes(self):
        """Check for schema changes in existing tables"""
        try:
            schema_changes = []
            
            for table in self.current_tables:
                current_schema = self._get_table_columns(table)
                old_schema = self.table_schemas.get(table, [])
                
                if current_schema != old_schema:
                    schema_changes.append({
                        'table': table,
                        'old_columns': old_schema,
                        'new_columns': current_schema
                    })
                    
                    # Update stored schema
                    self.table_schemas[table] = current_schema
                    
                    logger.info(f"Schema change detected in {table}")
            
            if schema_changes:
                self._handle_schema_changes(schema_changes)
            
        except Exception as e:
            logger.error(f"Schema change detection failed: {str(e)}")
    
    def _handle_schema_changes(self, schema_changes: List[Dict]):
        """Handle detected schema changes"""
        try:
            for change in schema_changes:
                table = change['table']
                logger.info(f"Handling schema change for {table}")
                
                # Could trigger connector restart if needed
                # For now, just log the change
                logger.info(f"Schema updated for {table}: {len(change['new_columns'])} columns")
            
        except Exception as e:
            logger.error(f"Failed to handle schema changes: {str(e)}")
    
    def _setup_initial_connectors(self):
        """Setup initial CDC connectors with current tables"""
        try:
            if not self.current_tables:
                logger.warning("No tables to monitor, skipping connector setup")
                return False
            
            logger.info("Setting up dynamic CDC connectors...")
            
            # Wait for Kafka Connect
            self._wait_for_kafka_connect()
            
            # Clean up existing connectors
            self._cleanup_all_connectors()
            
            # Create new dynamic connector
            success = self._create_dynamic_connector()
            
            if success:
                time.sleep(10)  # Wait for connector to initialize
                self._verify_connector_status()
                logger.info("Dynamic CDC connectors setup completed")
                return True
            else:
                logger.error("Failed to setup dynamic connectors")
                return False
                
        except Exception as e:
            logger.error(f"Connector setup failed: {str(e)}")
            return False
    
    def _wait_for_kafka_connect(self, max_retries=30):
        """Wait for Kafka Connect to be ready"""
        logger.info("Waiting for Kafka Connect...")
        
        for attempt in range(max_retries):
            try:
                response = requests.get(f"{self.connect_url}/connectors", timeout=5)
                if response.status_code == 200:
                    logger.info("Kafka Connect is ready")
                    return True
            except Exception:
                pass
            
            time.sleep(2)
            if attempt % 10 == 0:  # Log every 20 seconds
                logger.info(f"Still waiting for Kafka Connect... ({attempt + 1}/{max_retries})")
        
        raise Exception("Kafka Connect not available after waiting")
    
    def _cleanup_all_connectors(self):
        """Remove all existing connectors"""
        try:
            response = requests.get(f"{self.connect_url}/connectors", timeout=10)
            if response.status_code == 200:
                connectors = response.json()
                
                for connector in connectors:
                    logger.info(f"Removing connector: {connector}")
                    delete_response = requests.delete(f"{self.connect_url}/connectors/{connector}", timeout=10)
                    if delete_response.status_code in [204, 404]:
                        logger.info(f"Removed: {connector}")
                    else:
                        logger.warning(f"Failed to remove {connector}: {delete_response.status_code}")
                        
        except Exception as e:
            logger.warning(f"Error cleaning connectors: {str(e)}")
    
    def _create_dynamic_connector(self):
        """Create a single dynamic MySQL connector for all current tables"""
        try:
            table_include_list = [f"{self.database_name}.{table}" for table in self.current_tables]
            
            # Generate unique server ID based on host
            import hashlib
            server_id = str(abs(hash(self.mysql_host)) % 10000000)
            
            connector_config = {
                "name": "dynamic-mysql-source",
                "config": {
                    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                    "tasks.max": "1",
                    "database.hostname": self.mysql_host,
                    "database.port": self.mysql_port,
                    "database.user": self.mysql_user,
                    "database.password": self.mysql_password,
                    "database.server.id": server_id,
                    "database.server.name": f"dynamic_mysql_{server_id}",
                    "database.include.list": self.database_name,
                    "table.include.list": ",".join(table_include_list),
                    "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
                    "schema.history.internal.kafka.topic": f"schema-changes.dynamic.{server_id}",
                    "include.schema.changes": "true",
                    "topic.prefix": "dynamic",
                    "snapshot.mode": "initial",
                    "transforms": "route",
                    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
                    "transforms.route.regex": "([^.]+)\\.([^.]+)\\.([^.]+)",
                    "transforms.route.replacement": "$3",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter.schemas.enable": "false",
                    "decimal.handling.mode": "string",
                    "time.precision.mode": "connect",
                    "bigint.unsigned.handling.mode": "long"
                }
            }
            
            logger.info(f"📡 Creating MySQL connector for {self.mysql_host}:{self.mysql_port}/{self.database_name}")
            logger.info(f"📊 Tables to monitor: {table_include_list}")
            
            response = requests.post(
                f"{self.connect_url}/connectors",
                headers={"Content-Type": "application/json"},
                json=connector_config,
                timeout=30
            )
            
            if response.status_code in [200, 201]:
                logger.info("✅ Dynamic MySQL source connector created successfully")
                self.global_stats["connector_status"]["mysql"] = "running"
                return True
            else:
                logger.error(f"❌ Failed to create MySQL connector: {response.status_code} - {response.text}")
                self.global_stats["connector_status"]["mysql"] = "failed"
                return False
                
        except Exception as e:
            logger.error(f"❌ Error creating MySQL connector: {str(e)}")
            self.global_stats["connector_status"]["mysql"] = "error"
            return False
    
    def _update_connector_with_new_tables(self, new_tables: Set[str]):
        """Update existing connector to include new tables"""
        try:
            # For now, recreate the connector with all tables
            # In production, you might want to use connector reconfiguration API
            logger.info(f"Updating connector to include {len(new_tables)} new tables...")
            
            self._cleanup_all_connectors()
            time.sleep(5)
            self._create_dynamic_connector()
            time.sleep(10)
            self._verify_connector_status()
            
        except Exception as e:
            logger.error(f"Failed to update connector: {str(e)}")
    
    def _update_connector_configuration(self):
        """Update connector configuration after table changes"""
        try:
            if self.current_tables:
                self._update_connector_with_new_tables(set())
            else:
                # No tables left, remove connector
                self._cleanup_all_connectors()
                
        except Exception as e:
            logger.error(f"Failed to update connector configuration: {str(e)}")
    
    def _verify_connector_status(self):
        """Verify connector status"""
        try:
            response = requests.get(f"{self.connect_url}/connectors/dynamic-mysql-source/status", timeout=10)
            if response.status_code == 200:
                status = response.json()
                connector_state = status.get("connector", {}).get("state", "unknown")
                
                logger.info(f"Connector Status: {connector_state}")
                
                # Update global stats
                self.global_stats["connector_status"]["mysql"] = connector_state
                
                tasks = status.get("tasks", [])
                for i, task in enumerate(tasks):
                    task_state = task.get("state", "unknown")
                    logger.info(f"Task {i}: {task_state}")
                    
                    if task_state == "FAILED":
                        logger.error(f"Task {i} failed: {task.get('trace', 'No trace')}")
                
                return connector_state == "RUNNING"
            
        except Exception as e:
            logger.error(f"Error verifying connector: {str(e)}")
            return False
    
    def _start_cdc_monitoring(self):
        """Start CDC event monitoring with dynamic topic subscription"""
        try:
            if self.running:
                logger.warning("CDC monitoring already running")
                return
            
            logger.info("Starting dynamic CDC event monitoring...")
            
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                key_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='dynamic-table-monitor',
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                consumer_timeout_ms=1000
            )
            
            # Subscribe to current table topics
            self._update_kafka_subscription()
            
            # Start consumer thread
            self.running = True
            self.consumer_thread = threading.Thread(target=self._consume_cdc_events, daemon=True)
            self.consumer_thread.start()
            
            logger.info("Dynamic CDC monitoring started")
            
        except Exception as e:
            logger.error(f"Failed to start CDC monitoring: {str(e)}")
            self.running = False
    
    def _update_kafka_subscription(self):
        """Update Kafka consumer subscription with current tables"""
        try:
            if self.consumer and self.current_tables:
                topics = list(self.current_tables)
                logger.info(f"Updating Kafka subscription to {len(topics)} topics: {topics}")
                self.consumer.subscribe(topics)
            
        except Exception as e:
            logger.error(f"Failed to update Kafka subscription: {str(e)}")
    
    def _consume_cdc_events(self):
        """Consume CDC events from dynamically subscribed topics"""
        logger.info("Dynamic CDC event consumer started")
        
        message_count = 0
        last_log_time = time.time()
        
        while self.running:
            try:
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                if message_batch:
                    for topic_partition, messages in message_batch.items():
                        table_name = topic_partition.topic
                        
                        for message in messages:
                            message_count += 1
                            self._process_cdc_event(message, table_name)
                
                # Heartbeat log
                current_time = time.time()
                if current_time - last_log_time > 60:
                    logger.info(f"CDC Monitor: {message_count} events processed, monitoring {len(self.current_tables)} tables")
                    last_log_time = current_time
                    
            except Exception as e:
                logger.error(f"Error consuming CDC events: {str(e)}")
                time.sleep(5)
        
        logger.info("Dynamic CDC event consumer stopped")
    
    def _process_cdc_event(self, message, table_name):
        """Process a CDC event from any monitored table"""
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
                
                # Store event
                self._store_cdc_event(cdc_event, table_name, operation)
                
                # Update table sync status
                self._update_table_sync_status(table_name, operation)
                
                logger.info(f"CDC Event: {operation} on {table_name} (Total: {self.global_stats['cdc_events_processed']})")
                
        except Exception as e:
            logger.error(f"Failed to process CDC event: {str(e)}")
    
    def _extract_operation(self, cdc_event):
        """Extract operation type from CDC event"""
        try:
            if 'op' in cdc_event:
                op_map = {'c': 'insert', 'u': 'update', 'd': 'delete', 'r': 'read'}
                return op_map.get(cdc_event['op'], 'unknown')
            return None
        except Exception as e:
            logger.error(f"Failed to extract operation: {str(e)}")
            return None
    
    def _store_cdc_event(self, cdc_event, table_name, operation):
        """Store CDC event in Redis"""
        try:
            if self.redis:
                event_data = {
                    "table": table_name,
                    "operation": operation,
                    "timestamp": datetime.now().isoformat(),
                    "event_data": cdc_event
                }
                
                self.redis.lpush("cdc_events", json.dumps(event_data))
                self.redis.ltrim("cdc_events", 0, 999)
                
                # Update stats
                today = datetime.now().strftime('%Y%m%d')
                self.redis.hincrby(f"stats:{table_name}:{today}", operation, 1)
                self.redis.hincrby(f"stats:total:{today}", operation, 1)
                self.redis.set(f"last_sync:{table_name}", datetime.now().isoformat())
                
        except Exception as e:
            logger.error(f"Failed to store CDC event: {str(e)}")
    
    def _update_table_sync_status(self, table_name, operation):
        """Update sync status for a table"""
        try:
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
            logger.error(f"Failed to update sync status: {str(e)}")
    
    def _get_table_count(self, db_type, table_name):
        """Get table count from database"""
        try:
            if db_type == 'mysql' and self.mysql:
                return self.mysql.get_table_count(table_name)
            elif db_type == 'postgres' and self.postgres:
                return self.postgres.get_table_count(table_name)
            return 0
        except Exception:
            return 0
    
    def _initialize_table_sync_status(self, table_name):
        """Initialize sync status for a new table"""
        try:
            mysql_count = self._get_table_count('mysql', table_name)
            postgres_count = self._get_table_count('postgres', table_name)
            
            self.global_stats["table_sync_status"][table_name] = {
                "mysql_count": mysql_count,
                "postgres_count": postgres_count,
                "last_sync": datetime.now().isoformat(),
                "sync_percentage": (postgres_count / mysql_count * 100) if mysql_count > 0 else 0
            }
            
            logger.info(f"Initialized sync status for new table: {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize sync status for {table_name}: {str(e)}")
    
    def get_monitoring_status(self):
        """Get comprehensive monitoring status"""
        try:
            return {
                "monitoring_active": self.monitoring_tables,
                "cdc_active": self.running,
                "current_tables": list(self.current_tables),
                "total_tables": len(self.current_tables),
                "table_schemas": {table: len(cols) for table, cols in self.table_schemas.items()},
                "discovery_interval": self.discovery_interval,
                "database_name": self.database_name,
                "cdc_events_processed": self.global_stats.get("cdc_events_processed", 0),
                "last_cdc_event": self.global_stats.get("last_cdc_event"),
                "connector_status": self.global_stats.get("connector_status", {}),
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to get monitoring status: {str(e)}")
            return {}
    
    def add_table_manually(self, table_name: str):
        """Manually add a table to monitoring"""
        try:
            if self._should_monitor_table(table_name):
                if table_name not in self.current_tables:
                    self.current_tables.add(table_name)
                    self.table_schemas[table_name] = self._get_table_columns(table_name)
                    self._handle_new_tables({table_name})
                    logger.info(f"Manually added table to monitoring: {table_name}")
                    return True
                else:
                    logger.warning(f"Table already being monitored: {table_name}")
                    return False
            else:
                logger.warning(f"Table excluded from monitoring: {table_name}")
                return False
        except Exception as e:
            logger.error(f"Failed to manually add table {table_name}: {str(e)}")
            return False
    
    def stop_monitoring(self):
        """Stop all monitoring activities"""
        try:
            logger.info("Stopping Dynamic Table Monitor...")
            
            # Stop table discovery
            self.monitoring_tables = False
            
            # Stop CDC monitoring
            self.running = False
            
            # Close consumer
            if self.consumer:
                self.consumer.close()
            
            # Wait for threads
            if self.consumer_thread and self.consumer_thread.is_alive():
                self.consumer_thread.join(timeout=10)
            
            if self.monitor_thread and self.monitor_thread.is_alive():
                self.monitor_thread.join(timeout=10)
            
            logger.info("Dynamic Table Monitor stopped")
            
        except Exception as e:
            logger.error(f"Error stopping monitor: {str(e)}") 