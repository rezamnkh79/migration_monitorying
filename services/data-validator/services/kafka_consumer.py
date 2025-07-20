import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from kafka import KafkaConsumer
import threading
import asyncio

logger = logging.getLogger(__name__)

class DebeziumKafkaConsumer:
    def __init__(self, bootstrap_servers, data_validator, monitoring_service, global_stats):
        self.bootstrap_servers = bootstrap_servers
        self.data_validator = data_validator
        self.monitoring_service = monitoring_service
        self.global_stats = global_stats
        self.consumer = None
        self.running = False
        
    def start_consuming(self):
        """Start consuming Debezium CDC events from Kafka"""
        try:
            logger.info("Initializing Kafka Consumer...")
            
            # Subscribe to inventory table topics (Debezium creates topics with table names)
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                key_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                auto_offset_reset='latest',  # Start from latest messages
                enable_auto_commit=True,
                group_id='inventory-migration-validator',
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                consumer_timeout_ms=1000  # Timeout for polling
            )
            
            # Subscribe to inventory table topics
            inventory_topics = ['users', 'products', 'orders', 'order_items']
            self.consumer.subscribe(inventory_topics)
            
            logger.info(f"Debezium CDC Consumer started, listening for inventory topics: {inventory_topics}")
            logger.info(f"📡 Bootstrap servers: {self.bootstrap_servers}")
            self.running = True
            
            # Start consuming messages
            message_count = 0
            while self.running:
                try:
                    # Poll for messages with timeout
                    message_batch = self.consumer.poll(timeout_ms=1000)
                    
                    if message_batch:
                        logger.info(f"Received {len(message_batch)} topic partitions with messages")
                    
                    for topic_partition, messages in message_batch.items():
                        logger.info(f"Processing {len(messages)} messages from {topic_partition.topic}")
                        for message in messages:
                            message_count += 1
                            logger.info(f"Processing message #{message_count} from topic {topic_partition.topic}")
                            # Process event synchronously to avoid async issues
                            self.process_cdc_event_sync(message, topic_partition.topic)
                    
                    # Log every 60 seconds that we're still listening
                    if message_count % 60 == 0:
                        logger.info(f"Still listening for CDC events... (processed: {message_count})")
                            
                except Exception as e:
                    logger.error(f"Error polling Kafka messages: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Failed to start Kafka consumer: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            
    def stop_consuming(self):
        """Stop consuming CDC events"""
        self.running = False
        if self.consumer:
            self.consumer.close()
            logger.info("Debezium CDC Consumer stopped")
    
    def process_cdc_event_sync(self, message, topic):
        """Process a single CDC event from Debezium (synchronous version)"""
        try:
            if not message.value:
                return
                
            cdc_event = message.value
            table_name = topic  # Topic name is the table name in our setup
            
            # Extract operation type from Debezium message
            operation = self.get_operation_type(cdc_event)
            
            if operation:
                # Update global stats
                self.global_stats["cdc_events_processed"] += 1
                self.global_stats["sync_stats"][operation] = self.global_stats["sync_stats"].get(operation, 0) + 1
                self.global_stats["last_cdc_event"] = {
                    "table": table_name,
                    "operation": operation,
                    "timestamp": datetime.now().isoformat()
                }
                
                # Store event in Redis for history (synchronous)
                self.store_cdc_event_sync(cdc_event, table_name, operation)
                
                # Update table sync status (synchronous)
                self.update_table_sync_status_sync(table_name, operation)
                
                logger.info(f"CDC Event: {operation} on {table_name} (Total: {self.global_stats['cdc_events_processed']})")
                
        except Exception as e:
            logger.error(f"Failed to process CDC event: {str(e)}")
    
    def get_operation_type(self, cdc_event):
        """Extract operation type from Debezium CDC event"""
        try:
            # Debezium uses 'op' field for operation type
            if 'op' in cdc_event:
                op_code = cdc_event['op']
                operation_map = {
                    'c': 'insert',  # Create
                    'u': 'update',  # Update  
                    'd': 'delete',  # Delete
                    'r': 'read'     # Read (snapshot)
                }
                return operation_map.get(op_code, 'unknown')
            
            # Alternative: check if before/after fields exist
            elif 'before' in cdc_event and 'after' in cdc_event:
                if cdc_event['before'] is None and cdc_event['after'] is not None:
                    return 'insert'
                elif cdc_event['before'] is not None and cdc_event['after'] is not None:
                    return 'update'
                elif cdc_event['before'] is not None and cdc_event['after'] is None:
                    return 'delete'
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to extract operation type: {str(e)}")
            return None
    
    def store_cdc_event_sync(self, cdc_event, table_name, operation):
        """Store CDC event in Redis for history tracking (synchronous)"""
        try:
            if self.data_validator and self.data_validator.redis_client:
                event_data = {
                    "table": table_name,
                    "operation": operation,
                    "timestamp": datetime.now().isoformat(),
                    "event_data": cdc_event
                }
                
                # Store in Redis list (keep last 1000 events)
                redis_client = self.data_validator.redis_client
                redis_client.lpush("cdc_events", json.dumps(event_data))
                redis_client.ltrim("cdc_events", 0, 999)  # Keep only last 1000 events
                
        except Exception as e:
            logger.error(f"Failed to store CDC event: {str(e)}")
    
    def update_table_sync_status_sync(self, table_name, operation):
        """Update table sync status after CDC event (synchronous)"""
        try:
            # Get current counts for the table
            if self.data_validator:
                mysql_count = self.get_table_count_sync('mysql', table_name)
                postgres_count = self.get_table_count_sync('postgres', table_name)
                
                sync_percentage = (postgres_count / mysql_count * 100) if mysql_count > 0 else 0
                
                self.global_stats["table_sync_status"][table_name] = {
                    "mysql_count": mysql_count,
                    "postgres_count": postgres_count,
                    "last_sync": datetime.now().isoformat(),
                    "sync_percentage": sync_percentage,
                    "last_operation": operation
                }
                
        except Exception as e:
            logger.error(f"Failed to update table sync status: {str(e)}")
    
    def get_table_count_sync(self, db_type, table_name):
        """Get count of records in a table (synchronous)"""
        try:
            if db_type == 'mysql' and self.data_validator.mysql_client:
                query = f"SELECT COUNT(*) as count FROM `{table_name}`"
                result = self.data_validator.mysql_client.execute_query(query)
                return result[0]['count'] if result else 0
                
            elif db_type == 'postgres' and self.data_validator.postgres_client:
                query = f'SELECT COUNT(*) as count FROM "{table_name}"'
                result = self.data_validator.postgres_client.execute_query(query)
                return result[0]['count'] if result else 0
                
            return 0
            
        except Exception as e:
            logger.error(f"Failed to get {db_type} table count for {table_name}: {str(e)}")
            return 0