import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
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
        """Start consuming CDC events from adtrace_migration topic"""
        try:
            logger.info("🚀 Starting AdTrace Migration CDC Consumer...")
            
            # Use the working adtrace_migration topic
            topic_name = "adtrace_migration"
            
            self.consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                key_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                auto_offset_reset='latest',  # Start from latest messages
                enable_auto_commit=True,
                group_id='adtrace-migration-consumer',  # Updated group ID
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                consumer_timeout_ms=1000  # Timeout for polling
            )
            
            logger.info(f"✅ CDC Consumer connected to topic: {topic_name}")
            logger.info(f"📡 Bootstrap servers: {self.bootstrap_servers}")
            self.running = True
            
            # Start consuming messages
            message_count = 0
            last_log_time = datetime.now()
            
            for message in self.consumer:
                if not self.running:
                    break
                    
                try:
                    message_count += 1
                    
                    # Log progress every 10 messages or 30 seconds
                    current_time = datetime.now()
                    time_diff = (current_time - last_log_time).total_seconds()
                    
                    if message_count % 10 == 0 or time_diff >= 30:
                        logger.info(f"📨 Processed {message_count} CDC messages from {topic_name}")
                        last_log_time = current_time
                    
                    # Process the CDC message
                    self.process_cdc_message(message, topic_name)
                    
                except Exception as e:
                    logger.error(f"❌ Error processing message from {topic_name}: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"❌ Error in CDC consumer: {str(e)}")
            self.running = False
        finally:
            if self.consumer:
                self.consumer.close()
                logger.info("🔒 CDC Consumer closed")
    
    def process_cdc_message(self, message, topic):
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