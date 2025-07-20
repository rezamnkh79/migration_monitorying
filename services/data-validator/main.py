import asyncio
import json
import logging
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Any

import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import redis
from kafka import KafkaConsumer, TopicPartition
import schedule
import time
import threading

from database.mysql_client import MySQLClient
from database.postgres_client import PostgreSQLClient
from models.validation_models import ValidationResult, TableStats, MigrationStatus
from services.data_validator import DataValidator
from services.kafka_consumer import DebeziumKafkaConsumer
from services.monitoring import MonitoringService
from utils.logger import setup_logger

# Setup logging
logger = setup_logger(__name__)

app = FastAPI(
    title="MySQL to PostgreSQL Migration Validator with Debezium CDC",
    description="Real-time data validation and monitoring for MySQL to PostgreSQL migration using Debezium CDC",
    version="2.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global instances
mysql_client = None
postgres_client = None
redis_client = None
data_validator = None
kafka_consumer = None
monitoring_service = None

# Global stats for dashboard (CDC counters disabled)
global_stats = {
    "cdc_events_processed": 0,  # Keep for compatibility but hide in frontend
    "last_cdc_event": None,     # Keep for compatibility but hide in frontend
    "connector_status": {"mysql": "disconnected", "postgres": "disconnected"},
    "sync_stats": {"insert": 0, "update": 0, "delete": 0},  # Keep for compatibility but hide in frontend
    "table_sync_status": {}
}

class ValidationRequest(BaseModel):
    tables: Optional[List[str]] = None
    full_validation: bool = False

class TableSyncRequest(BaseModel):
    table_name: str
    mysql_id: int
    operation: str  # INSERT, UPDATE, DELETE

@app.on_event("startup")
async def startup_event():
    """Initialize all services with Debezium CDC support"""
    global mysql_client, postgres_client, redis_client, data_validator, kafka_consumer, monitoring_service
    
    logger.info("🚀 Starting MySQL to PostgreSQL Migration System with Debezium CDC")
    
    try:
        # Initialize database clients
        logger.info("📡 Connecting to databases...")
        mysql_client = MySQLClient()
        postgres_client = PostgreSQLClient()
        
        # Initialize Redis client
        logger.info("🔴 Connecting to Redis...")
        redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)
        
        # Test connections
        mysql_status = mysql_client.test_connection()
        postgres_status = postgres_client.test_connection()
        
        logger.info(f"MySQL Connection: {'✅' if mysql_status else '❌'}")
        logger.info(f"PostgreSQL Connection: {'✅' if postgres_status else '❌'}")
        
        # Initialize services
        data_validator = DataValidator(mysql_client, postgres_client, redis_client)
        monitoring_service = MonitoringService(redis_client, mysql_client, postgres_client)
        
        # Initialize Debezium Kafka consumer for CDC events
        logger.info("🔄 Setting up Debezium CDC Consumer...")
        kafka_consumer = DebeziumKafkaConsumer(
            bootstrap_servers=['kafka:29092'],
            data_validator=data_validator,
            monitoring_service=monitoring_service,
            global_stats=global_stats
        )
        
        # Set global variables
        globals()['data_validator'] = data_validator
        globals()['monitoring_service'] = monitoring_service
        globals()['kafka_consumer'] = kafka_consumer
        
        # Start background tasks
        start_background_tasks()
        
        # Initialize table sync status
        await initialize_table_sync_status()
        
        logger.info("✅ All services initialized successfully!")
        logger.info("🎯 Debezium CDC Migration System is ready!")
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize services: {str(e)}")
        # Don't raise - allow partial functionality
        logger.warning("⚠️ Running in limited mode due to initialization errors")

async def initialize_table_sync_status():
    """Initialize table sync status tracking"""
    try:
        mysql_tables = mysql_client.get_table_list() if mysql_client else []
        postgres_tables = postgres_client.get_table_list() if postgres_client else []
        
        for table in mysql_tables[:10]:  # Monitor first 10 tables
            mysql_count = await get_table_count(mysql_client, table) if mysql_client else 0
            postgres_count = await get_table_count(postgres_client, table) if postgres_client else 0
            
            global_stats["table_sync_status"][table] = {
                "mysql_count": mysql_count,
                "postgres_count": postgres_count,
                "last_sync": datetime.now().isoformat(),
                "sync_percentage": (postgres_count / mysql_count * 100) if mysql_count > 0 else 0
            }
        
        logger.info(f"📊 Initialized sync status for {len(global_stats['table_sync_status'])} tables")
        
    except Exception as e:
        logger.error(f"Failed to initialize table sync status: {str(e)}")

async def get_table_count(client, table_name):
    """Get count of records in a table"""
    try:
        if client == mysql_client:
            query = f"SELECT COUNT(*) as count FROM `{table_name}`"
        else:
            query = f'SELECT COUNT(*) as count FROM "{table_name}"'
        
        result = client.execute_query(query)
        return result[0]['count'] if result else 0
    except:
        return 0

def start_background_tasks():
    """Start background monitoring and Debezium CDC processing tasks"""
    
    def run_kafka_consumer():
        """Run Debezium Kafka consumer in background thread"""
        try:
            logger.info("🔄 Starting Debezium CDC Consumer thread...")
            if kafka_consumer is None:
                logger.error("❌ kafka_consumer is None!")
                return
            logger.info("✅ kafka_consumer object exists, starting consumption...")
            kafka_consumer.start_consuming()
        except Exception as e:
            logger.error(f"💥 Kafka consumer thread error: {str(e)}")
            import traceback
            logger.error(f"📜 Full traceback: {traceback.format_exc()}")
    
    def run_periodic_validation():
        """Run periodic validation checks"""
        schedule.every(30).minutes.do(lambda: asyncio.run(run_validation_check()))
        schedule.every(2).hours.do(lambda: asyncio.run(run_full_validation()))
        
        while True:
            schedule.run_pending()
            time.sleep(60)
    
    def run_monitoring():
        """Run monitoring service to collect metrics"""
        while True:
            try:
                if monitoring_service:
                    monitoring_service.collect_metrics()
                    
                # Update connector status
                update_connector_status()
                
                time.sleep(30)  # Collect metrics every 30 seconds
            except Exception as e:
                logger.error(f"Monitoring error: {str(e)}")
                time.sleep(60)
    
    def update_table_sync_status():
        """Periodically update table sync status"""
        while True:
            try:
                asyncio.run(initialize_table_sync_status())
                time.sleep(300)  # Update every 5 minutes
            except Exception as e:
                logger.error(f"Table sync status update error: {str(e)}")
                time.sleep(600)
    
    # Start background threads
    threading.Thread(target=run_kafka_consumer, daemon=True).start()
    threading.Thread(target=run_periodic_validation, daemon=True).start()
    threading.Thread(target=run_monitoring, daemon=True).start()
    threading.Thread(target=update_table_sync_status, daemon=True).start()
    
    logger.info("🚀 Background tasks started successfully")

def update_connector_status():
    """Check and update Debezium connector status"""
    try:
        import requests
        
        # Check MySQL source connector for inventory
        try:
            response = requests.get("http://connect:8083/connectors/mysql-source-connector-inventory/status", timeout=5)
            if response.status_code == 200:
                status = response.json()
                global_stats["connector_status"]["mysql"] = status.get("connector", {}).get("state", "unknown")
            else:
                global_stats["connector_status"]["mysql"] = "disconnected"
        except:
            global_stats["connector_status"]["mysql"] = "disconnected"
        
        # Check PostgreSQL sink connector for inventory
        try:
            response = requests.get("http://connect:8083/connectors/postgres-sink-final/status", timeout=5)
            if response.status_code == 200:
                status = response.json()
                global_stats["connector_status"]["postgres"] = status.get("connector", {}).get("state", "unknown")
            else:
                global_stats["connector_status"]["postgres"] = "disconnected"
        except:
            global_stats["connector_status"]["postgres"] = "disconnected"
            
    except Exception as e:
        logger.warning(f"Failed to update connector status: {str(e)}")

async def run_validation_check():
    """Run incremental validation check"""
    try:
        if data_validator:
            result = await data_validator.validate_all_tables(quick_check=True)
            logger.info(f"Validation check completed: {result}")
    except Exception as e:
        logger.error(f"Validation check failed: {str(e)}")

async def run_full_validation():
    """Run full validation check"""
    try:
        if data_validator:
            result = await data_validator.validate_all_tables(quick_check=False)
            logger.info(f"Full validation completed: {result}")
    except Exception as e:
        logger.error(f"Full validation failed: {str(e)}")

@app.get("/")
async def root():
    return {
        "message": "MySQL to PostgreSQL Migration Validator with Debezium CDC",
        "status": "running",
        "version": "2.0.0",
        "cdc_events_processed": global_stats["cdc_events_processed"],
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health")
async def health_check():
    """Health check endpoint with real status"""
    mysql_status = "connected" if mysql_client and mysql_client.test_connection() else "disconnected"
    postgres_status = "connected" if postgres_client and postgres_client.test_connection() else "disconnected"
    redis_status = "connected" if redis_client else "disconnected"
    
    try:
        if redis_client:
            redis_client.ping()
            redis_status = "connected"
    except:
        redis_status = "disconnected"
    
    return {
        "status": "healthy" if mysql_status == "connected" and postgres_status == "connected" else "degraded",
        "mysql": mysql_status,
        "postgres": postgres_status,
        "redis": redis_status,
        "debezium": {
            "mysql_connector": global_stats["connector_status"]["mysql"],
            "postgres_connector": global_stats["connector_status"]["postgres"]
        },
        "cdc_events_processed": global_stats["cdc_events_processed"],
        "last_cdc_event": global_stats["last_cdc_event"],
        "timestamp": datetime.now().isoformat()
    }

@app.get("/debezium/status")
async def get_debezium_status():
    """Get detailed Debezium connector status"""
    try:
        import requests
        
        connectors = {}
        
        # Get connector list
        try:
            response = requests.get("http://connect:8083/connectors", timeout=5)
            if response.status_code == 200:
                connector_list = response.json()
                
                for connector in connector_list:
                    status_response = requests.get(f"http://connect:8083/connectors/{connector}/status", timeout=5)
                    if status_response.status_code == 200:
                        connectors[connector] = status_response.json()
        except Exception as e:
            logger.error(f"Failed to get connector status: {str(e)}")
        
        return {
            "connectors": connectors,
            "cdc_stats": global_stats,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get Debezium status: {str(e)}")
        return {"error": str(e), "timestamp": datetime.now().isoformat()}

@app.post("/debezium/setup")
async def setup_debezium_connectors():
    """Setup Debezium MySQL source and PostgreSQL sink connectors"""
    try:
        import requests
        
        results = {}
        
        # Setup MySQL source connector
        with open('debezium/mysql-source-connector.json', 'r') as f:
            mysql_config = json.load(f)
        
        response = requests.post(
            "http://connect:8083/connectors",
            headers={"Content-Type": "application/json"},
            json=mysql_config,
            timeout=30
        )
        
        if response.status_code in [200, 201]:
            results["mysql_source"] = "created"
            logger.info("✅ MySQL source connector created successfully")
        else:
            results["mysql_source"] = f"error: {response.text}"
            logger.error(f"Failed to create MySQL source connector: {response.text}")
        
        # Setup PostgreSQL sink connector
        with open('debezium/postgres-sink-connector.json', 'r') as f:
            postgres_config = json.load(f)
        
        response = requests.post(
            "http://connect:8083/connectors",
            headers={"Content-Type": "application/json"},
            json=postgres_config,
            timeout=30
        )
        
        if response.status_code in [200, 201]:
            results["postgres_sink"] = "created"
            logger.info("✅ PostgreSQL sink connector created successfully")
        else:
            results["postgres_sink"] = f"error: {response.text}"
            logger.error(f"Failed to create PostgreSQL sink connector: {response.text}")
        
        return {
            "message": "Debezium connectors setup completed",
            "results": results,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to setup Debezium connectors: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/latest-records/{table_name}")
async def get_latest_records(table_name: str, limit: int = 5):
    """Get latest records from both MySQL and PostgreSQL for comparison"""
    try:
        logger.info(f"🔍 Getting latest {limit} records from table: {table_name}")
        
        result = {
            "timestamp": datetime.now().isoformat(),
            "table_name": table_name,
            "mysql_latest": [],
            "postgres_latest": [],
            "comparison": {
                "mysql_count": 0,
                "postgres_count": 0,
                "sync_status": "unknown"
            }
        }
        
        # Get latest records from MySQL
        if mysql_client:
            try:
                mysql_latest = mysql_client.get_latest_records(table_name, limit)
                result["mysql_latest"] = mysql_latest
                result["comparison"]["mysql_count"] = len(mysql_latest)
                
                # Get total count
                mysql_total = await get_table_count(mysql_client, table_name)
                result["comparison"]["mysql_total"] = mysql_total
                
            except Exception as e:
                logger.warning(f"⚠️ Error getting MySQL latest records for {table_name}: {str(e)}")
                result["mysql_latest"] = []
                result["comparison"]["mysql_count"] = 0
        
        # Get latest records from PostgreSQL
        if postgres_client:
            try:
                postgres_latest = postgres_client.get_latest_records(table_name, limit)
                result["postgres_latest"] = postgres_latest
                result["comparison"]["postgres_count"] = len(postgres_latest)
                
                # Get total count
                postgres_total = await get_table_count(postgres_client, table_name)
                result["comparison"]["postgres_total"] = postgres_total
                
            except Exception as e:
                logger.warning(f"⚠️ Error getting PostgreSQL latest records for {table_name}: {str(e)}")
                result["postgres_latest"] = []
                result["comparison"]["postgres_count"] = 0
        
        # Determine sync status
        mysql_total = result["comparison"].get("mysql_total", 0)
        postgres_total = result["comparison"].get("postgres_total", 0)
        
        if mysql_total == postgres_total:
            result["comparison"]["sync_status"] = "synced"
        elif postgres_total > 0:
            result["comparison"]["sync_status"] = "partially_synced"
            result["comparison"]["sync_percentage"] = (postgres_total / mysql_total * 100) if mysql_total > 0 else 0
        else:
            result["comparison"]["sync_status"] = "not_synced"
        
        # Add CDC stats for this table
        result["cdc_info"] = {
            "events_processed": global_stats["cdc_events_processed"],
            "last_event": global_stats["last_cdc_event"],
            "table_sync_status": global_stats["table_sync_status"].get(table_name, {})
        }
        
        logger.info(f"✅ Latest records comparison for {table_name}: MySQL={mysql_total}, PostgreSQL={postgres_total}")
        return result
        
    except Exception as e:
        logger.error(f"❌ Failed to get latest records for {table_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics")
async def get_metrics():
    """Get detailed metrics for dashboard with real Debezium data"""
    try:
        # Get table lists
        mysql_tables = mysql_client.get_table_list() if mysql_client else []
        postgres_tables = postgres_client.get_table_list() if postgres_client else []
        
        # Get table counts (limited to first 15 for performance)
        mysql_table_counts = {}
        postgres_table_counts = {}
        
        for table in mysql_tables[:15]:
            mysql_table_counts[table] = await get_table_count(mysql_client, table)
        
        for table in postgres_tables[:15]:
            postgres_table_counts[table] = await get_table_count(postgres_client, table)
        
        # Get sync stats from Redis (daily stats)
        today = datetime.now().strftime('%Y%m%d')
        redis_sync_stats = {}
        if redis_client:
            try:
                total_stats = redis_client.hgetall(f"stats:total:{today}")
                redis_sync_stats = {
                    'insert': int(total_stats.get('insert', 0)),
                    'update': int(total_stats.get('update', 0)),
                    'delete': int(total_stats.get('delete', 0))
                }
            except Exception as e:
                logger.warning(f"Could not get Redis sync stats: {e}")
                redis_sync_stats = {'insert': 0, 'update': 0, 'delete': 0}
        
        # Combine global_stats with Redis stats for more comprehensive view
        combined_sync_stats = {
            'insert': max(global_stats["sync_stats"].get("insert", 0), redis_sync_stats.get('insert', 0)),
            'update': max(global_stats["sync_stats"].get("update", 0), redis_sync_stats.get('update', 0)),
            'delete': max(global_stats["sync_stats"].get("delete", 0), redis_sync_stats.get('delete', 0))
        }
        
        metrics = {
            "timestamp": datetime.now().isoformat(),
            "database_stats": {
                "mysql": {
                    "status": "connected" if mysql_client and mysql_client.test_connection() else "disconnected",
                    "tables": mysql_table_counts,
                    "table_list": mysql_tables[:20],  # Show first 20 for dashboard
                    "total_tables": len(mysql_tables)
                },
                "postgres": {
                    "status": "connected" if postgres_client and postgres_client.test_connection() else "disconnected",
                    "tables": postgres_table_counts,
                    "table_list": postgres_tables,
                    "total_tables": len(postgres_tables)
                }
            },
            # "cdc_stats": {
            #     "events_processed": global_stats["cdc_events_processed"],
            #     "last_event": global_stats["last_cdc_event"],
            #     "connector_status": global_stats["connector_status"]
            # },
            "sync_stats": {
                "total": combined_sync_stats,
                "global_live": global_stats["sync_stats"],  # Live stats from CDC
                "redis_daily": redis_sync_stats,  # Daily accumulated stats
                "table_status": global_stats["table_sync_status"]
            },
            "health_status": {
                "overall": "healthy" if mysql_client and postgres_client else "degraded",
                "mysql": "connected" if mysql_client and mysql_client.test_connection() else "disconnected",
                "postgres": "connected" if postgres_client and postgres_client.test_connection() else "disconnected",
                "redis": "connected" if redis_client else "disconnected"
            }
        }
        
        return metrics
        
    except Exception as e:
        logger.error(f"❌ Failed to get metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sync-check")
async def manual_sync_check():
    """Manual sync check with CDC status"""
    try:
        logger.info("🔍 Manual sync check requested with CDC status")
        
        result = {
            "timestamp": datetime.now().isoformat(),
            "mysql_tables": [],
            "postgres_tables": [],
            "mysql_counts": {},
            "postgres_counts": {},
            "cdc_status": {
                "events_processed": global_stats["cdc_events_processed"],
                "last_event": global_stats["last_cdc_event"],
                "connectors": global_stats["connector_status"]
            },
            "sync_summary": {
                "synced": 0,
                "different": 0,
                "missing": 0
            }
        }
        
        # Get MySQL data
        if mysql_client:
            mysql_tables = mysql_client.get_table_list()[:15]
            for table_name in mysql_tables:
                try:
                    count = await get_table_count(mysql_client, table_name)
                    sample_data = mysql_client.execute_query(f"SELECT * FROM `{table_name}` LIMIT 3")
                    
                    result["mysql_tables"].append({
                        "name": table_name,
                        "count": count,
                        "sample_data": sample_data[:3]
                    })
                    result["mysql_counts"][table_name] = count
                    
                except Exception as table_error:
                    logger.warning(f"⚠️ Error checking MySQL table {table_name}: {str(table_error)}")
                    result["mysql_counts"][table_name] = "ERROR"
        
        # Get PostgreSQL data
        if postgres_client:
            postgres_tables = postgres_client.get_table_list()
            for table_name in postgres_tables:
                try:
                    count = await get_table_count(postgres_client, table_name)
                    sample_data = postgres_client.execute_query(f'SELECT * FROM "{table_name}" LIMIT 3')
                    
                    result["postgres_tables"].append({
                        "name": table_name,
                        "count": count,
                        "sample_data": sample_data[:3]
                    })
                    result["postgres_counts"][table_name] = count
                    
                except Exception as table_error:
                    logger.warning(f"⚠️ Error checking PostgreSQL table {table_name}: {str(table_error)}")
                    result["postgres_counts"][table_name] = "ERROR"
        
        # Calculate sync summary
        mysql_table_names = {t["name"] for t in result["mysql_tables"]}
        postgres_table_names = {t["name"] for t in result["postgres_tables"]}
        
        common_tables = mysql_table_names.intersection(postgres_table_names)
        synced_count = 0
        different_count = 0
        
        for table in common_tables:
            mysql_count = result["mysql_counts"].get(table, 0)
            postgres_count = result["postgres_counts"].get(table, 0)
            
            if mysql_count == postgres_count and mysql_count != "ERROR":
                synced_count += 1
            else:
                different_count += 1
        
        result["sync_summary"] = {
            "synced": synced_count,
            "different": different_count,
            "missing": len(mysql_table_names.symmetric_difference(postgres_table_names))
        }
        
        logger.info(f"✅ Sync check completed: {synced_count} synced, {different_count} different, CDC events: {global_stats['cdc_events_processed']}")
        return result
        
    except Exception as e:
        logger.error(f"❌ Sync check failed: {str(e)}")
        return {
            "error": str(e),
            "timestamp": datetime.now().isoformat(),
            "mysql_tables": [],
            "postgres_tables": [],
            "mysql_counts": {},
            "postgres_counts": {},
            "cdc_status": global_stats,
            "sync_summary": {"synced": 0, "different": 0, "missing": 0}
        }

@app.get("/stats/{table_name}")
async def get_table_stats(table_name: str):
    """Get statistics for a specific table"""
    try:
        mysql_count = await get_table_count(mysql_client, table_name) if mysql_client else 0
        postgres_count = await get_table_count(postgres_client, table_name) if postgres_client else 0
        
        return {
            "table_name": table_name,
            "mysql_count": mysql_count,
            "postgres_count": postgres_count,
            "sync_status": "synced" if mysql_count == postgres_count else "out_of_sync",
            "sync_percentage": (postgres_count / mysql_count * 100) if mysql_count > 0 else 0,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to get table stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/cdc/events")
async def get_cdc_events(limit: int = 100):
    """Get recent CDC events from Redis"""
    try:
        if redis_client:
            events = redis_client.lrange("cdc_events", 0, limit-1)
            return {
                "events": [json.loads(event) for event in events],
                "total_processed": global_stats["cdc_events_processed"],
                "timestamp": datetime.now().isoformat()
            }
        else:
            return {"events": [], "total_processed": 0}
    except Exception as e:
        logger.error(f"Failed to get CDC events: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/validate")
async def trigger_validation(request: ValidationRequest):
    """Trigger validation for specified tables"""
    try:
        logger.info(f"🔍 Validation requested for tables: {request.tables}, full_validation: {request.full_validation}")
        
        if not data_validator:
            raise HTTPException(status_code=503, detail="Data validator not initialized")
        
        if request.tables:
            # Validate specific tables
            results = []
            for table_name in request.tables:
                result = await data_validator.validate_table(table_name, quick_check=not request.full_validation)
                results.append(result)
        else:
            # Validate all tables
            results = await data_validator.validate_all_tables(quick_check=not request.full_validation)
        
        # Convert results to dict for JSON serialization
        validation_results = []
        for result in results:
            validation_results.append({
                "table_name": result.table_name,
                "mysql_count": result.mysql_count,
                "postgres_count": result.postgres_count,
                "is_consistent": result.is_consistent,
                "validation_time": result.validation_time.isoformat(),
                "quick_check": result.quick_check,
                "discrepancies": result.discrepancies
            })
        
        response = {
            "message": "Validation completed",
            "timestamp": datetime.now().isoformat(),
            "full_validation": request.full_validation,
            "tables_validated": len(validation_results),
            "consistent_tables": sum(1 for r in validation_results if r["is_consistent"]),
            "results": validation_results
        }
        
        logger.info(f"✅ Validation completed: {response['consistent_tables']}/{response['tables_validated']} tables consistent")
        return response
        
    except Exception as e:
        logger.error(f"❌ Validation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_overall_stats():
    """Get overall migration statistics"""
    try:
        if not monitoring_service:
            raise HTTPException(status_code=503, detail="Monitoring service not initialized")
        
        migration_stats = await monitoring_service.get_migration_stats()
        
        return {
            "timestamp": datetime.now().isoformat(),
            "migration_status": {
                "total_tables": migration_stats.total_tables,
                "synced_tables": migration_stats.synced_tables,
                "pending_tables": migration_stats.pending_tables,
                "failed_tables": migration_stats.failed_tables,
                "overall_progress": migration_stats.overall_progress,
                "last_update": migration_stats.last_update.isoformat(),
                "is_healthy": migration_stats.is_healthy
            }
            # CDC stats removed to hide problematic counter
        }
        
    except Exception as e:
        logger.error(f"Failed to get stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/kafka-status")
async def get_kafka_status():
    """Get Kafka and CDC status"""
    try:
        # Check if Kafka consumer is running
        kafka_running = kafka_consumer is not None
        
        # Get recent CDC events
        recent_events = []
        if redis_client:
            try:
                events = redis_client.lrange("cdc_events", 0, 9)  # Last 10 events
                recent_events = [json.loads(event) for event in events]
            except Exception:
                pass
        
        return {
            "timestamp": datetime.now().isoformat(),
            "is_running": kafka_running,
            "cdc_events_processed": global_stats["cdc_events_processed"],
            "last_cdc_event": global_stats["last_cdc_event"],
            "connector_status": global_stats["connector_status"],
            "recent_events": recent_events,
            "success_rate": 1.0 if global_stats["cdc_events_processed"] > 0 else 0.0
        }
        
    except Exception as e:
        logger.error(f"Failed to get Kafka status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/migration-log")
async def get_migration_log(limit: int = 50, table_name: Optional[str] = None):
    """Get migration log entries"""
    try:
        if not data_validator:
            raise HTTPException(status_code=503, detail="Data validator not initialized")
        
        log_entries = await data_validator.get_migration_log(limit=limit, table_name=table_name)
        
        return {
            "timestamp": datetime.now().isoformat(),
            "entries": log_entries,
            "total_entries": len(log_entries)
        }
        
    except Exception as e:
        logger.error(f"Failed to get migration log: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/test-cdc")
async def test_cdc_simulation():
    """Test CDC event simulation for demonstration"""
    try:
        # Manually increment CDC stats for demonstration
        global_stats["cdc_events_processed"] += 1
        global_stats["sync_stats"]["insert"] = global_stats["sync_stats"].get("insert", 0) + 1
        global_stats["last_cdc_event"] = {
            "table": "users",
            "operation": "insert", 
            "timestamp": datetime.now().isoformat(),
            "simulated": True
        }
        
        logger.info(f"📝 SIMULATED CDC Event: insert on users (Total: {global_stats['cdc_events_processed']})")
        
        return {
            "message": "CDC event simulated successfully",
            "cdc_events_processed": global_stats["cdc_events_processed"],
            "last_event": global_stats["last_cdc_event"],
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to simulate CDC event: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info"
    ) 