#!/usr/bin/env python3
import requests
import json
import os
import time
import subprocess

def create_working_connector():
    """Create a connector that actually works for real-time CDC"""
    
    mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
    mysql_port = os.getenv('MYSQL_PORT', '3306')
    mysql_user = os.getenv('MYSQL_USER', 'root')
    mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
    database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
    
    current_time = int(time.time())
    server_id = str(current_time)[-7:]
    
    print(f"🔧 Creating WORKING real-time CDC connector...")
    print(f"🆔 Server ID: {server_id}")
    
    # Delete existing connector
    print("🗑️  Deleting existing connectors...")
    try:
        response = requests.get("http://localhost:8083/connectors")
        if response.status_code == 200:
            connectors = response.json()
            for connector in connectors:
                print(f"   Deleting {connector}...")
                requests.delete(f"http://localhost:8083/connectors/{connector}")
                time.sleep(1)
    except Exception as e:
        print(f"❌ Error deleting connectors: {str(e)}")
    
    time.sleep(5)
    
    # THIS TIME WE'LL DO IT RIGHT!
    connector_config = {
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
    
    try:
        response = requests.post(
            "http://localhost:8083/connectors",
            headers={"Content-Type": "application/json"},
            json=connector_config,
            timeout=30
        )
        
        if response.status_code in [200, 201]:
            print("✅ Working CDC connector created successfully")
            return True
        else:
            print(f"❌ Failed to create connector: {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"❌ Error creating connector: {str(e)}")
        return False

def wait_for_kafka_connect():
    """Wait for Kafka Connect to be ready"""
    print("⏳ Waiting for Kafka Connect...")
    for i in range(60):
        try:
            response = requests.get("http://localhost:8083/connectors", timeout=5)
            if response.status_code == 200:
                print("✅ Kafka Connect is ready")
                return True
        except:
            pass
        time.sleep(2)
        if i % 10 == 0:
            print(f"Still waiting for Kafka Connect... ({i+1}/60)")
    
    print("❌ Kafka Connect not ready after 2 minutes")
    return False

def main():
    print("🚀 Production CDC Connector Setup...")
    print("=" * 60)
    
    if wait_for_kafka_connect():
        if create_working_connector():
            print("🎉 SUCCESS! Production connector is ready!")
            print("✅ Connector: adtrace-migration-working")
            print("✅ Topic: adtrace_migration")
        else:
            print("❌ Failed to create production connector")
    else:
        print("❌ Kafka Connect not available")

if __name__ == "__main__":
    main() 