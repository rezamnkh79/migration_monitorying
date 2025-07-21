#!/usr/bin/env python3
import requests
import json
import os
import time
import hashlib

def main():
    print("🔧 Fixing CDC Issues...")
    
    # Configuration from environment
    mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
    mysql_port = os.getenv('MYSQL_PORT', '3306')
    mysql_user = os.getenv('MYSQL_USER', 'root')
    mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
    database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
    
    # Generate unique server ID
    server_id = str(abs(hash(mysql_host)) % 10000000)
    
    print(f"📋 Configuration:")
    print(f"   MySQL Host: {mysql_host}")
    print(f"   MySQL Database: {database_name}")
    print(f"   Server ID: {server_id}")
    
    # Step 1: Delete any existing connector
    print("\n🗑️ Cleaning up existing connectors...")
    try:
        response = requests.delete("http://localhost:8083/connectors/dynamic-mysql-source", timeout=10)
        print(f"   Deleted old connector: {response.status_code}")
    except:
        print("   No existing connector found")
    
    time.sleep(2)
    
    # Step 2: Test key tables for CDC
    # Focus on tables that are most likely to change
    key_tables = ['buy_transaction', 'adtrace_tracker', 'user_message', 'email', 'sms']
    table_include_list = [f"{database_name}.{table}" for table in key_tables]
    
    # Step 3: Create new connector with proper configuration
    print(f"\n📡 Creating new connector for tables: {key_tables}")
    
    connector_config = {
        "name": "dynamic-mysql-source",
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "1",
            "database.hostname": mysql_host,
            "database.port": mysql_port,
            "database.user": mysql_user,
            "database.password": mysql_password,
            "database.server.id": server_id,
            "database.server.name": f"adtrace_mysql_{server_id}",
            "database.include.list": database_name,
            "table.include.list": ",".join(table_include_list),
            
            # Schema history configuration
            "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
            "schema.history.internal.kafka.topic": f"schema-history-{server_id}",
            "include.schema.changes": "true",
            
            # Topic configuration
            "topic.prefix": "adtrace",
            
            # Snapshot configuration - use schema_only for existing data
            "snapshot.mode": "schema_only",  # Don't snapshot existing data, just monitor new changes
            "snapshot.locking.mode": "none",
            
            # Transform settings to get clean topic names
            "transforms": "route",
            "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.route.regex": "([^.]+)\\.([^.]+)\\.([^.]+)",
            "transforms.route.replacement": "$3",
            
            # Converter settings
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false",
            
            # Data type handling
            "decimal.handling.mode": "string",
            "time.precision.mode": "connect",
            "bigint.unsigned.handling.mode": "long",
            
            # Performance settings
            "max.batch.size": "2048",
            "max.queue.size": "8192",
            
            # Binary handling
            "binary.handling.mode": "base64",
            
            # Heartbeat for monitoring
            "heartbeat.interval.ms": "10000",
            "heartbeat.topics.prefix": "heartbeat"
        }
    }
    
    print("📊 Connector Configuration:")
    print(f"   Server Name: adtrace_mysql_{server_id}")
    print(f"   Tables: {table_include_list}")
    print(f"   Schema History Topic: schema-history-{server_id}")
    print(f"   Topic Prefix: adtrace")
    print(f"   Snapshot Mode: schema_only (monitor new changes only)")
    
    try:
        response = requests.post(
            "http://localhost:8083/connectors",
            headers={"Content-Type": "application/json"},
            json=connector_config,
            timeout=30
        )
        
        if response.status_code in [200, 201]:
            print("✅ Connector created successfully!")
            print(f"   Response: {response.json()}")
        else:
            print(f"❌ Failed to create connector: {response.status_code}")
            print(f"   Error: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error creating connector: {str(e)}")
        return False
    
    # Step 4: Wait and check connector status
    print("\n⏳ Waiting for connector to initialize...")
    time.sleep(10)
    
    try:
        response = requests.get("http://localhost:8083/connectors/dynamic-mysql-source/status", timeout=10)
        status = response.json()
        
        print("📊 Connector Status:")
        print(f"   Connector State: {status['connector']['state']}")
        
        if status['tasks']:
            task_state = status['tasks'][0]['state']
            print(f"   Task State: {task_state}")
            
            if task_state == "FAILED":
                print(f"   Error: {status['tasks'][0].get('trace', 'Unknown error')}")
                return False
        else:
            print("   No tasks found")
            
    except Exception as e:
        print(f"❌ Error checking connector status: {str(e)}")
        return False
    
    # Step 5: Check topics
    print("\n📋 Checking Kafka topics...")
    try:
        import subprocess
        result = subprocess.run(
            ["docker", "exec", "kafka", "kafka-topics", "--bootstrap-server", "kafka:29092", "--list"],
            capture_output=True, text=True, timeout=10
        )
        
        topics = result.stdout.strip().split('\n')
        relevant_topics = [t for t in topics if any(table in t for table in key_tables)]
        
        print(f"   Total topics: {len(topics)}")
        print(f"   Relevant topics: {relevant_topics}")
        
    except Exception as e:
        print(f"⚠️ Could not check topics: {str(e)}")
    
    print("\n🎯 Fix Summary:")
    print("   1. ✅ Deleted old failed connector")
    print("   2. ✅ Created new connector with proper configuration")
    print("   3. ✅ Focused on key tables for testing")
    print("   4. ✅ Used schema_only snapshot mode")
    print("   5. ✅ Configured proper topic naming")
    
    print("\n📝 Next Steps:")
    print("   1. Test by inserting/updating/deleting data in MySQL")
    print("   2. Check dashboard at http://localhost:3000")
    print("   3. Monitor topics in Kafka UI at http://localhost:8080")
    print("   4. Check CDC events at http://localhost:8000/metrics")
    
    print("\n🧪 Test Commands:")
    print("   # Insert test data")
    print(f"   mysql -h {mysql_host} -u {mysql_user} -p{mysql_password} {database_name}")
    print("   INSERT INTO buy_transaction (user_id, amount) VALUES (999, 100);")
    print("")
    print("   # Check CDC events")
    print("   curl http://localhost:8000/metrics")
    
    return True

if __name__ == "__main__":
    main() 