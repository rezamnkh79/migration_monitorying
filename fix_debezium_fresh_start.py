#!/usr/bin/env python3
import requests
import json
import os
import time

def delete_connector(connector_name):
    """Delete existing connector"""
    try:
        response = requests.delete(f"http://localhost:8083/connectors/{connector_name}")
        if response.status_code in [204, 404]:
            print(f"✅ Connector {connector_name} deleted successfully")
            return True
        else:
            print(f"❌ Failed to delete connector: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error deleting connector: {str(e)}")
        return False

def create_fresh_connector():
    """Create a fresh connector with new configuration"""
    
    # Configuration from environment
    mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
    mysql_port = os.getenv('MYSQL_PORT', '3306')
    mysql_user = os.getenv('MYSQL_USER', 'root')
    mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
    database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
    
    # Generate unique server ID and topic name based on current timestamp
    current_time = int(time.time())
    server_id = str(current_time)[-7:]  # Use last 7 digits of timestamp
    schema_topic = f"schema-history-fresh-{server_id}"
    
    print(f"🔧 Creating fresh MySQL connector...")
    print(f"🆔 Server ID: {server_id}")
    print(f"📝 Schema topic: {schema_topic}")
    print(f"🏗️  Using initial snapshot mode")
    
    # Focus on a few key tables first for testing
    key_tables = [
        f"{database_name}.buy_transaction",
        f"{database_name}.adtrace_tracker", 
        f"{database_name}.adtrace_transaction"
    ]
    
    connector_config = {
        "name": "mysql-fresh-connector",
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "1",
            "database.hostname": mysql_host,
            "database.port": mysql_port,
            "database.user": mysql_user,
            "database.password": mysql_password,
            "database.server.id": server_id,
            "database.server.name": f"mysql_fresh_{server_id}",
            "database.include.list": database_name,
            "table.include.list": ",".join(key_tables),
            "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
            "schema.history.internal.kafka.topic": schema_topic,
            "include.schema.changes": "true",
            "topic.prefix": "fresh",
            # Use initial snapshot to capture existing data and schema
            "snapshot.mode": "initial",
            "snapshot.locking.mode": "minimal",
            # Transform settings
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
            # MySQL specific settings
            "database.ssl.mode": "disabled",
            "database.history.kafka.bootstrap.servers": "kafka:29092",
            "database.history.kafka.topic": schema_topic
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
            print("✅ Fresh MySQL connector created successfully")
            return True
        else:
            print(f"❌ Failed to create connector: {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"❌ Error creating connector: {str(e)}")
        return False

def check_connector_status(connector_name, wait_time=20):
    """Check connector status after creation"""
    print(f"⏳ Waiting {wait_time} seconds for connector to initialize...")
    time.sleep(wait_time)
    
    try:
        status_response = requests.get(f"http://localhost:8083/connectors/{connector_name}/status")
        if status_response.status_code == 200:
            status_data = status_response.json()
            connector_state = status_data.get("connector", {}).get("state", "unknown")
            tasks = status_data.get("tasks", [])
            
            print(f"📊 Connector State: {connector_state}")
            for i, task in enumerate(tasks):
                task_state = task.get("state", "unknown")
                print(f"📋 Task {i}: {task_state}")
                
                if task_state == "FAILED":
                    trace = task.get('trace', 'No trace')
                    print(f"❌ Task failed: {trace[:500]}...")
                    return False
                elif task_state == "RUNNING":
                    print("✅ Connector is running successfully!")
                    return True
            
            return connector_state == "RUNNING"
        else:
            print(f"❌ Failed to get connector status: {status_response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error checking connector status: {str(e)}")
        return False

def check_kafka_topics():
    """Check if new topics are created"""
    print("\n📊 Checking Kafka topics...")
    try:
        import subprocess
        result = subprocess.run(
            ["docker", "exec", "kafka", "kafka-topics", "--list", "--bootstrap-server", "localhost:9092"],
            capture_output=True, text=True
        )
        
        if result.returncode == 0:
            topics = result.stdout.strip().split('\n')
            fresh_topics = [t for t in topics if 'fresh' in t or 'schema-history-fresh' in t]
            if fresh_topics:
                print(f"✅ New topics created: {fresh_topics}")
                return True
            else:
                print("⚠️  No fresh topics found yet")
                return False
        else:
            print(f"❌ Failed to list topics: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error checking topics: {str(e)}")
        return False

def main():
    print("🚀 Creating Fresh Debezium Connector...")
    print("=" * 60)
    
    # Step 1: Clean up existing connectors
    print("🗑️  Step 1: Cleaning up existing connectors...")
    delete_connector("mysql-recovery-connector")
    delete_connector("dynamic-mysql-source")
    time.sleep(5)
    
    # Step 2: Create fresh connector
    print("\n🔧 Step 2: Creating fresh connector...")
    if create_fresh_connector():
        print("✅ Connector created successfully")
        
        # Step 3: Check status
        print("\n📊 Step 3: Checking connector status...")
        if check_connector_status("mysql-fresh-connector"):
            print("\n🎉 SUCCESS: Fresh Debezium connector is working!")
            
            # Step 4: Check topics
            check_kafka_topics()
            
            print("\n🔄 Next steps:")
            print("1. The connector will now monitor changes in real-time")
            print("2. Test by inserting/updating data in MySQL tables:")
            print("   - buy_transaction")
            print("   - adtrace_tracker") 
            print("   - adtrace_transaction")
            print("3. Check Kafka topics: docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 | grep fresh")
        else:
            print("\n❌ Connector still has issues. Let me check the logs...")
            time.sleep(5)
            check_connector_status("mysql-fresh-connector", 0)
    else:
        print("❌ Failed to create fresh connector")

if __name__ == "__main__":
    main() 