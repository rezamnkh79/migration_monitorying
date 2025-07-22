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

def create_connector_with_schema_recovery():
    """Create connector with SCHEMA_ONLY_RECOVERY mode"""
    
    # Configuration from environment
    mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
    mysql_port = os.getenv('MYSQL_PORT', '3306')
    mysql_user = os.getenv('MYSQL_USER', 'root')
    mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
    database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
    
    # Generate unique server ID and topic name
    import hashlib
    server_id = str(abs(hash(f"{mysql_host}-{int(time.time())}")) % 10000000)
    schema_topic = f"schema-history-recovery-{server_id}"
    
    print(f"🔧 Creating MySQL connector with SCHEMA_ONLY_RECOVERY...")
    print(f"🆔 Server ID: {server_id}")
    print(f"📝 Schema topic: {schema_topic}")
    
    connector_config = {
        "name": "mysql-recovery-connector",
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "1",
            "database.hostname": mysql_host,
            "database.port": mysql_port,
            "database.user": mysql_user,
            "database.password": mysql_password,
            "database.server.id": server_id,
            "database.server.name": f"mysql_recovery_{server_id}",
            "database.include.list": database_name,
            "table.include.list": f"{database_name}.buy_transaction,{database_name}.adtrace_tracker,{database_name}.adtrace_transaction",
            "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
            "schema.history.internal.kafka.topic": schema_topic,
            "include.schema.changes": "true",
            "topic.prefix": "recovery",
            # THIS IS THE KEY: Use SCHEMA_ONLY_RECOVERY mode
            "snapshot.mode": "schema_only_recovery",
            "snapshot.locking.mode": "none",
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
            # Binary handling
            "binary.handling.mode": "base64"
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
            print("✅ MySQL recovery connector created successfully")
            return True
        else:
            print(f"❌ Failed to create connector: {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"❌ Error creating connector: {str(e)}")
        return False

def check_connector_status(connector_name, wait_time=30):
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

def main():
    print("🚀 Fixing Debezium Schema History Issue...")
    print("=" * 60)
    
    # Step 1: Delete existing connector
    print("🗑️  Step 1: Deleting existing connector...")
    delete_connector("dynamic-mysql-source")
    time.sleep(5)
    
    # Step 2: Create new connector with SCHEMA_ONLY_RECOVERY
    print("\n🔧 Step 2: Creating connector with SCHEMA_ONLY_RECOVERY...")
    if create_connector_with_schema_recovery():
        print("✅ Connector created successfully")
        
        # Step 3: Check status
        print("\n📊 Step 3: Checking connector status...")
        if check_connector_status("mysql-recovery-connector"):
            print("\n🎉 SUCCESS: Debezium connector is now working!")
            print("🔄 The connector will now monitor changes in real-time")
            print("📝 Test by inserting data into MySQL tables")
        else:
            print("\n❌ Connector still has issues. Check logs for details.")
    else:
        print("❌ Failed to create recovery connector")

if __name__ == "__main__":
    main() 