#!/usr/bin/env python3
import requests
import json
import os
import time

def create_simple_working_connector():
    """Create a simple working connector"""
    
    mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
    mysql_port = os.getenv('MYSQL_PORT', '3306')
    mysql_user = os.getenv('MYSQL_USER', 'root')
    mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
    database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
    
    current_time = int(time.time())
    server_id = str(current_time)[-7:]
    
    print(f"🔧 Creating SIMPLE working CDC connector...")
    print(f"🆔 Server ID: {server_id}")
    
    # Delete all existing connectors
    print("🗑️  Cleaning up...")
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
    
    # Simple connector with minimal config
    connector_config = {
        "name": "adtrace-simple",
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "1",
            "database.hostname": mysql_host,
            "database.port": mysql_port,
            "database.user": mysql_user,
            "database.password": mysql_password,
            "database.server.id": server_id,
            "database.server.name": f"simple_{server_id}",
            "database.include.list": database_name,
            "table.include.list": f"{database_name}.buy_transaction",
            "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
            "schema.history.internal.kafka.topic": f"history-simple-{server_id}",
            "include.schema.changes": "true",
            
            # Simple topic naming - just use "adtrace_migration" as prefix
            "topic.prefix": "adtrace_migration",
            
            # Use initial snapshot to capture existing data and build schema
            "snapshot.mode": "initial",
            "snapshot.locking.mode": "minimal",
            
            # NO TRANSFORMS - keep it simple
            
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
            "database.ssl.mode": "disabled"
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
            print("✅ Simple connector created successfully")
            return True
        else:
            print(f"❌ Failed to create connector: {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"❌ Error creating connector: {str(e)}")
        return False

def check_connector():
    """Check connector status"""
    print("\n📊 Checking connector...")
    time.sleep(25)
    
    try:
        status_response = requests.get("http://localhost:8083/connectors/adtrace-simple/status")
        if status_response.status_code == 200:
            status_data = status_response.json()
            connector_state = status_data.get("connector", {}).get("state", "unknown")
            tasks = status_data.get("tasks", [])
            
            print(f"📋 Connector: {connector_state}")
            for i, task in enumerate(tasks):
                task_state = task.get("state", "unknown")
                print(f"📋 Task {i}: {task_state}")
                
                if task_state == "FAILED":
                    trace = task.get('trace', 'No trace')
                    print(f"❌ Failed: {trace[:200]}...")
                    return False
                elif task_state == "RUNNING":
                    print("✅ Running!")
            
            return connector_state == "RUNNING"
        else:
            print(f"❌ Status check failed: {status_response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error checking: {str(e)}")
        return False

def main():
    print("🚀 Creating Simple Working Connector...")
    print("=" * 50)
    
    if create_simple_working_connector():
        if check_connector():
            print("\n🎉 SUCCESS!")
            print("✅ Simple connector is working")
            print("📝 This will create topic: adtrace_migration.adtrace_db_stage.buy_transaction")
            print("\n💡 Test insert:")
            print("mysql -h 46.245.77.98 -u root -p -e \"INSERT INTO adtrace_db_stage.buy_transaction (account_id, user_id, wallet_id, amount, creation_time, last_update_time, is_deleted) VALUES (555555, 444444, 1, '999999', NOW(), NOW(), 0);\"")
            print("\n💡 Monitor:")
            print("docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic adtrace_migration.adtrace_db_stage.buy_transaction --from-beginning")
        else:
            print("\n❌ Connector failed")
    else:
        print("\n❌ Failed to create")

if __name__ == "__main__":
    main() 