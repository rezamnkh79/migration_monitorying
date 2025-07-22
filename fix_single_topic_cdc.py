#!/usr/bin/env python3
import requests
import json
import os
import time
import subprocess

def delete_all_connectors():
    """Delete all existing connectors"""
    print("🗑️  Cleaning up all existing connectors...")
    try:
        response = requests.get("http://localhost:8083/connectors")
        if response.status_code == 200:
            connectors = response.json()
            for connector in connectors:
                print(f"   Deleting {connector}...")
                requests.delete(f"http://localhost:8083/connectors/{connector}")
                time.sleep(1)
            print("✅ All connectors deleted")
        return True
    except Exception as e:
        print(f"❌ Error deleting connectors: {str(e)}")
        return False

def create_corrected_connector():
    """Create corrected single topic connector"""
    
    mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
    mysql_port = os.getenv('MYSQL_PORT', '3306')
    mysql_user = os.getenv('MYSQL_USER', 'root')
    mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
    database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
    
    current_time = int(time.time())
    server_id = str(current_time)[-7:]
    schema_topic = f"schema-history-{server_id}"
    
    print(f"🔧 Creating corrected single topic CDC connector...")
    print(f"🆔 Server ID: {server_id}")
    print(f"📝 Schema topic: {schema_topic}")
    print(f"🎯 Target topic: adtrace_migration")
    
    connector_config = {
        "name": "adtrace-migration-fixed",
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
            "table.include.list": f"{database_name}.buy_transaction,{database_name}.adtrace_tracker,{database_name}.adtrace_transaction",
            "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
            "schema.history.internal.kafka.topic": schema_topic,
            "include.schema.changes": "true",
            # Use topic.prefix to create base topic name
            "topic.prefix": "adtrace",  
            "snapshot.mode": "never",  # Skip snapshot, only capture new changes
            "snapshot.locking.mode": "none",
            # Simple transform to route all to single topic
            "transforms": "route",
            "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.route.regex": "adtrace\\.(.*)",
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
            print("✅ Corrected connector created successfully")
            return True
        else:
            print(f"❌ Failed to create connector: {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"❌ Error creating connector: {str(e)}")
        return False

def check_connector_status(connector_name="adtrace-migration-fixed", wait_time=10):
    """Check connector status"""
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
                    print(f"❌ Task failed: {trace[:300]}...")
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

def test_real_time_cdc():
    """Test real-time CDC functionality"""
    print("\n🧪 Testing real-time CDC...")
    
    # Generate unique test ID
    test_id = int(time.time()) % 100000
    
    mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
    mysql_user = os.getenv('MYSQL_USER', 'root') 
    mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
    database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
    
    print(f"📝 Inserting test record with account_id: {test_id}")
    
    # Insert test data
    insert_query = f"INSERT INTO {database_name}.buy_transaction (account_id, user_id, wallet_id, amount, creation_time, last_update_time, is_deleted) VALUES ({test_id}, {test_id}, 1, '999999', NOW(), NOW(), 0);"
    
    try:
        mysql_cmd = [
            "mysql", 
            f"-h{mysql_host}",
            f"-u{mysql_user}",
            f"-p{mysql_password}",
            "-e", insert_query
        ]
        
        result = subprocess.run(mysql_cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("✅ Test data inserted successfully into MySQL")
            
            # Wait a moment for CDC to process
            print("⏳ Waiting 10 seconds for CDC to process...")
            time.sleep(10)
            
            # Check adtrace_migration topic
            print("📡 Checking adtrace_migration topic...")
            kafka_cmd = [
                "docker", "exec", "kafka",
                "kafka-console-consumer",
                "--bootstrap-server", "localhost:9092", 
                "--topic", "adtrace_migration",
                "--from-beginning",
                "--timeout-ms", "15000"
            ]
            
            kafka_result = subprocess.run(kafka_cmd, capture_output=True, text=True, timeout=20)
            
            if kafka_result.stdout:
                messages = kafka_result.stdout.strip().split('\n')
                print(f"📨 Found {len(messages)} messages in adtrace_migration topic")
                
                # Look for our test message
                for message in messages[-5:]:  # Check last 5 messages
                    try:
                        msg_data = json.loads(message)
                        if 'after' in msg_data and msg_data['after']:
                            account_id = msg_data['after'].get('account_id')
                            if account_id == test_id:
                                print(f"🎉 FOUND our test message! Account ID: {account_id}")
                                print(f"   Operation: {msg_data.get('op', 'unknown')}")
                                print(f"   Table: {msg_data.get('source', {}).get('table', 'unknown')}")
                                return True
                    except:
                        continue
                
                print(f"⚠️  Test message not found. Latest message sample:")
                try:
                    latest = json.loads(messages[-1])
                    print(f"   Account ID: {latest.get('after', {}).get('account_id', 'N/A')}")
                    print(f"   Operation: {latest.get('op', 'unknown')}")
                except:
                    print(f"   Raw: {messages[-1][:100]}...")
                    
            else:
                print("❌ No messages found in adtrace_migration topic")
                
                # Check if topic exists
                topics_result = subprocess.run(
                    ["docker", "exec", "kafka", "kafka-topics", "--list", "--bootstrap-server", "localhost:9092"],
                    capture_output=True, text=True
                )
                
                if "adtrace_migration" in topics_result.stdout:
                    print("✅ Topic exists but no messages")
                else:
                    print("❌ Topic adtrace_migration does not exist")
                    
            return False
            
        else:
            print(f"❌ Failed to insert test data: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error in CDC test: {str(e)}")
        return False

def main():
    print("🚀 Setting up Corrected Single Topic CDC...")
    print("=" * 60)
    
    # Step 1: Clean up
    delete_all_connectors()
    time.sleep(3)
    
    # Step 2: Create corrected connector
    print("\n🔧 Step 2: Creating corrected connector...")
    if create_corrected_connector():
        
        # Step 3: Check status
        print("\n📊 Step 3: Checking connector status...")
        if check_connector_status():
            
            # Step 4: Test real-time CDC
            if test_real_time_cdc():
                print("\n🎉 SUCCESS: Real-time CDC is working!")
                print("✅ Topic: adtrace_migration")
                print("✅ New changes are being captured")
            else:
                print("\n⚠️  CDC test failed - check configuration")
                
            print("\n💡 Manual monitoring command:")
            print("docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic adtrace_migration --from-beginning")
            
        else:
            print("❌ Connector failed to start")
    else:
        print("❌ Failed to create connector")

if __name__ == "__main__":
    main() 