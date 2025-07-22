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
        # Get list of connectors
        response = requests.get("http://localhost:8083/connectors")
        if response.status_code == 200:
            connectors = response.json()
            for connector in connectors:
                print(f"   Deleting {connector}...")
                requests.delete(f"http://localhost:8083/connectors/{connector}")
                time.sleep(1)
            print("✅ All connectors deleted")
        else:
            print("❌ Failed to get connector list")
    except Exception as e:
        print(f"❌ Error deleting connectors: {str(e)}")

def create_single_topic_connector():
    """Create a connector with single topic for all changes"""
    
    # Configuration from environment
    mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
    mysql_port = os.getenv('MYSQL_PORT', '3306')
    mysql_user = os.getenv('MYSQL_USER', 'root')
    mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
    database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
    
    # Generate unique server ID
    current_time = int(time.time())
    server_id = str(current_time)[-7:]
    schema_topic = f"schema-history-{server_id}"
    
    print(f"🔧 Creating single topic CDC connector...")
    print(f"🆔 Server ID: {server_id}")
    print(f"📝 Schema topic: {schema_topic}")
    print(f"🎯 Main topic: adtrace_migration")
    
    connector_config = {
        "name": "adtrace-migration-connector",
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
            # Monitor specific tables for testing
            "table.include.list": f"{database_name}.buy_transaction,{database_name}.adtrace_tracker,{database_name}.adtrace_transaction",
            "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
            "schema.history.internal.kafka.topic": schema_topic,
            "include.schema.changes": "true",
            # THIS IS KEY: Single topic name
            "topic.prefix": "adtrace_migration",
            "snapshot.mode": "when_needed",  # Only snapshot if needed
            "snapshot.locking.mode": "minimal",
            # Transform settings - Route all to single topic
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
            # MySQL specific settings
            "database.ssl.mode": "disabled",
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
            print("✅ Single topic connector created successfully")
            return True
        else:
            print(f"❌ Failed to create connector: {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"❌ Error creating connector: {str(e)}")
        return False

def check_connector_status(connector_name="adtrace-migration-connector", wait_time=15):
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

def check_kafka_topics():
    """Check if adtrace_migration topic exists"""
    print("\n📊 Checking Kafka topics...")
    try:
        result = subprocess.run(
            ["docker", "exec", "kafka", "kafka-topics", "--list", "--bootstrap-server", "localhost:9092"],
            capture_output=True, text=True, timeout=30
        )
        
        if result.returncode == 0:
            topics = result.stdout.strip().split('\n')
            migration_topics = [t for t in topics if 'adtrace_migration' in t]
            if migration_topics:
                print(f"✅ Migration topics found: {migration_topics}")
                return True
            else:
                print("⚠️  No adtrace_migration topic found yet")
                return False
        else:
            print(f"❌ Failed to list topics: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error checking topics: {str(e)}")
        return False

def test_insert_data():
    """Test inserting data into MySQL and check if message appears in Kafka"""
    print("\n🧪 Testing real-time CDC by inserting test data...")
    
    # Generate test data
    test_id = int(time.time()) % 10000
    test_amount = f"{test_id * 100}"
    
    mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
    mysql_user = os.getenv('MYSQL_USER', 'root')
    mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
    database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
    
    # Insert test data using mysql command
    insert_query = f"INSERT INTO {database_name}.buy_transaction (account_id, user_id, wallet_id, amount, creation_time, last_update_time, is_deleted, mobile_app_id, description) VALUES ({test_id}, {test_id}, 1, '{test_amount}', {int(time.time() * 1000)}, {int(time.time() * 1000)}, 0, NULL, 'CDC Test Data');"
    
    try:
        print(f"📝 Inserting test record with ID: {test_id}, Amount: {test_amount}")
        
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
            return test_id
        else:
            print(f"❌ Failed to insert test data: {result.stderr}")
            return None
            
    except Exception as e:
        print(f"❌ Error inserting test data: {str(e)}")
        return None

def monitor_kafka_topic(test_id=None, timeout_seconds=30):
    """Monitor adtrace_migration topic for new messages"""
    print(f"\n📡 Monitoring adtrace_migration topic for {timeout_seconds} seconds...")
    if test_id:
        print(f"🔍 Looking for test record with ID: {test_id}")
    
    try:
        # Use kafka-console-consumer to monitor the topic
        kafka_cmd = [
            "docker", "exec", "kafka", 
            "kafka-console-consumer",
            "--bootstrap-server", "localhost:9092",
            "--topic", "adtrace_migration",
            "--from-beginning",
            "--timeout-ms", str(timeout_seconds * 1000)
        ]
        
        result = subprocess.run(kafka_cmd, capture_output=True, text=True, timeout=timeout_seconds + 5)
        
        if result.stdout:
            messages = result.stdout.strip().split('\n')
            print(f"📨 Found {len(messages)} messages in topic")
            
            # Look for our test message
            test_found = False
            for i, message in enumerate(messages[-10:]):  # Check last 10 messages
                try:
                    msg_data = json.loads(message)
                    if test_id and 'after' in msg_data and msg_data['after']:
                        account_id = msg_data['after'].get('account_id')
                        if account_id == test_id:
                            print(f"🎉 FOUND our test message! Account ID: {account_id}")
                            print(f"   Operation: {msg_data.get('op', 'unknown')}")
                            print(f"   Table: {msg_data.get('source', {}).get('table', 'unknown')}")
                            test_found = True
                            break
                except:
                    continue
            
            if test_id and not test_found:
                print(f"⚠️  Test message with ID {test_id} not found in recent messages")
            
            # Show sample of latest message
            try:
                latest_msg = json.loads(messages[-1])
                print(f"\n📋 Latest message sample:")
                print(f"   Operation: {latest_msg.get('op', 'unknown')}")
                print(f"   Table: {latest_msg.get('source', {}).get('table', 'unknown')}")
                if 'after' in latest_msg and latest_msg['after']:
                    print(f"   Data: {str(latest_msg['after'])[:100]}...")
            except:
                print(f"   Raw: {messages[-1][:100]}...")
                
            return True
        else:
            print("⚠️  No messages found in adtrace_migration topic")
            return False
            
    except Exception as e:
        print(f"❌ Error monitoring topic: {str(e)}")
        return False

def main():
    print("🚀 Setting up Single Topic CDC for AdTrace Migration...")
    print("=" * 70)
    
    # Step 1: Clean up
    delete_all_connectors()
    time.sleep(3)
    
    # Step 2: Create single topic connector
    print("\n🔧 Step 2: Creating single topic connector...")
    if create_single_topic_connector():
        
        # Step 3: Check status
        print("\n📊 Step 3: Checking connector status...")
        if check_connector_status():
            
            # Step 4: Check topics
            print("\n📋 Step 4: Checking Kafka topics...")
            check_kafka_topics()
            
            # Step 5: Test real-time CDC
            print("\n🧪 Step 5: Testing real-time CDC...")
            test_id = test_insert_data()
            
            if test_id:
                # Wait a moment for message to propagate
                time.sleep(5)
                
                # Monitor topic for our test message
                monitor_kafka_topic(test_id)
            
            print("\n" + "=" * 70)
            print("🎯 SUMMARY:")
            print("✅ Single topic CDC connector created")
            print("✅ Topic name: adtrace_migration")
            print("✅ Monitoring buy_transaction, adtrace_tracker, adtrace_transaction")
            print("\n💡 Manual test commands:")
            print("1. Monitor topic: docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic adtrace_migration --from-beginning")
            print("2. Insert test data: mysql -h 46.245.77.98 -u root -p")
            print("3. Check connector: curl -s http://localhost:8083/connectors/adtrace-migration-connector/status | jq .")
                
        else:
            print("❌ Connector failed to start properly")
    else:
        print("❌ Failed to create connector")

if __name__ == "__main__":
    main() 