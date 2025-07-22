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

def create_working_connector():
    """Create final working CDC connector"""
    
    mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
    mysql_port = os.getenv('MYSQL_PORT', '3306')
    mysql_user = os.getenv('MYSQL_USER', 'root')
    mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
    database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
    
    current_time = int(time.time())
    server_id = str(current_time)[-7:]
    
    print(f"🔧 Creating final working CDC connector...")
    print(f"🆔 Server ID: {server_id}")
    print(f"🎯 Target topic: adtrace_migration")
    
    connector_config = {
        "name": "adtrace-migration-final",
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
            "schema.history.internal.kafka.topic": f"schema-history-final-{server_id}",
            "include.schema.changes": "true",
            # Use direct topic naming without complex transforms
            "topic.prefix": "adtrace_migration",
            "snapshot.mode": "schema_only",  # Capture schema first, then real-time changes
            "snapshot.locking.mode": "minimal",
            # NO TRANSFORMS - let each table create its own topic under adtrace_migration prefix
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
            # MySQL specific settings for better reliability
            "connect.timeout.ms": "30000",
            "database.connectionTimeZone": "UTC"
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
            print("✅ Final connector created successfully")
            return True
        else:
            print(f"❌ Failed to create connector: {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"❌ Error creating connector: {str(e)}")
        return False

def check_connector_status(connector_name="adtrace-migration-final", wait_time=15):
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

def check_topics():
    """Check what topics were created"""
    print("\n📋 Checking created topics...")
    try:
        result = subprocess.run(
            ["docker", "exec", "kafka", "kafka-topics", "--list", "--bootstrap-server", "localhost:9092"],
            capture_output=True, text=True, timeout=30
        )
        
        if result.returncode == 0:
            topics = result.stdout.strip().split('\n')
            migration_topics = [t for t in topics if 'adtrace_migration' in t]
            if migration_topics:
                print(f"✅ AdTrace Migration topics found:")
                for topic in migration_topics:
                    print(f"   📊 {topic}")
                return migration_topics
            else:
                print("⚠️  No adtrace_migration topics found")
                return []
        else:
            print(f"❌ Failed to list topics: {result.stderr}")
            return []
            
    except Exception as e:
        print(f"❌ Error checking topics: {str(e)}")
        return []

def test_real_time_insert():
    """Test real-time CDC by inserting data"""
    print("\n🧪 Testing real-time CDC...")
    
    # Generate unique test ID based on current time
    test_id = int(time.time()) % 100000
    
    mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
    mysql_user = os.getenv('MYSQL_USER', 'root') 
    mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
    database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
    
    print(f"📝 Inserting test record with account_id: {test_id}")
    
    # Insert test data
    insert_query = f"INSERT INTO {database_name}.buy_transaction (account_id, user_id, wallet_id, amount, creation_time, last_update_time, is_deleted) VALUES ({test_id}, {test_id}, 1, '888888', NOW(), NOW(), 0);"
    
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
            return test_id
        else:
            print(f"❌ Failed to insert test data: {result.stderr}")
            return None
            
    except Exception as e:
        print(f"❌ Error inserting test data: {str(e)}")
        return None

def monitor_topic_for_message(topic_name, test_id, timeout_seconds=20):
    """Monitor specific topic for our test message"""
    print(f"📡 Monitoring topic '{topic_name}' for test message...")
    
    try:
        kafka_cmd = [
            "docker", "exec", "kafka",
            "kafka-console-consumer",
            "--bootstrap-server", "localhost:9092", 
            "--topic", topic_name,
            "--from-beginning",
            "--timeout-ms", str(timeout_seconds * 1000)
        ]
        
        result = subprocess.run(kafka_cmd, capture_output=True, text=True, timeout=timeout_seconds + 5)
        
        if result.stdout:
            messages = result.stdout.strip().split('\n')
            print(f"   📨 Found {len(messages)} messages")
            
            # Look for our test message in recent messages
            for message in messages[-10:]:  # Check last 10 messages
                try:
                    msg_data = json.loads(message)
                    if 'after' in msg_data and msg_data['after']:
                        account_id = msg_data['after'].get('account_id')
                        if account_id == test_id:
                            print(f"   🎉 FOUND test message! Account ID: {account_id}")
                            print(f"   🔄 Operation: {msg_data.get('op', 'unknown')}")
                            print(f"   📋 Table: {msg_data.get('source', {}).get('table', 'unknown')}")
                            return True
                except:
                    continue
            
            print(f"   ⚠️  Test message with ID {test_id} not found")
            return False
        else:
            print(f"   ❌ No messages found in topic '{topic_name}'")
            return False
            
    except Exception as e:
        print(f"❌ Error monitoring topic: {str(e)}")
        return False

def comprehensive_cdc_test():
    """Comprehensive CDC test"""
    print("\n🎯 Comprehensive CDC Test...")
    
    # Step 1: Insert test data
    test_id = test_real_time_insert()
    if not test_id:
        print("❌ Failed to insert test data")
        return False
    
    # Step 2: Wait for CDC processing
    print("⏳ Waiting 10 seconds for CDC processing...")
    time.sleep(10)
    
    # Step 3: Check all adtrace_migration topics
    topics = check_topics()
    if not topics:
        print("❌ No topics found")
        return False
    
    # Step 4: Monitor each topic for our message
    found_message = False
    for topic in topics:
        if monitor_topic_for_message(topic, test_id):
            found_message = True
            break
    
    if found_message:
        print("🎉 SUCCESS: Real-time CDC is working!")
        print("✅ Message found in Kafka topic")
        return True
    else:
        print("⚠️  Test message not found in any topic")
        return False

def main():
    print("🚀 Final AdTrace Migration CDC Setup...")
    print("=" * 60)
    
    # Step 1: Clean up
    delete_all_connectors()
    time.sleep(3)
    
    # Step 2: Create final connector
    print("\n🔧 Step 2: Creating final working connector...")
    if create_working_connector():
        
        # Step 3: Check status
        print("\n📊 Step 3: Checking connector status...")
        if check_connector_status():
            
            # Step 4: Check topics
            topics = check_topics()
            
            # Step 5: Comprehensive test
            if comprehensive_cdc_test():
                print("\n🎉 FINAL SUCCESS!")
                print("✅ AdTrace Migration CDC is working")
                print("✅ Real-time changes are being captured")
                print(f"✅ Active topics: {topics}")
                
                print("\n💡 Manual monitoring commands:")
                for topic in topics:
                    print(f"   docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic {topic} --from-beginning")
                    
            else:
                print("\n⚠️  CDC test failed but connector is running")
                print("💡 Try manual insert: mysql -h 46.245.77.98 -u root -p")
                
        else:
            print("❌ Connector failed to start")
    else:
        print("❌ Failed to create connector")

if __name__ == "__main__":
    main() 