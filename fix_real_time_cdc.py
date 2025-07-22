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

def wait_and_check_connector():
    """Wait for connector to initialize and check status"""
    
    print("\n📊 Waiting for connector to initialize...")
    time.sleep(30)  # Give it more time
    
    try:
        status_response = requests.get("http://localhost:8083/connectors/adtrace-migration-working/status")
        if status_response.status_code == 200:
            status_data = status_response.json()
            connector_state = status_data.get("connector", {}).get("state", "unknown")
            tasks = status_data.get("tasks", [])
            
            print(f"📋 Connector State: {connector_state}")
            for i, task in enumerate(tasks):
                task_state = task.get("state", "unknown")
                print(f"📋 Task {i}: {task_state}")
                
                if task_state == "FAILED":
                    trace = task.get('trace', 'No trace')
                    print(f"❌ Task failed: {trace[:500]}...")
                    return False
                elif task_state == "RUNNING":
                    print("✅ Connector is running!")
            
            return connector_state == "RUNNING"
        else:
            print(f"❌ Failed to get connector status: {status_response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error checking status: {str(e)}")
        return False

def test_real_time_cdc():
    """Test real-time CDC properly"""
    
    # Check what topics exist
    print("\n📋 Checking available topics...")
    try:
        topics_result = subprocess.run([
            "docker", "exec", "kafka", "kafka-topics", 
            "--list", "--bootstrap-server", "localhost:9092"
        ], capture_output=True, text=True)
        
        migration_topics = [t.strip() for t in topics_result.stdout.split('\n') if 'adtrace_migration' in t and t.strip()]
        print(f"✅ Found migration topics: {migration_topics}")
        
        if not migration_topics:
            print("❌ No adtrace_migration topics found!")
            return False
            
    except Exception as e:
        print(f"❌ Error checking topics: {str(e)}")
        return False
    
    # Generate unique test ID
    test_id = int(time.time()) % 100000
    
    mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
    mysql_user = os.getenv('MYSQL_USER', 'root')
    mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
    database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
    
    print(f"\n🧪 Testing real-time CDC with account_id: {test_id}")
    
    # Start monitoring topic in background BEFORE inserting data
    print("📡 Starting topic monitoring...")
    
    # Use the main migration topic
    monitor_topic = "adtrace_migration"
    if monitor_topic not in migration_topics:
        monitor_topic = migration_topics[0]  # Use first available topic
    
    print(f"📡 Will monitor topic: {monitor_topic}")
    
    # Insert test data
    insert_query = f"INSERT INTO {database_name}.buy_transaction (account_id, user_id, wallet_id, amount, creation_time, last_update_time, is_deleted) VALUES ({test_id}, {test_id}, 1, '999999', NOW(), NOW(), 0);"
    
    try:
        print(f"📝 Inserting test record with account_id: {test_id}")
        result = subprocess.run([
            "mysql", 
            f"-h{mysql_host}",
            f"-u{mysql_user}",
            f"-p{mysql_password}",
            "-e", insert_query
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("✅ Test data inserted into MySQL")
            
            # Wait for CDC processing
            print("⏳ Waiting 20 seconds for CDC to process...")
            time.sleep(20)
            
            # Check topic for messages
            print(f"📡 Checking {monitor_topic} for our message...")
            kafka_result = subprocess.run([
                "docker", "exec", "kafka",
                "kafka-console-consumer",
                "--bootstrap-server", "localhost:9092",
                "--topic", monitor_topic,
                "--from-beginning",
                "--timeout-ms", "15000"
            ], capture_output=True, text=True, timeout=20)
            
            if kafka_result.stdout:
                lines = [line.strip() for line in kafka_result.stdout.strip().split('\n') if line.strip()]
                print(f"📨 Found {len(lines)} messages in {monitor_topic}")
                
                # Look for our test message
                found_test = False
                for line in lines:
                    try:
                        msg = json.loads(line)
                        if 'after' in msg and msg['after'] and msg['after'].get('account_id') == test_id:
                            print(f"🎉 SUCCESS! Found our test message!")
                            print(f"   Account ID: {msg['after']['account_id']}")
                            print(f"   Amount: {msg['after']['amount']}")
                            print(f"   Operation: {msg.get('op', 'unknown')}")
                            print(f"   Table: {msg.get('source', {}).get('table', 'unknown')}")
                            found_test = True
                            break
                    except:
                        continue
                
                if not found_test:
                    print(f"⚠️  Test message with ID {test_id} not found")
                    print("📋 Latest messages:")
                    for line in lines[-3:]:
                        try:
                            msg = json.loads(line)
                            account_id = msg.get('after', {}).get('account_id', 'N/A')
                            operation = msg.get('op', 'unknown')
                            print(f"   Account {account_id}, Op: {operation}")
                        except:
                            print(f"   Raw: {line[:100]}...")
                else:
                    print("🎯 Real-time CDC is working correctly!")
                    return True
                    
            else:
                print(f"❌ No messages found in {monitor_topic}")
                print("💡 This means CDC is not capturing real-time changes")
                return False
            
        else:
            print(f"❌ Failed to insert test data: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error in CDC test: {str(e)}")
        return False
    
    return False

def main():
    print("🚀 FIXING Real-Time CDC...")
    print("=" * 60)
    
    if create_working_connector():
        if wait_and_check_connector():
            print("\n✅ Connector is running, now testing...")
            
            if test_real_time_cdc():
                print("\n🎉 SUCCESS!")
                print("✅ Real-time CDC is now working correctly")
                print("✅ Changes to MySQL are being captured in Kafka")
                print("\n💡 Monitor topic:")
                print("docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic adtrace_migration --from-beginning")
            else:
                print("\n⚠️  CDC setup completed but real-time capture is not working")
                print("💡 Try manual test:")
                print("1. Insert: mysql -h 46.245.77.98 -u root -p")
                print("2. Monitor: docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic adtrace_migration --from-beginning")
        else:
            print("\n❌ Connector failed to start properly")
    else:
        print("\n❌ Failed to create connector")

if __name__ == "__main__":
    main() 