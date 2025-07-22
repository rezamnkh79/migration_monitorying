#!/usr/bin/env python3
import requests
import json
import os
import time
import subprocess

def create_simple_connector():
    """Create simple connector with direct topic naming"""
    
    mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
    mysql_port = os.getenv('MYSQL_PORT', '3306')
    mysql_user = os.getenv('MYSQL_USER', 'root')
    mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
    database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
    
    current_time = int(time.time())
    server_id = str(current_time)[-7:]
    
    print(f"🔧 Creating simple adtrace_migration connector...")
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
    
    time.sleep(3)
    
    # Simple connector - use adtrace_migration as direct topic prefix
    connector_config = {
        "name": "adtrace-migration-simple",
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
            # Monitor only buy_transaction for now
            "table.include.list": f"{database_name}.buy_transaction",
            "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
            "schema.history.internal.kafka.topic": f"schema-history-{server_id}",
            "include.schema.changes": "true",
            # Direct topic naming - this will create topic: adtrace_migration.adtrace_db_stage.buy_transaction
            "topic.prefix": "adtrace_migration",
            "snapshot.mode": "initial",
            "snapshot.locking.mode": "minimal",
            # NO TRANSFORMS - let it create natural topic names
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
            print("✅ Simple adtrace_migration connector created successfully")
            return True
        else:
            print(f"❌ Failed to create connector: {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"❌ Error creating connector: {str(e)}")
        return False

def check_and_test():
    """Check connector and test CDC"""
    
    print("\n📊 Waiting for connector to initialize...")
    time.sleep(20)
    
    # Check connector status
    try:
        status_response = requests.get("http://localhost:8083/connectors/adtrace-migration-simple/status")
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
                    print(f"❌ Task failed: {trace[:300]}...")
                    return False
                elif task_state == "RUNNING":
                    print("✅ Connector is running!")
            
            if connector_state == "RUNNING":
                # Check what topics were created
                print("\n📋 Checking created topics...")
                topics_result = subprocess.run([
                    "docker", "exec", "kafka", "kafka-topics", 
                    "--list", "--bootstrap-server", "localhost:9092"
                ], capture_output=True, text=True)
                
                migration_topics = [t for t in topics_result.stdout.split('\n') if 'adtrace_migration' in t]
                if migration_topics:
                    print(f"✅ Found migration topics: {migration_topics}")
                    
                    # Test CDC
                    return test_cdc_with_topics(migration_topics)
                else:
                    print("❌ No adtrace_migration topics found")
                    return False
        return False
            
    except Exception as e:
        print(f"❌ Error checking status: {str(e)}")
        return False

def test_cdc_with_topics(topics):
    """Test CDC with discovered topics"""
    
    # Generate unique test ID
    test_id = int(time.time()) % 100000
    
    mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
    mysql_user = os.getenv('MYSQL_USER', 'root')
    mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
    database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
    
    print(f"\n🧪 Testing CDC with account_id: {test_id}")
    
    # Insert test data
    insert_query = f"INSERT INTO {database_name}.buy_transaction (account_id, user_id, wallet_id, amount, creation_time, last_update_time, is_deleted) VALUES ({test_id}, {test_id}, 1, '888888', NOW(), NOW(), 0);"
    
    try:
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
            print("⏳ Waiting 15 seconds for CDC...")
            time.sleep(15)
            
            # Check each migration topic
            for topic in topics:
                print(f"\n📡 Checking topic: {topic}")
                kafka_result = subprocess.run([
                    "docker", "exec", "kafka",
                    "kafka-console-consumer",
                    "--bootstrap-server", "localhost:9092",
                    "--topic", topic,
                    "--timeout-ms", "10000"
                ], capture_output=True, text=True, timeout=15)
                
                if kafka_result.stdout:
                    lines = kafka_result.stdout.strip().split('\n')
                    print(f"📨 Found {len(lines)} messages in {topic}")
                    
                    # Look for our test message
                    found_test = False
                    for line in lines[-5:]:
                        try:
                            msg = json.loads(line)
                            if 'after' in msg and msg['after']:
                                account_id = msg['after'].get('account_id')
                                if account_id == test_id:
                                    print(f"🎉 SUCCESS! Found test message in {topic}!")
                                    print(f"   Account ID: {account_id}")
                                    print(f"   Operation: {msg.get('op', 'unknown')}")
                                    print(f"   Table: {msg.get('source', {}).get('table', 'unknown')}")
                                    found_test = True
                                    break
                        except:
                            continue
                    
                    if not found_test:
                        # Show latest message
                        try:
                            latest = json.loads(lines[-1])
                            account_id = latest.get('after', {}).get('account_id', 'N/A')
                            operation = latest.get('op', 'unknown')
                            print(f"📋 Latest in {topic}: Account {account_id}, Op: {operation}")
                        except:
                            print(f"📋 Latest raw: {lines[-1][:100]}...")
                    
                    if found_test:
                        return True
                        
                else:
                    print(f"❌ No messages in {topic}")
            
            return False
            
        else:
            print(f"❌ Failed to insert test data: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error in CDC test: {str(e)}")
        return False

def main():
    print("🚀 Creating Simple AdTrace Migration CDC...")
    print("=" * 60)
    
    if create_simple_connector():
        if check_and_test():
            print("\n🎉 SUCCESS!")
            print("✅ CDC is working with adtrace_migration topics")
            print("\n💡 Monitor topics:")
            print("docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 | grep adtrace_migration")
            print("docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic [TOPIC_NAME] --from-beginning")
        else:
            print("\n⚠️  Setup completed but CDC test needs verification")
    else:
        print("\n❌ Failed to create connector")

if __name__ == "__main__":
    main() 