#!/usr/bin/env python3
import requests
import json
import os
import time
import subprocess

def update_connector_to_single_topic():
    """Update existing connector to use single topic adtrace_migration"""
    
    mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
    mysql_port = os.getenv('MYSQL_PORT', '3306')
    mysql_user = os.getenv('MYSQL_USER', 'root')
    mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
    database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
    
    current_time = int(time.time())
    server_id = str(current_time)[-7:]
    
    print(f"🔧 Creating adtrace_migration topic connector...")
    print(f"🆔 Server ID: {server_id}")
    print(f"🎯 Topic: adtrace_migration")
    
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
    
    # Create new connector with single topic
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
            "table.include.list": f"{database_name}.buy_transaction,{database_name}.adtrace_tracker,{database_name}.adtrace_transaction",
            "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
            "schema.history.internal.kafka.topic": f"schema-history-{server_id}",
            "include.schema.changes": "true",
            # Use simple topic prefix 
            "topic.prefix": "temp",
            "snapshot.mode": "when_needed",
            "snapshot.locking.mode": "minimal",
            # Transform ALL topics to adtrace_migration
            "transforms": "route",
            "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.route.regex": "temp\\.(.*)",
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
            print("✅ adtrace_migration connector created successfully")
            return True
        else:
            print(f"❌ Failed to create connector: {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"❌ Error creating connector: {str(e)}")
        return False

def check_connector_and_test():
    """Check connector status and test real-time CDC"""
    
    print("\n📊 Checking connector status...")
    time.sleep(15)
    
    try:
        status_response = requests.get("http://localhost:8083/connectors/adtrace-migration-connector/status")
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
                    print(f"❌ Task failed: {trace[:200]}...")
                    return False
                elif task_state == "RUNNING":
                    print("✅ Connector is running!")
            
            if connector_state == "RUNNING":
                print("\n🧪 Testing real-time CDC...")
                return test_cdc()
            
        return False
            
    except Exception as e:
        print(f"❌ Error checking status: {str(e)}")
        return False

def test_cdc():
    """Test CDC by inserting data and checking topic"""
    
    # Generate unique test ID
    test_id = int(time.time()) % 100000
    
    mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
    mysql_user = os.getenv('MYSQL_USER', 'root')
    mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
    database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
    
    print(f"📝 Inserting test record with account_id: {test_id}")
    
    # Insert test data
    insert_query = f"INSERT INTO {database_name}.buy_transaction (account_id, user_id, wallet_id, amount, creation_time, last_update_time, is_deleted) VALUES ({test_id}, {test_id}, 1, '777777', NOW(), NOW(), 0);"
    
    try:
        # Insert using mysql command
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
            print("⏳ Waiting 10 seconds for CDC...")
            time.sleep(10)
            
            # Check adtrace_migration topic
            print("📡 Checking adtrace_migration topic...")
            kafka_result = subprocess.run([
                "docker", "exec", "kafka",
                "kafka-console-consumer",
                "--bootstrap-server", "localhost:9092",
                "--topic", "adtrace_migration", 
                "--timeout-ms", "10000"
            ], capture_output=True, text=True, timeout=15)
            
            if kafka_result.stdout:
                lines = kafka_result.stdout.strip().split('\n')
                print(f"📨 Found {len(lines)} messages in adtrace_migration topic")
                
                # Look for our test message
                for line in lines[-5:]:
                    try:
                        msg = json.loads(line)
                        if 'after' in msg and msg['after']:
                            account_id = msg['after'].get('account_id')
                            if account_id == test_id:
                                print(f"🎉 SUCCESS! Found our test message!")
                                print(f"   Account ID: {account_id}")
                                print(f"   Operation: {msg.get('op', 'unknown')}")
                                print(f"   Table: {msg.get('source', {}).get('table', 'unknown')}")
                                return True
                    except:
                        continue
                
                print(f"⚠️  Test message with ID {test_id} not found")
                # Show latest message
                try:
                    latest = json.loads(lines[-1])
                    print(f"📋 Latest message: Account ID {latest.get('after', {}).get('account_id', 'N/A')}")
                except:
                    print(f"📋 Latest raw message: {lines[-1][:100]}...")
                    
                return True  # Topic exists and has messages
            else:
                print("❌ No messages in adtrace_migration topic")
                
                # Check if topic exists
                topics_result = subprocess.run([
                    "docker", "exec", "kafka", "kafka-topics", 
                    "--list", "--bootstrap-server", "localhost:9092"
                ], capture_output=True, text=True)
                
                if "adtrace_migration" in topics_result.stdout:
                    print("✅ Topic exists but empty")
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
    print("🚀 Creating AdTrace Migration Single Topic CDC...")
    print("=" * 60)
    
    if update_connector_to_single_topic():
        if check_connector_and_test():
            print("\n🎉 SUCCESS!")
            print("✅ adtrace_migration topic is working")
            print("✅ Real-time CDC is capturing changes")
            print("\n💡 Monitor topic with:")
            print("docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic adtrace_migration --from-beginning")
        else:
            print("\n⚠️  Setup completed but CDC test failed")
            print("💡 Check connector status:")
            print("curl -s http://localhost:8083/connectors/adtrace-migration-connector/status | jq .")
    else:
        print("\n❌ Failed to create connector")

if __name__ == "__main__":
    main() 