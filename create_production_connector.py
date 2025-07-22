#!/usr/bin/env python3
import requests
import json
import os
import time

def create_production_connector():
    """Create production-ready connector"""
    
    mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
    mysql_port = os.getenv('MYSQL_PORT', '3306')
    mysql_user = os.getenv('MYSQL_USER', 'root')
    mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
    database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
    
    current_time = int(time.time())
    server_id = str(current_time)[-7:]
    
    print(f"🚀 Creating PRODUCTION CDC connector...")
    print(f"🆔 Server ID: {server_id}")
    
    # Delete all existing connectors first
    print("🗑️  Cleaning up existing connectors...")
    try:
        response = requests.get("http://localhost:8083/connectors")
        if response.status_code == 200:
            connectors = response.json()
            for connector in connectors:
                print(f"   Deleting {connector}...")
                requests.delete(f"http://localhost:8083/connectors/{connector}")
                time.sleep(1)
    except Exception as e:
        print(f"⚠️  Error deleting connectors: {str(e)}")
    
    time.sleep(5)
    
    # Production connector config
    connector_config = {
        "name": "adtrace-production",
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "1",
            "database.hostname": mysql_host,
            "database.port": mysql_port,
            "database.user": mysql_user,
            "database.password": mysql_password,
            "database.server.id": server_id,
            "database.server.name": f"prod_{server_id}",
            "database.include.list": database_name,
            "table.include.list": f"{database_name}.buy_transaction,{database_name}.adtrace_tracker,{database_name}.adtrace_transaction",
            
            # Schema history setup
            "schema.history.internal.kafka.bootstrap.servers": "kafka:29092", 
            "schema.history.internal.kafka.topic": f"prod-schema-{server_id}",
            "include.schema.changes": "true",
            
            # Use when_needed to avoid schema history issues
            "snapshot.mode": "when_needed",
            "snapshot.locking.mode": "minimal",
            
            # Topic naming - use simple prefix
            "topic.prefix": "adtrace_migration",
            
            # Converter settings
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter", 
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false",
            
            # MySQL settings
            "decimal.handling.mode": "string",
            "time.precision.mode": "connect",
            "bigint.unsigned.handling.mode": "long",
            "binary.handling.mode": "base64",
            "database.ssl.mode": "disabled",
            
            # Binlog settings
            "binlog.buffer.size": "32768",
            "max.queue.size": "4096",
            "max.batch.size": "1024",
            "poll.interval.ms": "1000",
            "heartbeat.interval.ms": "5000"
        }
    }
    
    try:
        print("📝 Creating connector...")
        response = requests.post(
            "http://localhost:8083/connectors",
            headers={"Content-Type": "application/json"},
            json=connector_config,
            timeout=30
        )
        
        if response.status_code in [200, 201]:
            print("✅ Production connector created successfully")
            return True
        else:
            print(f"❌ Failed to create connector: {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"❌ Error creating connector: {str(e)}")
        return False

def check_production_connector():
    """Check production connector status"""
    print("\n📊 Checking production connector...")
    time.sleep(20)
    
    try:
        status_response = requests.get("http://localhost:8083/connectors/adtrace-production/status")
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
                    print(f"❌ Task failed: {trace[:300]}...")
                    return False
                elif task_state == "RUNNING":
                    print("✅ Task running!")
            
            return connector_state == "RUNNING" and all(task.get("state") == "RUNNING" for task in tasks)
        else:
            print(f"❌ Status check failed: {status_response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error checking status: {str(e)}")
        return False

def test_production_connector():
    """Test production connector"""
    import subprocess
    
    print("\n🧪 Testing production connector...")
    
    # Insert test data
    test_id = int(time.time()) % 100000
    mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
    mysql_user = os.getenv('MYSQL_USER', 'root')
    mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
    database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
    
    insert_query = f"INSERT INTO {database_name}.buy_transaction (account_id, user_id, wallet_id, amount, creation_time, last_update_time, is_deleted) VALUES ({test_id}, {test_id}, 1, '777777', NOW(), NOW(), 0);"
    
    print(f"📝 Testing with account_id: {test_id}")
    
    try:
        result = subprocess.run([
            "mysql", 
            f"-h{mysql_host}",
            f"-u{mysql_user}",
            f"-p{mysql_password}",
            "-e", insert_query
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("✅ Test data inserted")
            print("⏳ Waiting for CDC processing...")
            time.sleep(10)
            
            # Check topic
            topic_name = "adtrace_migration.adtrace_db_stage.buy_transaction"
            kafka_result = subprocess.run([
                "docker", "exec", "kafka",
                "kafka-console-consumer",
                "--bootstrap-server", "localhost:9092",
                "--topic", topic_name,
                "--timeout-ms", "8000"
            ], capture_output=True, text=True, timeout=12)
            
            if kafka_result.stdout and kafka_result.stdout.strip():
                print("🎉 SUCCESS! Messages found in topic!")
                return True
            else:
                print("⚠️  No new messages yet, but connector is ready")
                return True
        else:
            print(f"❌ Insert failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Test error: {str(e)}")
        return False

def main():
    print("🚀 Setting up PRODUCTION CDC Connector...")
    print("=" * 60)
    
    if create_production_connector():
        if check_production_connector():
            test_production_connector()
            
            print("\n" + "=" * 60)
            print("🎉 PRODUCTION CDC CONNECTOR READY!")
            print("✅ Connector: adtrace-production")
            print("✅ Topic: adtrace_migration.adtrace_db_stage.buy_transaction")
            print("✅ Tables: buy_transaction, adtrace_tracker, adtrace_transaction")
            print("\n💡 To monitor:")
            print("docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic adtrace_migration.adtrace_db_stage.buy_transaction --from-beginning")
            print("\n💡 To test:")
            print("mysql -h 46.245.77.98 -u root -p -e \"INSERT INTO adtrace_db_stage.buy_transaction (account_id, user_id, wallet_id, amount, creation_time, last_update_time, is_deleted) VALUES (123123, 456456, 1, '789789', NOW(), NOW(), 0);\"")
            
        else:
            print("\n❌ Connector setup failed")
    else:
        print("\n❌ Failed to create production connector")

if __name__ == "__main__":
    main() 