#!/usr/bin/env python3
import requests
import json
import os
import time
import hashlib

def main():
    print("🔧 Final CDC Fix with Schema Recovery...")
    
    # Configuration from environment
    mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
    mysql_port = os.getenv('MYSQL_PORT', '3306')
    mysql_user = os.getenv('MYSQL_USER', 'root')
    mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
    database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
    
    # Generate unique server ID
    server_id = str(abs(hash(mysql_host)) % 10000000)
    
    print(f"📋 Configuration:")
    print(f"   MySQL Host: {mysql_host}")
    print(f"   MySQL Database: {database_name}")
    print(f"   Server ID: {server_id}")
    
    # Step 1: Delete any existing connector
    print("\n🗑️ Cleaning up existing connectors...")
    try:
        response = requests.delete("http://localhost:8083/connectors/dynamic-mysql-source", timeout=10)
        print(f"   Deleted old connector: {response.status_code}")
    except:
        print("   No existing connector found")
    
    time.sleep(2)
    
    # Step 2: Use schema recovery mode to bypass history issues
    key_tables = ['buy_transaction', 'user_message', 'email']  # Start with fewer tables
    table_include_list = [f"{database_name}.{table}" for table in key_tables]
    
    print(f"\n📡 Creating recovery connector for tables: {key_tables}")
    
    connector_config = {
        "name": "dynamic-mysql-source",
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "1",
            "database.hostname": mysql_host,
            "database.port": mysql_port,
            "database.user": mysql_user,
            "database.password": mysql_password,
            "database.server.id": server_id,
            "database.server.name": f"adtrace_mysql_{server_id}",
            "database.include.list": database_name,
            "table.include.list": ",".join(table_include_list),
            
            # Schema history configuration with recovery
            "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
            "schema.history.internal.kafka.topic": f"schema-history-recovery-{server_id}",
            "include.schema.changes": "true",
            
            # Topic configuration
            "topic.prefix": "adtrace",
            
            # CRITICAL: Use schema recovery to bypass missing history
            "snapshot.mode": "schema_only_recovery",  # This will recover from missing schema history
            "snapshot.locking.mode": "none",
            
            # Transform settings to get clean topic names
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
            "max.batch.size": "1024",
            "max.queue.size": "4096",
            
            # Binary handling
            "binary.handling.mode": "base64",
            
            # Error handling
            "errors.tolerance": "all",
            "errors.log.enable": "true",
            "errors.log.include.messages": "true"
        }
    }
    
    print("📊 Recovery Connector Configuration:")
    print(f"   Server Name: adtrace_mysql_{server_id}")
    print(f"   Tables: {table_include_list}")
    print(f"   Schema History Topic: schema-history-recovery-{server_id}")
    print(f"   Snapshot Mode: schema_only_recovery")
    print(f"   Error Tolerance: all")
    
    try:
        response = requests.post(
            "http://localhost:8083/connectors",
            headers={"Content-Type": "application/json"},
            json=connector_config,
            timeout=30
        )
        
        if response.status_code in [200, 201]:
            print("✅ Recovery connector created successfully!")
        else:
            print(f"❌ Failed to create connector: {response.status_code}")
            print(f"   Error: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error creating connector: {str(e)}")
        return False
    
    # Step 3: Wait and check connector status multiple times
    print("\n⏳ Waiting for connector to recover and initialize...")
    
    for attempt in range(3):
        time.sleep(15)  # Wait longer for recovery
        
        try:
            response = requests.get("http://localhost:8083/connectors/dynamic-mysql-source/status", timeout=10)
            status = response.json()
            
            print(f"\n📊 Connector Status (Attempt {attempt + 1}):")
            print(f"   Connector State: {status['connector']['state']}")
            
            if status['tasks']:
                task_state = status['tasks'][0]['state']
                print(f"   Task State: {task_state}")
                
                if task_state == "RUNNING":
                    print("✅ Connector is now running successfully!")
                    break
                elif task_state == "FAILED":
                    error_msg = status['tasks'][0].get('trace', 'Unknown error')
                    print(f"   Error: {error_msg[:200]}...")
                    
                    if attempt < 2:
                        print("   Retrying...")
                        continue
                    else:
                        print("❌ Connector failed after all attempts")
                        return False
            else:
                print("   No tasks found")
                
        except Exception as e:
            print(f"❌ Error checking connector status: {str(e)}")
            if attempt == 2:
                return False
    
    # Step 4: Test the system
    print("\n🧪 Testing CDC System...")
    
    # Check if we can get metrics
    try:
        response = requests.get("http://localhost:8000/metrics", timeout=10)
        if response.status_code == 200:
            data = response.json()
            cdc_events = data.get('cdc_stats', {}).get('events_processed', 0)
            print(f"   Current CDC events processed: {cdc_events}")
        else:
            print(f"   Could not get metrics: {response.status_code}")
    except Exception as e:
        print(f"   Error getting metrics: {str(e)}")
    
    print("\n🎯 Recovery Summary:")
    print("   1. ✅ Used schema_only_recovery mode")
    print("   2. ✅ Focused on key tables only")  
    print("   3. ✅ Added error tolerance")
    print("   4. ✅ Created new schema history topic")
    
    print("\n📝 Test CDC Now:")
    print("   1. Insert test data in MySQL:")
    print(f"      mysql -h {mysql_host} -u {mysql_user} -p{mysql_password} {database_name}")
    print("      INSERT INTO buy_transaction (user_id, amount, created_at) VALUES (999, 100, NOW());")
    print("")
    print("   2. Check for CDC events:")
    print("      curl http://localhost:8000/metrics")
    print("")
    print("   3. Monitor topics:")
    print("      docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic buy_transaction --from-beginning")
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎉 CDC system should now be working!")
        print("Try inserting/updating/deleting data to test CDC functionality.")
    else:
        print("\n❌ CDC fix failed. Check logs for more details.") 