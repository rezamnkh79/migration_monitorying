#!/usr/bin/env python3
import requests
import json
import os
import time

print("🚀 Creating final CDC test connector...")

# Configuration
mysql_host = '46.245.77.98'
mysql_user = 'root'
mysql_password = 'mauFJcuf5dhRMQrjj'
database_name = 'adtrace_db_stage'
timestamp = str(int(time.time()))

connector_config = {
    "name": f"mysql-test-{timestamp}",
    "config": {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "tasks.max": "1",
        "database.hostname": mysql_host,
        "database.port": "3306",
        "database.user": mysql_user,
        "database.password": mysql_password,
        "database.server.id": timestamp,
        "database.server.name": f"mysql_{timestamp}",
        "database.include.list": database_name,
        "table.include.list": f"{database_name}.buy_transaction",
        "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
        "schema.history.internal.kafka.topic": f"schema-{timestamp}",
        "topic.prefix": "test",
        "snapshot.mode": "when_needed",
        "snapshot.locking.mode": "none",
        "transforms": "route",
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.route.regex": "([^.]+)\\.([^.]+)\\.([^.]+)",
        "transforms.route.replacement": "$3",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        "decimal.handling.mode": "string"
    }
}

try:
    # Clean existing connectors
    resp = requests.get("http://localhost:8083/connectors")
    if resp.status_code == 200:
        for conn in resp.json():
            print(f"🗑️ Deleting: {conn}")
            requests.delete(f"http://localhost:8083/connectors/{conn}")
    
    time.sleep(2)
    
    # Create connector
    print(f"🔧 Creating: {connector_config['name']}")
    response = requests.post(
        "http://localhost:8083/connectors",
        headers={"Content-Type": "application/json"},
        json=connector_config
    )
    
    if response.status_code in [200, 201]:
        print("✅ Created successfully")
        time.sleep(15)
        
        # Check status
        status_resp = requests.get(f"http://localhost:8083/connectors/{connector_config['name']}/status")
        if status_resp.status_code == 200:
            status = status_resp.json()
            connector_state = status.get("connector", {}).get("state")
            task_state = status.get("tasks", [{}])[0].get("state")
            
            print(f"📊 Connector: {connector_state}")
            print(f"📋 Task: {task_state}")
            
            if task_state == "RUNNING":
                print("🎉 SUCCESS! Ready to test!")
                print("💡 Now run:")
                print(f"   mysql -h {mysql_host} -u {mysql_user} -p{mysql_password} {database_name}")
                print("   INSERT INTO buy_transaction (account_id, user_id, wallet_id, amount, creation_time, last_update_time, is_deleted) VALUES ('999', '999', '1', '999999', NOW(), NOW(), '0');")
            else:
                print(f"❌ Task failed: {status.get('tasks', [{}])[0].get('trace', '')[:200]}")
    else:
        print(f"❌ Failed: {response.text}")

except Exception as e:
    print(f"❌ Error: {e}") 