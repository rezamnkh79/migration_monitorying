#!/usr/bin/env python3
import requests
import json
import os
import hashlib

# Configuration from environment
mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
mysql_port = os.getenv('MYSQL_PORT', '3306')
mysql_user = os.getenv('MYSQL_USER', 'root')
mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
database_name = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')

# Generate unique server ID
server_id = str(abs(hash(mysql_host)) % 10000000)

# Focus on just buy_transaction table for testing
tables_to_monitor = ['buy_transaction']

table_include_list = [f"{database_name}.{table}" for table in tables_to_monitor]

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
        "database.server.name": f"mysql_server_{server_id}",
        "database.include.list": database_name,
        "table.include.list": ",".join(table_include_list),
        "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
        "schema.history.internal.kafka.topic": f"schema-history-{server_id}",
        "include.schema.changes": "true",
        "topic.prefix": "adtrace",
        # Use initial snapshot to get existing data and then monitor changes
        "snapshot.mode": "initial", 
        "snapshot.locking.mode": "minimal",
        # Transform settings
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
        "max.batch.size": "2048",
        "max.queue.size": "8192",
        # Binary handling
        "binary.handling.mode": "base64"
    }
}

print(f"🔧 Creating MySQL connector for {mysql_host}:{mysql_port}/{database_name}")
print(f"📊 Tables to monitor: {tables_to_monitor}")
print(f"🆔 Server ID: {server_id}")
print("📝 Snapshot mode: SCHEMA_ONLY (capture schemas then monitor new changes)")

try:
    response = requests.post(
        "http://localhost:8083/connectors",
        headers={"Content-Type": "application/json"},
        json=connector_config,
        timeout=30
    )
    
    if response.status_code in [200, 201]:
        print("✅ MySQL source connector created successfully")
        print("⏳ Waiting 15 seconds for initialization...")
        
        import time
        time.sleep(15)
        
        # Check status
        status_response = requests.get("http://localhost:8083/connectors/dynamic-mysql-source/status")
        if status_response.status_code == 200:
            status_data = status_response.json()
            connector_state = status_data.get("connector", {}).get("state", "unknown")
            tasks = status_data.get("tasks", [])
            
            print(f"📊 Connector State: {connector_state}")
            for i, task in enumerate(tasks):
                task_state = task.get("state", "unknown")
                print(f"📋 Task {i}: {task_state}")
                
                if task_state == "FAILED":
                    print(f"❌ Task failed: {task.get('trace', 'No trace')[:300]}...")
                elif task_state == "RUNNING":
                    print("✅ Connector is running and ready to capture changes!")
                    print("🎯 Now you can test by inserting data into buy_transaction table")
        
    else:
        print(f"❌ Failed to create connector: {response.status_code}")
        print(response.text)
        
except Exception as e:
    print(f"❌ Error creating connector: {str(e)}") 