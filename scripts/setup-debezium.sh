#!/bin/bash

echo "🔧 Setting up Debezium MySQL Source Connector..."

# Wait for Kafka Connect to be ready
echo "⏳ Waiting for Kafka Connect to be ready..."
until curl -f http://localhost:8083/; do
    echo "Kafka Connect is not ready yet. Waiting 10 seconds..."
    sleep 10
done

echo "✅ Kafka Connect is ready!"

# Create MySQL Source Connector with dynamic table discovery
echo "📡 Creating Dynamic MySQL Source Connector..."
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
localhost:8083/connectors/ -d '{
  "name": "mysql-dynamic-source-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "'${MYSQL_HOST:-mysql}'",
    "database.port": "'${MYSQL_PORT:-3306}'",
    "database.user": "'${MYSQL_USER:-debezium}'",
    "database.password": "'${MYSQL_PASSWORD:-dbz}'",
    "database.server.id": "184054",
    "topic.prefix": "adtrace_dynamic",
    "database.include.list": "'${MYSQL_DATABASE:-inventory}'",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
    "schema.history.internal.kafka.topic": "schema-history.adtrace",
    "include.schema.changes": "true",
    "snapshot.mode": "when_needed",
    "transforms": "route",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "([^.]+)\\.([^.]+)\\.([^.]+)",
    "transforms.route.replacement": "$3",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  }
}'

echo ""
echo "⏳ Waiting a few seconds for connector to initialize..."
sleep 5

# Check connector status
echo "📊 Checking connector status..."
curl -s http://localhost:8083/connectors/mysql-dynamic-source-connector/status

echo ""
echo "📋 Listing all connectors..."
curl -s http://localhost:8083/connectors

echo ""
echo "🎯 Checking topics..."
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list

echo ""
echo "✅ Debezium dynamic setup completed!"
echo "📖 You can monitor the connector at: http://localhost:8083/connectors/mysql-dynamic-source-connector/status"
echo "📊 Kafka UI is available at: http://localhost:8080" 