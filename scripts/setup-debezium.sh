#!/bin/bash

echo "🔧 Setting up Debezium MySQL Source Connector..."

# Wait for Kafka Connect to be ready
echo "⏳ Waiting for Kafka Connect to be ready..."
until curl -f http://localhost:8083/; do
    echo "Kafka Connect is not ready yet. Waiting 10 seconds..."
    sleep 10
done

echo "✅ Kafka Connect is ready!"

# Create MySQL Source Connector
echo "📡 Creating MySQL Source Connector..."
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
localhost:8083/connectors/ -d '{
  "name": "mysql-source-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "dbz",
    "database.server.id": "184054",
    "topic.prefix": "dbserver1",
    "database.include.list": "inventory",
    "database.history.kafka.bootstrap.servers": "kafka:29092",
    "database.history.kafka.topic": "dbhistory.inventory",
    "include.schema.changes": "true",
    "table.include.list": "inventory.users,inventory.products,inventory.orders,inventory.order_items"
  }
}'

echo ""
echo "⏳ Waiting a few seconds for connector to initialize..."
sleep 5

# Check connector status
echo "📊 Checking connector status..."
curl -s http://localhost:8083/connectors/mysql-source-connector/status

echo ""
echo "📋 Listing all connectors..."
curl -s http://localhost:8083/connectors

echo ""
echo "🎯 Checking topics..."
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list

echo ""
echo "✅ Debezium setup completed!"
echo "📖 You can monitor the connector at: http://localhost:8083/connectors/mysql-source-connector/status"
echo "📊 Kafka UI is available at: http://localhost:8080" 