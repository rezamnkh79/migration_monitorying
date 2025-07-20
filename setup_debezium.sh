#!/bin/bash

echo "🚀 Setting up Debezium CDC Migration System..."

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 30

# Check if Kafka Connect is ready
echo "📡 Checking Kafka Connect status..."
until curl -s http://localhost:8083/connectors; do
    echo "Waiting for Kafka Connect..."
    sleep 5
done

echo "✅ Kafka Connect is ready!"

# Setup MySQL Source Connector
echo "🔧 Setting up MySQL Source Connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/mysql-source-connector.json

if [ $? -eq 0 ]; then
    echo "✅ MySQL Source Connector created successfully!"
else
    echo "❌ Failed to create MySQL Source Connector"
fi

# Wait a bit before setting up sink
sleep 10

# Setup PostgreSQL Sink Connector  
echo "🔧 Setting up PostgreSQL Sink Connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/postgres-sink-connector.json

if [ $? -eq 0 ]; then
    echo "✅ PostgreSQL Sink Connector created successfully!"
else
    echo "❌ Failed to create PostgreSQL Sink Connector"
fi

# Check connector status
echo "📊 Checking connector status..."
curl -s http://localhost:8083/connectors/mysql-source-connector/status | jq '.'
curl -s http://localhost:8083/connectors/postgres-sink-connector/status | jq '.'

echo "🎯 Debezium CDC Migration System setup complete!"
echo "💡 You can monitor the connectors at:"
echo "   - Kafka UI: http://localhost:8080"
echo "   - Dashboard: http://localhost:3000"
echo "   - API: http://localhost:8000" 