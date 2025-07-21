#!/bin/bash

echo "🚀 Setting up CDC for Remote MySQL Server..."

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Display configuration
echo "📋 Configuration:"
echo "   MySQL Host: ${MYSQL_HOST}"
echo "   MySQL Port: ${MYSQL_PORT}"
echo "   MySQL User: ${MYSQL_USER}"
echo "   MySQL Database: ${MYSQL_DATABASE}"
echo ""

# Test MySQL connection first
echo "🔍 Testing MySQL connection..."
if command -v mysql &> /dev/null; then
    mysql -h ${MYSQL_HOST} -P ${MYSQL_PORT} -u ${MYSQL_USER} -p${MYSQL_PASSWORD} -e "SELECT 1;" 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "✅ MySQL connection successful"
    else
        echo "❌ MySQL connection failed. Please check your credentials."
        exit 1
    fi
else
    echo "⚠️  MySQL client not found locally. Proceeding with setup..."
fi

# Check if binlog is enabled on remote MySQL
echo "🔍 Checking MySQL binlog configuration..."
if command -v mysql &> /dev/null; then
    BINLOG_STATUS=$(mysql -h ${MYSQL_HOST} -P ${MYSQL_PORT} -u ${MYSQL_USER} -p${MYSQL_PASSWORD} -se "SHOW VARIABLES LIKE 'log_bin';" 2>/dev/null | cut -f2)
    if [ "$BINLOG_STATUS" = "ON" ]; then
        echo "✅ Binary logging is enabled"
    else
        echo "❌ Binary logging is NOT enabled. Please enable it on your MySQL server:"
        echo "   Add to MySQL config:"
        echo "   log-bin=mysql-bin"
        echo "   binlog-format=ROW"
        echo "   binlog-row-image=FULL"
        echo "   server-id=<unique_id>"
        exit 1
    fi
fi

# Start the services (excluding MySQL)
echo "🚀 Starting services..."
docker-compose up -d zookeeper kafka connect redis postgres data-validator monitoring-dashboard kafka-ui

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 30

# Check if Kafka Connect is ready
echo "📡 Checking Kafka Connect status..."
until curl -s http://localhost:8083/connectors > /dev/null; do
    echo "Waiting for Kafka Connect..."
    sleep 5
done

echo "✅ Kafka Connect is ready!"

# Setup dynamic CDC connector via API
echo "🔧 Setting up dynamic CDC connector..."
curl -X POST http://localhost:8000/table-monitor/setup -H "Content-Type: application/json"

if [ $? -eq 0 ]; then
    echo "✅ Dynamic CDC setup initiated successfully!"
else
    echo "❌ Failed to setup dynamic CDC"
    exit 1
fi

# Wait a bit for connector to initialize
echo "⏳ Waiting for connector to initialize..."
sleep 15

# Check connector status
echo "📊 Checking connector status..."
curl -s http://localhost:8000/table-monitor/status | jq '.' || echo "Status check completed"

echo ""
echo "🎯 Remote MySQL CDC setup complete!"
echo "💡 You can monitor the system at:"
echo "   - Dashboard: http://localhost:3000"
echo "   - API: http://localhost:8000"
echo "   - Kafka UI: http://localhost:8080"
echo ""
echo "📝 To test CDC, run some INSERT/UPDATE/DELETE operations on your remote MySQL:"
echo "   mysql -h ${MYSQL_HOST} -P ${MYSQL_PORT} -u ${MYSQL_USER} -p${MYSQL_PASSWORD} ${MYSQL_DATABASE}"
echo ""
echo "🔄 You should see events appearing in the dashboard within seconds!" 