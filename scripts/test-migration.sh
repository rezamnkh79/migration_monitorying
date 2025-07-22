#!/bin/bash

echo "🧪 Testing MySQL to PostgreSQL Migration Setup..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local status=$1
    local message=$2
    case $status in
        "SUCCESS") echo -e "${GREEN}✅ $message${NC}" ;;
        "ERROR") echo -e "${RED}❌ $message${NC}" ;;
        "WARNING") echo -e "${YELLOW}⚠️  $message${NC}" ;;
        "INFO") echo -e "${BLUE}ℹ️  $message${NC}" ;;
    esac
}

# Test MySQL connection
print_status "INFO" "Testing MySQL connection..."
if docker exec mysql mysql -uroot -pdebezium -e "SELECT 1;" >/dev/null 2>&1; then
    print_status "SUCCESS" "MySQL is accessible"
else
    print_status "ERROR" "MySQL connection failed"
    exit 1
fi

# Test PostgreSQL connection
print_status "INFO" "Testing PostgreSQL connection..."
if docker exec postgres psql -U postgres -d inventory -c "SELECT 1;" >/dev/null 2>&1; then
    print_status "SUCCESS" "PostgreSQL is accessible"
else
    print_status "ERROR" "PostgreSQL connection failed"
    exit 1
fi

# Test Redis connection
print_status "INFO" "Testing Redis connection..."
if docker exec redis redis-cli ping >/dev/null 2>&1; then
    print_status "SUCCESS" "Redis is accessible"
else
    print_status "ERROR" "Redis connection failed"
    exit 1
fi

# Test Kafka connection
print_status "INFO" "Testing Kafka connection..."
if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    print_status "SUCCESS" "Kafka is accessible"
else
    print_status "ERROR" "Kafka connection failed"
    exit 1
fi

# Test Kafka Connect
print_status "INFO" "Testing Kafka Connect..."
if curl -s http://localhost:8083/ >/dev/null 2>&1; then
    print_status "SUCCESS" "Kafka Connect is running"
else
    print_status "ERROR" "Kafka Connect is not accessible"
    exit 1
fi

# Test Data Validator service
print_status "INFO" "Testing Data Validator service..."
if curl -s http://localhost:8000/health >/dev/null 2>&1; then
    print_status "SUCCESS" "Data Validator service is running"
else
    print_status "WARNING" "Data Validator service is not accessible yet"
fi

# Test Monitoring Dashboard
print_status "INFO" "Testing Monitoring Dashboard..."
if curl -s http://localhost:3000/ >/dev/null 2>&1; then
    print_status "SUCCESS" "Monitoring Dashboard is running"
else
    print_status "WARNING" "Monitoring Dashboard is not accessible yet"
fi

# Test sample data counts (using dynamic discovery instead of hardcoded tables)
print_status "INFO" "Testing dynamic table counts..."

# Get table list dynamically from data validator API
dynamic_tables=$(curl -s http://localhost:8000/discover-tables 2>/dev/null | python3 -c "
import json
import sys
try:
    data = json.load(sys.stdin)
    tables = data.get('mysql_tables', {}).get('monitorable', [])[:5]  # Test first 5 tables
    print(' '.join(tables))
except:
    print('')
" 2>/dev/null)

if [ -n "$dynamic_tables" ]; then
    print_status "SUCCESS" "Found dynamic tables: $dynamic_tables"
    
    # Test a few sample tables
    for table in $dynamic_tables; do
        # Skip system tables for testing
        if [[ "$table" == *"django"* ]] || [[ "$table" == *"auth"* ]]; then
            continue
        fi
        
        mysql_count=$(docker exec mysql mysql -h ${MYSQL_HOST:-mysql} -P ${MYSQL_PORT:-3306} -u ${MYSQL_USER:-debezium} -p${MYSQL_PASSWORD:-dbz} -D ${MYSQL_DATABASE:-inventory} -se "SELECT COUNT(*) FROM \`$table\`;" 2>/dev/null || echo "0")
        postgres_count=$(docker exec postgres psql -U ${POSTGRES_USER:-postgres} -d ${POSTGRES_DATABASE:-inventory} -t -c "SELECT COUNT(*) FROM \"$table\";" 2>/dev/null | tr -d ' ' || echo "0")
        
        print_status "INFO" "Table $table: MySQL=$mysql_count, PostgreSQL=$postgres_count"
        break  # Test just one table for now
    done
else
    print_status "WARNING" "Could not get dynamic table list - using basic connectivity test"
fi

# Insert test data
print_status "INFO" "Inserting test data into MySQL..."
docker exec mysql mysql -uroot -pdebezium -D inventory -e "
INSERT INTO users (username, email, full_name, metadata) VALUES 
('test_user_$(date +%s)', 'test$(date +%s)@example.com', 'Test User', '{\"test\": true}');
"

print_status "INFO" "Test data inserted. Check Kafka topics for CDC events:"
echo "docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic dbserver1.inventory.users --from-beginning"

echo ""
print_status "SUCCESS" "Migration setup test completed!"
echo ""
print_status "INFO" "Access Points:"
echo "  📊 Monitoring Dashboard: http://localhost:3000"
echo "  🔍 Data Validator API:   http://localhost:8000"
echo "  📡 Kafka UI:            http://localhost:8080"
echo "  🔗 Kafka Connect:       http://localhost:8083"
echo ""
print_status "INFO" "Next Steps:"
echo "  1. Run: ./scripts/setup-debezium.sh"
echo "  2. Monitor the dashboard for real-time sync status"
echo "  3. Insert/update data in MySQL to see CDC in action" 