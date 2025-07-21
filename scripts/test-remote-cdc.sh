#!/bin/bash

echo "🧪 Testing Remote MySQL CDC Functionality..."

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

echo "📋 Configuration:"
echo "   MySQL Host: ${MYSQL_HOST}"
echo "   MySQL Database: ${MYSQL_DATABASE}"
echo "   Testing table: users (you can change this)"
echo ""

# Function to run MySQL command
run_mysql_command() {
    mysql -h ${MYSQL_HOST} -P ${MYSQL_PORT} -u ${MYSQL_USER} -p${MYSQL_PASSWORD} ${MYSQL_DATABASE} -e "$1" 2>/dev/null
}

# Function to check CDC events
check_cdc_events() {
    echo "📊 Checking CDC events..."
    curl -s http://localhost:8000/metrics | jq '.cdc_events_processed' || echo "API not responding"
}

# Test 1: Check initial state
echo "🔍 Step 1: Checking initial CDC state..."
INITIAL_EVENTS=$(curl -s http://localhost:8000/metrics | jq '.cdc_events_processed' 2>/dev/null || echo "0")
echo "   Initial CDC events: ${INITIAL_EVENTS}"

# Test 2: Insert test record
echo ""
echo "➕ Step 2: Inserting test record..."
TEST_TIMESTAMP=$(date +%s)
TEST_EMAIL="test_${TEST_TIMESTAMP}@example.com"

INSERT_SQL="INSERT INTO users (username, email, full_name, created_at) VALUES ('test_user_${TEST_TIMESTAMP}', '${TEST_EMAIL}', 'Test User ${TEST_TIMESTAMP}', NOW());"

if run_mysql_command "${INSERT_SQL}"; then
    echo "   ✅ Test record inserted successfully"
    INSERTED_ID=$(run_mysql_command "SELECT LAST_INSERT_ID();" | tail -n 1)
    echo "   📝 Inserted record ID: ${INSERTED_ID}"
else
    echo "   ❌ Failed to insert test record"
    echo "   💡 Make sure 'users' table exists with columns: username, email, full_name, created_at"
    exit 1
fi

# Wait for CDC to process
echo ""
echo "⏳ Step 3: Waiting for CDC to process the event..."
sleep 5

# Test 3: Check CDC events after insert
echo ""
echo "📊 Step 4: Checking CDC events after INSERT..."
AFTER_INSERT_EVENTS=$(curl -s http://localhost:8000/metrics | jq '.cdc_events_processed' 2>/dev/null || echo "0")
echo "   CDC events after INSERT: ${AFTER_INSERT_EVENTS}"

if [ "${AFTER_INSERT_EVENTS}" -gt "${INITIAL_EVENTS}" ]; then
    echo "   ✅ CDC detected INSERT operation!"
else
    echo "   ❌ CDC did not detect INSERT operation"
    echo "   🔍 Let's check connector status..."
    curl -s http://localhost:8083/connectors/dynamic-mysql-source/status | jq '.connector.state' || echo "Connector not found"
fi

# Test 4: Update test record
echo ""
echo "🔄 Step 5: Updating test record..."
UPDATE_SQL="UPDATE users SET full_name = 'Updated Test User ${TEST_TIMESTAMP}' WHERE email = '${TEST_EMAIL}';"

if run_mysql_command "${UPDATE_SQL}"; then
    echo "   ✅ Test record updated successfully"
else
    echo "   ❌ Failed to update test record"
fi

# Wait for CDC to process
echo ""
echo "⏳ Step 6: Waiting for CDC to process UPDATE..."
sleep 5

# Test 5: Check CDC events after update
echo ""
echo "📊 Step 7: Checking CDC events after UPDATE..."
AFTER_UPDATE_EVENTS=$(curl -s http://localhost:8000/metrics | jq '.cdc_events_processed' 2>/dev/null || echo "0")
echo "   CDC events after UPDATE: ${AFTER_UPDATE_EVENTS}"

if [ "${AFTER_UPDATE_EVENTS}" -gt "${AFTER_INSERT_EVENTS}" ]; then
    echo "   ✅ CDC detected UPDATE operation!"
else
    echo "   ❌ CDC did not detect UPDATE operation"
fi

# Test 6: Delete test record
echo ""
echo "🗑️ Step 8: Deleting test record..."
DELETE_SQL="DELETE FROM users WHERE email = '${TEST_EMAIL}';"

if run_mysql_command "${DELETE_SQL}"; then
    echo "   ✅ Test record deleted successfully"
else
    echo "   ❌ Failed to delete test record"
fi

# Wait for CDC to process
echo ""
echo "⏳ Step 9: Waiting for CDC to process DELETE..."
sleep 5

# Test 7: Check final CDC events
echo ""
echo "📊 Step 10: Checking final CDC events..."
FINAL_EVENTS=$(curl -s http://localhost:8000/metrics | jq '.cdc_events_processed' 2>/dev/null || echo "0")
echo "   Final CDC events: ${FINAL_EVENTS}"

# Summary
echo ""
echo "📋 TEST SUMMARY:"
echo "=================="
echo "   Initial events: ${INITIAL_EVENTS}"
echo "   After INSERT:   ${AFTER_INSERT_EVENTS}"
echo "   After UPDATE:   ${AFTER_UPDATE_EVENTS}"
echo "   Final events:   ${FINAL_EVENTS}"
echo ""

TOTAL_NEW_EVENTS=$((FINAL_EVENTS - INITIAL_EVENTS))
echo "   Total new events detected: ${TOTAL_NEW_EVENTS}"

if [ "${TOTAL_NEW_EVENTS}" -ge 3 ]; then
    echo "   ✅ CDC is working perfectly! All operations detected."
elif [ "${TOTAL_NEW_EVENTS}" -ge 1 ]; then
    echo "   ⚠️  CDC is partially working. Some operations detected."
else
    echo "   ❌ CDC is not working. No operations detected."
    echo ""
    echo "🔍 TROUBLESHOOTING:"
    echo "   1. Check connector status: curl http://localhost:8083/connectors/dynamic-mysql-source/status"
    echo "   2. Check connector logs: docker logs connect"
    echo "   3. Verify MySQL binlog: SHOW VARIABLES LIKE 'log_bin';"
    echo "   4. Check table permissions: SHOW GRANTS FOR '${MYSQL_USER}'@'%';"
fi

echo ""
echo "🔗 Useful Links:"
echo "   - Dashboard: http://localhost:3000"
echo "   - API Status: http://localhost:8000/table-monitor/status"
echo "   - Kafka UI: http://localhost:8080" 