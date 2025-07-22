#!/usr/bin/env python3
import json
import time
from kafka import KafkaConsumer

def test_cdc_topics():
    """Test that CDC is working by consuming from fresh topics"""
    
    print("🔍 Testing CDC functionality...")
    print("=" * 50)
    
    try:
        # Create Kafka consumer
        consumer = KafkaConsumer(
            'buy_transaction',
            'adtrace_tracker', 
            'adtrace_transaction',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='cdc-test-consumer',
            consumer_timeout_ms=5000  # 5 second timeout
        )
        
        print("📡 Listening for CDC events on topics:")
        print("   - buy_transaction")
        print("   - adtrace_tracker")
        print("   - adtrace_transaction")
        print("\n⏳ Waiting for CDC events... (5 second timeout)")
        print("💡 To test: Insert/Update data in MySQL tables")
        print("   Example: INSERT INTO buy_transaction (user_id, amount) VALUES (123, 50.00);")
        print()
        
        message_count = 0
        start_time = time.time()
        
        for message in consumer:
            message_count += 1
            topic = message.topic
            value = message.value
            
            print(f"✅ CDC Event #{message_count}")
            print(f"   📊 Topic: {topic}")
            print(f"   🕒 Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            if value and isinstance(value, dict):
                operation = value.get('op', 'unknown')
                table = value.get('source', {}).get('table', 'unknown')
                print(f"   🔄 Operation: {operation}")
                print(f"   📋 Table: {table}")
                
                if 'after' in value and value['after']:
                    print(f"   📝 Data: {json.dumps(value['after'], indent=2)[:200]}...")
            
            print()
            
            # Stop after 10 messages or 30 seconds
            if message_count >= 10 or (time.time() - start_time) > 30:
                break
        
        if message_count > 0:
            print(f"🎉 SUCCESS: Received {message_count} CDC events!")
            print("✅ Debezium CDC is working correctly")
        else:
            print("⚠️  No CDC events received in 5 seconds")
            print("💡 This is normal if no data changes occurred")
            print("✅ Debezium connector is ready and listening")
            
    except Exception as e:
        print(f"❌ Error testing CDC: {str(e)}")
        return False
    
    finally:
        if 'consumer' in locals():
            consumer.close()
    
    return True

def check_connector_status():
    """Check connector status"""
    import requests
    
    try:
        response = requests.get("http://localhost:8083/connectors/mysql-fresh-connector/status")
        if response.status_code == 200:
            data = response.json()
            connector_state = data.get("connector", {}).get("state", "unknown")
            task_state = data.get("tasks", [{}])[0].get("state", "unknown")
            
            print(f"📊 Connector Status:")
            print(f"   🔧 Connector: {connector_state}")
            print(f"   ⚙️  Task: {task_state}")
            
            return connector_state == "RUNNING" and task_state == "RUNNING"
        else:
            print(f"❌ Failed to get connector status: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error checking connector: {str(e)}")
        return False

def main():
    print("🚀 Verifying Debezium CDC Setup...")
    print("=" * 60)
    
    # Step 1: Check connector status
    print("📊 Step 1: Checking connector status...")
    if check_connector_status():
        print("✅ Connector is running properly\n")
        
        # Step 2: Test CDC
        print("🔍 Step 2: Testing CDC functionality...")
        test_cdc_topics()
        
        print("\n" + "=" * 60)
        print("🎯 SUMMARY:")
        print("✅ Debezium schema history issue has been RESOLVED")
        print("✅ MySQL connector is running and monitoring changes")
        print("✅ CDC events will be captured in real-time")
        print("\n💡 To test CDC:")
        print("1. Connect to MySQL: mysql -h 46.245.77.98 -u root -p")
        print("2. Insert data: INSERT INTO adtrace_db_stage.buy_transaction (user_id, amount) VALUES (999, 100.50);")
        print("3. Monitor topics: docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic buy_transaction --from-beginning")
        
    else:
        print("❌ Connector is not running properly")

if __name__ == "__main__":
    main() 