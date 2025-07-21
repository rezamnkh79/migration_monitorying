#!/usr/bin/env python3
import requests
import time
import json
import subprocess
import os

def test_cdc_system():
    print("🧪 Testing CDC System Functionality...")
    
    # Step 1: Check current CDC status
    print("\n1️⃣ Checking Current CDC Status...")
    try:
        response = requests.get("http://localhost:8000/metrics", timeout=10)
        if response.status_code == 200:
            data = response.json()
            current_events = data.get('cdc_stats', {}).get('events_processed', 0)
            print(f"   📊 Current CDC events processed: {current_events}")
        else:
            print(f"   ❌ Could not get metrics: {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ Error getting metrics: {str(e)}")
        return False
    
    # Step 2: Check connector status
    print("\n2️⃣ Checking Connector Status...")
    try:
        response = requests.get("http://localhost:8083/connectors/dynamic-mysql-source/status", timeout=10)
        if response.status_code == 200:
            status = response.json()
            connector_state = status.get('connector', {}).get('state', 'UNKNOWN')
            task_state = 'NO_TASKS'
            if status.get('tasks'):
                task_state = status['tasks'][0].get('state', 'UNKNOWN')
            
            print(f"   📡 Connector State: {connector_state}")
            print(f"   🔧 Task State: {task_state}")
            
            if connector_state != 'RUNNING' or task_state != 'RUNNING':
                print("   ❌ Connector is not running properly")
                return False
        else:
            print(f"   ❌ Could not get connector status: {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ Error checking connector: {str(e)}")
        return False
    
    # Step 3: Check Kafka topics
    print("\n3️⃣ Checking Kafka Topics...")
    try:
        result = subprocess.run(
            ["docker", "exec", "kafka", "kafka-topics", "--bootstrap-server", "kafka:29092", "--list"],
            capture_output=True, text=True, timeout=10
        )
        
        topics = result.stdout.strip().split('\n')
        target_topics = ['buy_transaction', 'user_message', 'email']
        existing_topics = [t for t in target_topics if t in topics]
        
        print(f"   📋 Target topics found: {existing_topics}")
        
        if len(existing_topics) == 0:
            print("   ❌ No target topics found")
            return False
    except Exception as e:
        print(f"   ❌ Error checking topics: {str(e)}")
        return False
    
    # Step 4: Check for recent messages in topics
    print("\n4️⃣ Checking for Recent Messages...")
    messages_found = False
    
    for topic in existing_topics[:2]:  # Check first 2 topics
        try:
            print(f"   🔍 Checking topic: {topic}")
            result = subprocess.run([
                "docker", "exec", "kafka", "kafka-console-consumer", 
                "--bootstrap-server", "kafka:29092", 
                "--topic", topic,
                "--from-beginning",
                "--timeout-ms", "3000",
                "--max-messages", "5"
            ], capture_output=True, text=True, timeout=10)
            
            if result.stdout.strip():
                message_count = len([line for line in result.stdout.strip().split('\n') if line.strip()])
                print(f"   📨 Found {message_count} messages in {topic}")
                messages_found = True
            else:
                print(f"   📭 No messages in {topic}")
        except Exception as e:
            print(f"   ⚠️ Could not check {topic}: {str(e)}")
    
    # Step 5: Monitor CDC events for changes
    print("\n5️⃣ Monitoring for New CDC Events...")
    
    # Get baseline
    try:
        response = requests.get("http://localhost:8000/metrics", timeout=10)
        baseline_events = response.json().get('cdc_stats', {}).get('events_processed', 0)
        print(f"   📊 Baseline CDC events: {baseline_events}")
    except:
        baseline_events = 0
    
    # Wait a bit and check again
    print("   ⏳ Waiting 30 seconds for new events...")
    time.sleep(30)
    
    try:
        response = requests.get("http://localhost:8000/metrics", timeout=10)
        new_events = response.json().get('cdc_stats', {}).get('events_processed', 0)
        print(f"   📊 New CDC events: {new_events}")
        
        if new_events > baseline_events:
            print(f"   ✅ CDC is working! {new_events - baseline_events} new events detected")
            return True
        else:
            print("   ⚠️ No new events detected during monitoring period")
    except Exception as e:
        print(f"   ❌ Error checking new events: {str(e)}")
    
    # Step 6: Generate test data to verify CDC
    print("\n6️⃣ Testing with Generated Data...")
    print("   💡 To test CDC manually, run:")
    print("   mysql -h 46.245.77.98 -u root -pmauFJcuf5dhRMQrjj adtrace_db_stage")
    print("   INSERT INTO buy_transaction (user_id, amount, created_at) VALUES (9999, 99.99, NOW());")
    print("   UPDATE buy_transaction SET amount = 199.99 WHERE user_id = 9999;")
    print("   DELETE FROM buy_transaction WHERE user_id = 9999;")
    print("")
    print("   Then check: curl http://localhost:8000/metrics")
    
    # Summary
    print(f"\n📝 Test Summary:")
    print(f"   Connector Status: {'✅' if connector_state == 'RUNNING' and task_state == 'RUNNING' else '❌'}")
    print(f"   Topics Created: {'✅' if len(existing_topics) > 0 else '❌'}")
    print(f"   Messages Found: {'✅' if messages_found else '❌'}")
    print(f"   CDC Events: {new_events if 'new_events' in locals() else baseline_events}")
    
    return True

if __name__ == "__main__":
    test_cdc_system() 