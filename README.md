# MySQL to PostgreSQL Migration System with Dynamic CDC

## Overview

This project implements a comprehensive real-time migration monitoring system from MySQL to PostgreSQL using Change Data Capture (CDC) technology. The system automatically discovers database tables, monitors changes in real-time, and provides detailed migration statistics through a web dashboard.

## System Architecture

### Core Components

The system consists of several interconnected services that work together to provide real-time data migration monitoring:

**Database Services:**
- MySQL: Source database containing original data
- PostgreSQL: Target database for migration
- Redis: Cache and statistics storage

**Message Processing:**
- Apache Kafka: Event streaming platform for CDC events
- Zookeeper: Kafka coordination service
- Kafka Connect: Debezium connector runtime environment

**Application Services:**
- Data Validator: Python FastAPI service for CDC processing and validation
- Monitoring Dashboard: Node.js web interface for real-time monitoring
- Kafka UI: Web interface for Kafka topic management

## How Change Data Capture Works

### The CDC Flow Process

Change Data Capture is the core technology that enables real-time detection of database changes. Here is how it works step by step:

### Visual Flow Diagram

```
┌────────────────────────────┐
│        You issue a         │ ← INSERT / UPDATE / DELETE
│        SQL command         │
└────────────┬───────────────┘
             │
             ▼
┌────────────────────────────┐
│        MySQL Engine        │ ← Executes the command
│                            │ ← Modifies data in the table
└────────────┬───────────────┘
             │
             ▼
┌────────────────────────────┐
│     mysql-bin.000011       │ ← MySQL writes to binlog automatically
│     Position: 803          │ ← This is done by MySQL internally
└────────────┬───────────────┘
             │
             ▼
┌────────────────────────────┐
│        Debezium            │ ← Reads binlog entries
│        Connector           │ ← Converts to JSON message
└────────────┬───────────────┘
             │
             ▼
┌────────────────────────────┐
│        Kafka Topic         │ ← Publishes the JSON message
│         "users"            │
└────────────┬───────────────┘
             │
             ▼
┌────────────────────────────┐
│        Our Code            │ ← Consumes the Kafka message
│        _process_cdc        │ ← Increments the counter, etc.
└────────────────────────────┘
```

### Detailed Step-by-Step Process

**Step 1: MySQL Binary Log Recording**
When you execute any SQL operation (INSERT, UPDATE, DELETE) on MySQL, the database engine automatically writes these changes to binary log files called binlog. These files contain a complete record of all data modifications with precise timestamps and positions.

**Step 2: Debezium Connector Reading**
Debezium connector acts as a specialized binary log reader. It continuously monitors the MySQL binlog files and reads new entries as they are written. The connector maintains its current position in the binlog to ensure no events are missed.

**Step 3: Event Transformation and Publishing**
When Debezium detects a change in the binlog, it converts the raw binary data into structured JSON messages. These messages contain information about what operation occurred (insert/update/delete), which table was affected, the data before and after the change, and metadata about the source.

**Step 4: Kafka Topic Distribution**
The JSON messages are published to specific Kafka topics. Each database table gets its own topic, allowing for organized and scalable event processing. Kafka ensures reliable delivery and ordering of these events.

**Step 5: Consumer Processing**
Our application consumes these messages from Kafka topics and processes them to update statistics, validate data consistency, and maintain real-time sync status information.

## Dynamic Table Discovery System

### Traditional vs Dynamic Approach

Traditional CDC systems require manual configuration of which tables to monitor through static configuration files. This project implements a dynamic discovery system that automatically detects and monitors all database tables without hardcoded configurations.

### Discovery Process

**Initial Discovery:**
The system connects to MySQL and queries the information schema to get a complete list of all tables in the target database. System tables and temporary tables are automatically filtered out.

**Continuous Monitoring:**
Every 30 seconds, the system re-scans the database for new tables or removed tables. When changes are detected, the CDC connectors are automatically updated to include or exclude tables as needed.

**Schema Awareness:**
The system also tracks column information for each table, allowing it to detect schema changes and adapt accordingly.

### Connector Management

**Dynamic Connector Creation:**
Instead of using static JSON configuration files, the system programmatically creates Debezium connectors through the Kafka Connect REST API. The table include list is built dynamically based on discovered tables.

**Automatic Updates:**
When new tables are detected, the system recreates the connector with an updated configuration that includes the new tables. This ensures all database changes are captured without manual intervention.

## Services Detailed Architecture

### Data Validator Service (Python FastAPI)

This is the core service that handles CDC processing and data validation.

**Key Components:**

`services/data-validator/main.py`: Main application entry point that initializes all services and provides REST API endpoints.

`services/data-validator/services/dynamic_table_monitor.py`: Core CDC management system that handles table discovery, connector management, and event processing.

`services/data-validator/database/mysql_client.py`: MySQL database connection and query utilities.

`services/data-validator/database/postgres_client.py`: PostgreSQL database connection and query utilities.

`services/data-validator/services/data_validator.py`: Data consistency validation logic.

`services/data-validator/services/monitoring.py`: System monitoring and metrics collection.

**Key Functions:**

Table Discovery: Automatically scans MySQL to find all monitorable tables.

Connector Management: Creates and updates Debezium connectors dynamically.

Event Processing: Consumes CDC events from Kafka and updates statistics.

Data Validation: Compares data between MySQL and PostgreSQL to detect inconsistencies.

API Endpoints: Provides REST APIs for status checking and system control.

### Monitoring Dashboard Service (Node.js)

Web-based interface for real-time monitoring of the migration process.

**Key Files:**

`services/monitoring-dashboard/server.js`: Express.js server that provides the web interface and WebSocket connections for real-time updates.

`services/monitoring-dashboard/public/index.html`: Single-page application that displays migration statistics, table sync status, and CDC event counters.

**Features:**

Real-time table sync percentages showing how much data has been migrated for each table.

CDC events counter that increments with each database change detected.

Connector status indicators showing the health of MySQL and PostgreSQL connections.

Latest records viewer for examining recent data in both databases.

### Database Clients

**MySQL Client:**
Handles connections to the source MySQL database. Provides methods for querying table lists, counting records, retrieving data, and accessing table schemas.

**PostgreSQL Client:**
Manages connections to the target PostgreSQL database. Includes functionality for data insertion, updates, deletions, and migration logging.

## Understanding CDC Events Counter

### What the Counter Represents

The CDC Events Processed counter displayed in the dashboard represents the total number of change events that have been detected and processed since the system started.

### When the Counter Increases

The counter increments by one for each of these operations:

**INSERT Operations:** When new records are added to any monitored table.
**UPDATE Operations:** When existing records are modified in any monitored table.  
**DELETE Operations:** When records are removed from any monitored table.

### Counter Reset Behavior

The counter resets to zero when Docker containers are restarted because it is stored in application memory (RAM) rather than persistent storage. However, the actual CDC events are stored in Redis for historical tracking, so the event history is preserved even after restarts.

### Event Processing Flow

When a database change occurs:
1. MySQL writes the change to its binary log
2. Debezium reads the binary log and creates a JSON event
3. The event is published to the appropriate Kafka topic
4. Our consumer reads the event from Kafka
5. The counter is incremented and sync statistics are updated

## Sync Status Determination

### Real-time Sync Calculation

The system uses CDC events to trigger sync status updates. When an event is received:

1. The system queries both MySQL and PostgreSQL to get current record counts
2. Sync percentage is calculated as (PostgreSQL count / MySQL count) * 100
3. The table status is updated with the new percentages and last operation type

### Kafka Role in Sync Detection

Kafka serves multiple critical functions beyond just event counting:

**Event Streaming:** Delivers change events from Debezium to our processing system.
**Real-time Triggers:** Each Kafka message triggers a sync status recalculation.
**Operation Tracking:** Kafka messages contain operation types (insert/update/delete) for detailed statistics.
**Historical Data:** Events are stored for audit trails and debugging.

## API Endpoints

### Core Endpoints

`GET /health`: System health check with database connection status.
`GET /metrics`: Detailed migration statistics and table sync percentages.
`GET /table-monitor/status`: Comprehensive CDC system status.
`GET /discover-tables`: Manual table discovery trigger.

### Control Endpoints

`POST /table-monitor/setup`: Initialize dynamic table monitoring.
`POST /table-monitor/add-table/{table_name}`: Manually add a table to monitoring.

### Legacy Endpoints

`GET /cdc/status`: Legacy CDC status (redirects to table-monitor).
`POST /cdc/setup-dynamic`: Legacy setup (redirects to table-monitor).

## Configuration

### Environment Variables

The system uses environment variables for database connections:

```
MYSQL_HOST=mysql
MYSQL_USER=debezium
MYSQL_PASSWORD=dbz
MYSQL_DATABASE=inventory
POSTGRES_HOST=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DATABASE=inventory
REDIS_HOST=redis
```

### Discovery Settings

Table discovery runs every 30 seconds by default. System tables, temporary tables, and backup tables are automatically excluded from monitoring.

## Deployment

The entire system runs in Docker containers orchestrated by Docker Compose. The main docker-compose.yml file defines all services and their interconnections.

### Starting the System

```bash
  docker-compose up --build -d
```

### Accessing Services

- Web Dashboard: http://localhost:3000
- API Documentation: http://localhost:8000
- Kafka UI: http://localhost:8080

## Data Flow Summary

1. User modifies data in MySQL database
2. MySQL writes change to binary log
3. Debezium connector reads binary log
4. Change event is published to Kafka topic
5. Python consumer processes the event
6. Sync statistics are updated in real-time
7. Dashboard displays current status

This architecture ensures that any change in MySQL is immediately detected, processed, and reflected in the monitoring system without requiring manual configuration or intervention.
