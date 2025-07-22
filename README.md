# AdTrace MySQL to PostgreSQL Migration System

## Overview

This project implements a comprehensive real-time migration monitoring system from MySQL to PostgreSQL using Change Data Capture (CDC) technology. The system automatically discovers database tables, monitors changes in real-time, and provides detailed migration statistics through a professional web dashboard.

## Quick Start

### 1. Start All Services

```bash
docker-compose up -d
```

### 2. Initialize the CDC System

```bash
curl -X POST http://localhost:8000/table-monitor/setup
```

### 3. Access the Dashboard

- **Monitoring Dashboard**: http://localhost:3000
- **API Documentation**: http://localhost:8000/docs
- **Kafka UI**: http://localhost:8080

## System Architecture

### Core Components

**Database Services:**
- **MySQL**: Source database (supports both local and remote)
- **PostgreSQL**: Target database for migration
- **Redis**: Cache and statistics storage

**Message Processing:**
- **Apache Kafka**: Event streaming platform for CDC events
- **Zookeeper**: Kafka coordination service
- **Kafka Connect**: Debezium connector runtime environment

**Application Services:**
- **Data Validator**: Python FastAPI service for CDC processing and validation
- **Monitoring Dashboard**: Professional Node.js web interface for real-time monitoring
- **Kafka UI**: Web interface for Kafka topic management

## How Change Data Capture Works

### The CDC Flow Process

```
┌────────────────────────────┐
│        SQL Command         │ ← INSERT / UPDATE / DELETE
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
│      "adtrace_migration"   │
└────────────┬───────────────┘
             │
             ▼
┌────────────────────────────┐
│        Our System          │ ← Consumes the Kafka message
│        Processes CDC       │ ← Updates statistics and sync status
└────────────────────────────┘
```

## Configuration

### Environment Variables

Create a `.env` file with your database credentials:

```bash
# Remote MySQL Configuration (AdTrace Production)
MYSQL_HOST=46.245.77.98
MYSQL_USER=root
MYSQL_PASSWORD=mauFJcuf5dhRMQrjj
MYSQL_DATABASE=adtrace_db_stage
MYSQL_PORT=3306

# Local PostgreSQL
POSTGRES_HOST=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DATABASE=inventory
POSTGRES_PORT=5432

# Local Services
REDIS_HOST=redis
REDIS_PORT=6380
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
MONITORING_PORT=3000
VALIDATOR_PORT=8000
LOG_LEVEL=INFO
```

### MySQL Requirements

Your MySQL server must have the following configuration:

```sql
-- Check binlog status
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'server_id';
```

If binlog is not enabled, add to MySQL configuration:

```ini
[mysqld]
server-id = 223344
log-bin = mysql-bin
binlog-format = ROW
binlog-row-image = FULL
expire_logs_days = 10
```

### MySQL User Permissions

The MySQL user needs these permissions:

```sql
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'your_user'@'%';
GRANT ALL PRIVILEGES ON your_database.* TO 'your_user'@'%';
FLUSH PRIVILEGES;
```

## Migration Status

### ✅ Complete Schema Migration

All 70+ AdTrace tables have been successfully migrated to PostgreSQL:

| Category | Tables | Status |
|----------|--------|---------|
| **AdTrace Core** | 17 tables | ✅ Complete |
| **Django Framework** | 8 tables | ✅ Complete |
| **Authentication** | 5 tables | ✅ Complete |
| **Business Logic** | 25+ tables | ✅ Complete |
| **Financial** | 10+ tables | ✅ Complete |
| **Supporting** | 15+ tables | ✅ Complete |
| **TOTAL** | **70+ tables** | ✅ **100% Complete** |

### Key AdTrace Tables Migrated

- `adtrace_tracker` ✅
- `adtrace_transaction` ✅
- `adtrace_mobile_app` ✅
- `adtrace_event_type` ✅
- `buy_transaction` ✅
- `mobile_app_detail` ✅
- `business_profile` ✅
- `invoice`, `wallet` ✅
- And 60+ more...

## API Endpoints

### Health & Status

```bash
# System health check
curl http://localhost:8000/health

# Detailed metrics and sync status
curl http://localhost:8000/metrics

# CDC connector status
curl http://localhost:8000/debezium/status

# Table monitoring status
curl http://localhost:8000/table-monitor/status
```

### Data Operations

```bash
# Get latest records from a table
curl http://localhost:8000/latest-records/buy_transaction

# Trigger data validation
curl -X POST http://localhost:8000/validate -H "Content-Type: application/json" -d '{"full_validation": true}'

# Manual sync check
curl -X POST http://localhost:8000/sync-check
```

### Table Management

```bash
# Discover all database tables
curl http://localhost:8000/discover-tables

# Setup table monitoring
curl -X POST http://localhost:8000/table-monitor/setup

# Add specific table to monitoring
curl -X POST http://localhost:8000/table-monitor/add-table/new_table_name
```

## Testing the System

### Add Test Data to MySQL

```bash
# Insert test data
mysql -h 46.245.77.98 -u root -p adtrace_db_stage -e "
INSERT INTO buy_transaction (account_id, user_id, wallet_id, amount, creation_time, last_update_time, is_deleted) 
VALUES (999, 999, 1, '100.00', NOW(), NOW(), 0);"

# Update existing data
mysql -h 46.245.77.98 -u root -p adtrace_db_stage -e "
UPDATE buy_transaction SET amount = amount * 1.1 WHERE account_id = 999;"

# Delete test data
mysql -h 46.245.77.98 -u root -p adtrace_db_stage -e "
DELETE FROM buy_transaction WHERE account_id = 999;"
```

### Monitor CDC Events

```bash
# View Kafka topics
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list

# Monitor the main migration topic
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic adtrace_migration --from-beginning

# Check system metrics
curl http://localhost:8000/metrics | jq '.cdc_stats'
```

## Dashboard Features

The professional monitoring dashboard at http://localhost:3000 provides:

- **Real-time Statistics**: Live CDC event counts and sync percentages
- **Table Overview**: Complete list of all tables with sync status
- **Filtering**: Filter tables by sync status (All, Synced, Not Synced, Empty, Errors)
- **Pagination**: View 10, 20, 40, 50 tables per page or show all
- **Connector Status**: Real-time status of MySQL and PostgreSQL connectors
- **Latest Records**: View and compare latest records from both databases
- **Professional UI**: Modern glass morphism design with gradients and animations

## Troubleshooting

### CDC Connector Issues

```bash
# Check connector status
curl http://localhost:8083/connectors/adtrace-migration-working/status

# Restart connector
curl -X POST http://localhost:8083/connectors/adtrace-migration-working/restart

# View connector logs
docker logs connect

# Recreate connector
curl -X DELETE http://localhost:8083/connectors/adtrace-migration-working
curl -X POST http://localhost:8000/table-monitor/setup
```

### Database Connection Issues

```bash
# Test MySQL connection
mysql -h 46.245.77.98 -u root -p adtrace_db_stage -e "SELECT 1;"

# Test PostgreSQL connection
docker exec postgres psql -U postgres -d inventory -c "SELECT 1;"

# Check MySQL binlog
mysql -h 46.245.77.98 -u root -p -e "SHOW VARIABLES LIKE 'log_bin';"
```

### Performance Issues

```bash
# Check Docker stats
docker stats

# Monitor Kafka consumer lag
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group adtrace-migration-consumer

# Check Redis memory
docker exec redis redis-cli info memory
```

### Reset System

```bash
# Stop all services
docker-compose down

# Complete cleanup (removes all data)
docker-compose down -v
docker system prune -f

# Fresh start
docker-compose up -d
curl -X POST http://localhost:8000/table-monitor/setup
```

## System Monitoring

### Key Metrics to Watch

1. **CDC Events Processed**: Total number of database changes captured
2. **Table Sync Percentages**: How much data has been synchronized for each table
3. **Connector Health**: Status of MySQL and PostgreSQL connectors
4. **Operation Counts**: Number of INSERT, UPDATE, DELETE operations

### Log Monitoring

```bash
# Follow all logs
docker-compose logs -f --tail=100

# Specific service logs
docker-compose logs -f data-validator
docker-compose logs -f connect
docker-compose logs -f monitoring-dashboard
```

## Advanced Features

### Dynamic Table Discovery

The system automatically:
- Scans MySQL every 30 seconds for new tables
- Excludes system tables and temporary tables
- Updates CDC connectors dynamically
- No manual configuration required

### Single Topic Routing

All CDC events are routed to one Kafka topic (`adtrace_migration`) for simplified processing and monitoring.

### Real-time Dashboard Updates

The dashboard uses WebSocket connections for real-time updates without page refresh.

### Professional UI Design

- Glass morphism effects with backdrop blur
- Gradient backgrounds and modern styling
- Smooth animations and hover effects
- Responsive design for all screen sizes

## Architecture Details

### Services Communication

1. **MySQL** → **Debezium** → **Kafka** → **Data Validator**
2. **Data Validator** ↔ **PostgreSQL** (for validation)
3. **Data Validator** ↔ **Redis** (for statistics)
4. **Data Validator** ↔ **Dashboard** (via WebSocket)

### Data Flow

1. User modifies data in MySQL
2. MySQL writes change to binary log
3. Debezium reads binlog and creates JSON event
4. Event published to `adtrace_migration` Kafka topic
5. Data Validator consumes event and updates statistics
6. Dashboard displays real-time updates
7. PostgreSQL sync status calculated on demand

## Security Considerations

1. **Strong Passwords**: Use strong passwords for all databases
2. **Network Security**: Restrict MySQL access to specific IPs
3. **SSL/TLS**: Use encrypted connections in production
4. **User Permissions**: Grant minimal required permissions

## Performance Optimization

1. **Batch Processing**: Configure appropriate batch sizes
2. **Consumer Threads**: Adjust consumer thread counts
3. **Network Latency**: Monitor network performance for remote MySQL
4. **Disk Space**: Monitor binlog disk usage
5. **Memory Usage**: Monitor Redis and Kafka memory consumption

## Support

For issues and questions:

1. Check the dashboard at http://localhost:3000
2. Review API documentation at http://localhost:8000/docs
3. Monitor logs with `docker-compose logs -f`
4. Check connector status with provided API endpoints

This system provides a complete, professional-grade solution for MySQL to PostgreSQL migration with real-time monitoring and comprehensive CDC capabilities.
