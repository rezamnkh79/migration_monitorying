# CDC Replication System Guide

## New Feature Introduction

A new feature has been added to the migration system that **automatically applies MySQL changes to PostgreSQL**.

### Key Features:

**Automatic Change Application**: Every INSERT/UPDATE/DELETE in MySQL is immediately applied to PostgreSQL  
**Preserve Existing Code**: No previous code has been changed and the existing system continues to work  
**Complete Monitoring**: Complete statistics of successful and failed operations  
**Error Management**: In case of error, the system continues to work  
**High Performance**: Works in parallel with the previous monitoring system

## How It Works

### 1. Operation Flow

```
MySQL Change → Debezium → Kafka → CDC Consumer → CDC Replicator → PostgreSQL
```

### 2. Supported Operation Types

- **INSERT**: New record is added to PostgreSQL
- **UPDATE**: Existing record is updated in PostgreSQL
- **DELETE**: Record is deleted from PostgreSQL

## Installation and Setup

### 1. System is already installed
If your previous system is working, no changes are needed. Just restart:

```bash
docker-compose down
docker-compose up -d
```

### 2. Test System

```bash
# 1. Check replication status
curl http://localhost:9000/replication/stats

# 2. View statistics
curl http://localhost:9000/replication/stats | jq .
```

## Testing Functionality

### 1. Adding New Record

```sql
-- In MySQL
INSERT INTO buy_transaction (account_id, user_id, wallet_id, amount, creation_time, last_update_time, is_deleted) 
VALUES (9999, 9999, 1, '500.00', NOW(), NOW(), 0);
```

### 2. Update Record

```sql
-- In MySQL  
UPDATE buy_transaction SET amount = '600.00' WHERE account_id = 9999;
```

### 3. Delete Record

```sql
-- In MySQL
DELETE FROM buy_transaction WHERE account_id = 9999;
```

### 4. Check Results

```sql
-- In PostgreSQL
SELECT * FROM buy_transaction WHERE account_id = 9999;
```

## Monitoring

### 1. Replication Statistics

```bash
curl http://localhost:9000/replication/stats
```

Sample Output:
```json
{
  "status": "active",
  "replication_stats": {
    "total_replicated": 150,
    "successful_inserts": 50,
    "successful_updates": 70,
    "successful_deletes": 25,
    "failed_operations": 5,
    "last_replication": "2024-01-15T10:30:45"
  },
  "timestamp": "2024-01-15T10:30:45"
}
```

### 2. Dashboard

At http://localhost:4000 you can:
- View real-time statistics
- See the number of CDC events
- Check connector status

### 3. Reset Statistics

```bash
curl -X POST http://localhost:9000/replication/reset-stats
```

## Troubleshooting

### 1. If replication is not working

```bash
# Check logs
docker logs data-validator

# Check status
curl http://localhost:9000/health
```

### 2. Common Errors

**Error: "Table does not exist in PostgreSQL"**
- Table not created in PostgreSQL
- First sync the schema

**Error: "Could not find primary key"**
- Table has no primary key or its name is not 'id'
- Check primary key field

**Error: "Duplicate key"**  
- Record already existed
- System automatically tries UPDATE

### 3. Check Functionality

```bash
# Number of MySQL records
mysql -h 46.245.77.98 -u root -p adtrace_db_stage -e "SELECT COUNT(*) FROM buy_transaction;"

# Number of PostgreSQL records
docker exec postgres psql -U postgres -d inventory -c "SELECT COUNT(*) FROM buy_transaction;"
```

## Advanced Settings

### 1. Table Control

Only tables that exist in PostgreSQL are replicated.

### 2. Type Conversion

The system automatically converts data types:
- MySQL DATETIME → PostgreSQL TIMESTAMP
- MySQL INT → PostgreSQL INTEGER
- MySQL VARCHAR → PostgreSQL TEXT

### 3. Performance

- Each CDC event is applied in less than 1 second
- System works with MySQL and PostgreSQL connection pooling
- In case of error, does not retry to maintain performance

## Frequently Asked Questions

**Q: Will previous code break?**  
A: No, no previous code has been changed and everything continues to work.

**Q: What happens if PostgreSQL is down?**  
A: The monitoring system continues to work, only replication stops.

**Q: Can replication be turned off?**  
A: Yes, just set the environment variable or restart the service.

**Q: How can specific tables be excluded?**  
A: A filter can be added in the code or in Debezium settings.

## Logs

```bash
# View replication logs
docker logs data-validator | grep "CDC Replication"

# View error logs
docker logs data-validator | grep "ERROR"
```

## Support

In case of problems:
1. Check the logs
2. Check health endpoint status  
3. View replication statistics
4. If needed, create an issue 