# Quick Start Guide

## Initial Setup Steps

### 1. Start All Services

Run the following command to start all Docker containers:

```bash
docker-compose up -d
```

Check the status of all services:

```bash
docker-compose ps
```

### 2. Wait for Services to Initialize

Monitor the logs to ensure all services are ready:

```bash
docker-compose logs -f
```

Check specific service logs if needed:

```bash
docker logs mysql
docker logs postgres
docker logs kafka
docker logs zookeeper
docker logs kafka-connect
docker logs data-validator
docker logs monitoring-dashboard
```

### 3. Verify Database Connections

Test MySQL connection:

```bash
docker exec mysql mysql -uroot -pdebezium -e "SHOW DATABASES;"
```

Test PostgreSQL connection:

```bash
docker exec postgres psql -U postgres -c "\l"
```

### 4. Initialize Debezium Connector

The system uses dynamic CDC management. Start the table monitor:

```bash
curl -X POST http://localhost:8000/table-monitor/setup
```

Check monitoring status:

```bash
curl http://localhost:8000/table-monitor/status
```

### 5. Access Dashboards and APIs

Access the monitoring dashboard at:
http://localhost:3000

Access the Data Validator API documentation at:
http://localhost:8000/docs

Access Kafka Connect REST API at:
http://localhost:8083

## Test System Functionality

### Add Test Data to MySQL

Insert a new user:

```bash
docker exec mysql mysql -uroot -pdebezium -D inventory -e "INSERT INTO users (username, email, full_name, metadata) VALUES ('testuser123', 'test123@example.com', 'Test User', '{\"test\": true}');"
```

Update an existing product:

```bash
docker exec mysql mysql -uroot -pdebezium -D inventory -e "UPDATE products SET price = price * 1.1 WHERE id = 1;"
```

Delete a record:

```bash
docker exec mysql mysql -uroot -pdebezium -D inventory -e "DELETE FROM users WHERE username = 'testuser123';"
```

### Monitor CDC Messages

View Kafka messages for users table:

```bash
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic dbserver1.inventory.users --from-beginning
```

View messages for products table:

```bash
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic dbserver1.inventory.products --from-beginning
```

List all available topics:

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

## API Endpoints Reference

### Health Check

Check system health:

```bash
curl http://localhost:8000/health
```

### Migration Statistics

Get migration stats:

```bash
curl http://localhost:8000/stats
```

Get detailed metrics:

```bash
curl http://localhost:8000/metrics
```

### Data Validation

Trigger full validation:

```bash
curl -X POST http://localhost:8000/validate -H "Content-Type: application/json" -d '{"full_validation": true}'
```

Trigger quick validation:

```bash
curl -X POST http://localhost:8000/validate -H "Content-Type: application/json" -d '{"full_validation": false}'
```

### Latest Records

Get latest records from a table:

```bash
curl http://localhost:8000/latest-records/users
curl http://localhost:8000/latest-records/products
curl http://localhost:8000/latest-records/orders
```

### Connector Management

Get connector status:

```bash
curl http://localhost:8000/connector-status
```

Discover new tables:

```bash
curl -X POST http://localhost:8000/discover-tables
```

Add specific table to monitoring:

```bash
curl -X POST http://localhost:8000/table-monitor/add-table/new_table_name
```

### Kafka Status

Check Kafka connection:

```bash
curl http://localhost:8000/kafka-status
```

### Migration Log

View migration log:

```bash
curl http://localhost:8000/migration-log
```

## Troubleshooting Guide

### If Kafka Connect is Not Working

Restart the connector:

```bash
curl -X POST http://localhost:8083/connectors/dynamic-mysql-source/restart
```

Check connector status:

```bash
curl http://localhost:8083/connectors/dynamic-mysql-source/status
```

Delete and recreate connector:

```bash
curl -X DELETE http://localhost:8083/connectors/dynamic-mysql-source
curl -X POST http://localhost:8000/table-monitor/setup
```

### If PostgreSQL Tables are Empty

Check data validator logs:

```bash
docker logs data-validator
```

Trigger manual validation:

```bash
curl -X POST http://localhost:8000/validate
```

Check Redis for accumulated stats:

```bash
docker exec redis redis-cli keys "*"
docker exec redis redis-cli get table_stats
```

### If Dashboard Shows Zero Statistics

Restart the data validator service:

```bash
docker-compose restart data-validator
```

Check if CDC events are being processed:

```bash
curl http://localhost:8000/metrics
```

Clear Redis cache and restart:

```bash
docker exec redis redis-cli FLUSHALL
docker-compose restart data-validator
```

### If Debezium Connector Fails

Check connector configuration:

```bash
curl http://localhost:8083/connectors/dynamic-mysql-source/config
```

View connector logs:

```bash
docker logs kafka-connect
```

Verify MySQL binlog is enabled:

```bash
docker exec mysql mysql -uroot -pdebezium -e "SHOW VARIABLES LIKE 'log_bin';"
```

Check MySQL user permissions:

```bash
docker exec mysql mysql -uroot -pdebezium -e "SHOW GRANTS FOR 'debezium'@'%';"
```

### Database Connection Issues

Test MySQL connection manually:

```bash
docker exec mysql mysql -uroot -pdebezium -e "SELECT 1;"
```

Test PostgreSQL connection manually:

```bash
docker exec postgres psql -U postgres -c "SELECT 1;"
```

Check if databases exist:

```bash
docker exec mysql mysql -uroot -pdebezium -e "SHOW DATABASES;"
docker exec postgres psql -U postgres -l
```

### Performance Issues

Check system resources:

```bash
docker stats
```

Monitor Kafka lag:

```bash
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --list
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group data-validator-group
```

Check Redis memory usage:

```bash
docker exec redis redis-cli info memory
```

## Stop and Clean System

### Stop All Services

Stop all containers:

```bash
docker-compose down
```

### Remove All Data

Stop and remove all volumes (this will delete all data):

```bash
docker-compose down -v
```

### Remove Docker Images

Remove all project images:

```bash
docker rmi $(docker images | grep migration | awk '{print $3}')
```

### Complete Cleanup

Remove everything including networks:

```bash
docker-compose down -v --remove-orphans
docker system prune -f
```

## Configuration Files

### Environment Variables

The system uses these key environment variables in .env file:

- MYSQL_ROOT_PASSWORD: MySQL root password
- MYSQL_DATABASE: MySQL database name
- POSTGRES_DB: PostgreSQL database name
- POSTGRES_USER: PostgreSQL username
- POSTGRES_PASSWORD: PostgreSQL password
- KAFKA_BOOTSTRAP_SERVERS: Kafka server addresses
- REDIS_URL: Redis connection URL

### Docker Compose Services

- mysql: Source MySQL database
- postgres: Target PostgreSQL database
- zookeeper: Kafka coordination service
- kafka: Message streaming platform
- kafka-connect: Debezium CDC platform
- redis: Caching and statistics storage
- data-validator: Python FastAPI validation service
- monitoring-dashboard: Node.js monitoring interface

## Additional Resources

### View Database Schemas

MySQL schema:

```bash
docker exec mysql mysql -uroot -pdebezium -D inventory -e "DESCRIBE users;"
docker exec mysql mysql -uroot -pdebezium -D inventory -e "DESCRIBE products;"
```

PostgreSQL schema:

```bash
docker exec postgres psql -U postgres -d inventory -c "\d users"
docker exec postgres psql -U postgres -d inventory -c "\d products"
```

### Monitor System Logs

Follow all logs in real-time:

```bash
docker-compose logs -f --tail=100
```

Follow specific service logs:

```bash
docker-compose logs -f data-validator
docker-compose logs -f kafka-connect
```

### Backup and Restore

Create MySQL backup:

```bash
docker exec mysql mysqldump -uroot -pdebezium inventory > backup.sql
```

Restore to PostgreSQL:

```bash
docker exec -i postgres psql -U postgres inventory < backup.sql
```

This completes the comprehensive quick start guide for the MySQL to PostgreSQL migration system. 