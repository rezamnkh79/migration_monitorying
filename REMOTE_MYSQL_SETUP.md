# Remote MySQL CDC Setup Guide

این راهنما برای راه‌اندازی CDC با سرور MySQL خارجی (غیر لوکال) می‌باشد.

## پیش‌نیازها

### 1. تنظیمات MySQL سرور

سرور MySQL شما باید تنظیمات زیر را داشته باشد:

```sql
-- بررسی وضعیت binlog
SHOW VARIABLES LIKE 'log_bin';

-- بررسی فرمت binlog
SHOW VARIABLES LIKE 'binlog_format';

-- بررسی server_id
SHOW VARIABLES LIKE 'server_id';
```

اگر binlog فعال نیست، به فایل کانفیگ MySQL اضافه کنید:

```ini
[mysqld]
# CDC Configuration
server-id = 223344
log-bin = mysql-bin
binlog-format = ROW
binlog-row-image = FULL
expire_logs_days = 10
```

### 2. دسترسی‌های کاربر MySQL

کاربری که استفاده می‌کنید باید دسترسی‌های زیر را داشته باشد:

```sql
-- ایجاد کاربر CDC (در صورت نیاز)
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'strong_password';

-- دادن دسترسی‌های لازم
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';

-- دسترسی برای دیتابیس خاص
GRANT ALL PRIVILEGES ON your_database.* TO 'cdc_user'@'%';

FLUSH PRIVILEGES;
```

## راه‌اندازی

### 1. تنظیم Environment Variables

فایل `.env` را ویرایش کنید:

```bash
# Remote MySQL Configuration
MYSQL_HOST=46.245.77.98
MYSQL_USER=root
MYSQL_PASSWORD=mauFJcuf5dhRMQrjj
MYSQL_DATABASE=adtrace_db_stage
MYSQL_PORT=3306

# Local Services
POSTGRES_HOST=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DATABASE=inventory
POSTGRES_PORT=5432

REDIS_HOST=redis
REDIS_PORT=6380

KAFKA_BOOTSTRAP_SERVERS=kafka:29092

MONITORING_PORT=3000
VALIDATOR_PORT=8000
LOG_LEVEL=INFO
```

### 2. اجرای Setup Script

```bash
# اجرای script راه‌اندازی
./scripts/setup-remote-mysql.sh
```

این script موارد زیر را انجام می‌دهد:
- بررسی اتصال به MySQL خارجی
- چک کردن تنظیمات binlog
- راه‌اندازی سرویس‌های Docker
- ایجاد CDC connector
- بررسی وضعیت کانکتور

### 3. بررسی وضعیت

```bash
# بررسی وضعیت کلی
curl http://localhost:8000/table-monitor/status

# بررسی metrics
curl http://localhost:8000/metrics

# بررسی health
curl http://localhost:8000/health
```

## عیب‌یابی

### مشکل 1: عدم اتصال به MySQL

```bash
# تست اتصال دستی
mysql -h 46.245.77.98 -P 3306 -u root -p adtrace_db_stage

# بررسی firewall و network
telnet 46.245.77.98 3306
```

### مشکل 2: binlog غیرفعال

```sql
-- بررسی تنظیمات binlog
SHOW VARIABLES LIKE '%log_bin%';
SHOW VARIABLES LIKE '%binlog%';

-- فعال‌سازی binlog (نیاز به restart)
SET GLOBAL log_bin = ON;  -- ممکن است کار نکند
```

### مشکل 3: مشکل دسترسی

```sql
-- بررسی دسترسی‌های کاربر
SHOW GRANTS FOR 'root'@'%';

-- دادن دسترسی REPLICATION
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'root'@'%';
```

### مشکل 4: Connector failed

```bash
# بررسی لاگ‌های Kafka Connect
docker logs connect

# بررسی وضعیت کانکتور
curl http://localhost:8083/connectors/dynamic-mysql-source/status

# حذف و ایجاد مجدد کانکتور
curl -X DELETE http://localhost:8083/connectors/dynamic-mysql-source
curl -X POST http://localhost:8000/table-monitor/setup
```

## تست عملکرد

### 1. تست INSERT

```sql
-- اجرا روی سرور خارجی
INSERT INTO your_table (name, email) VALUES ('Test User', 'test@example.com');
```

### 2. مشاهده Events

```bash
# بررسی dashboard
curl http://localhost:8000/metrics

# مشاهده Kafka topics
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list

# مشاهده messages در topic
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic your_table_name --from-beginning
```

### 3. بررسی آمار

- Dashboard: http://localhost:3000
- API Docs: http://localhost:8000/docs
- Kafka UI: http://localhost:8080

## نکات مهم

### امنیت

1. **رمز عبور قوی**: حتما از رمز عبور قوی استفاده کنید
2. **محدودیت IP**: کاربر MySQL را به IP های خاص محدود کنید
3. **SSL**: در production از SSL استفاده کنید

### Performance

1. **Network Latency**: تاخیر شبکه روی CDC تاثیر می‌گذارد
2. **Binlog Size**: اندازه binlog را مدیریت کنید
3. **Table Size**: جداول بزرگ ممکن است initial snapshot طولانی داشته باشند

### Monitoring

1. **CPU Usage**: مصرف CPU سرور MySQL را مانیتور کنید
2. **Disk Space**: فضای binlog را چک کنید
3. **Network Usage**: ترافیک شبکه را بررسی کنید

## مشکلات شایع

### کانکتور وصل نمی‌شود

**علت**: مشکل network یا credentials

**راه حل**:
```bash
# تست اتصال
mysql -h MYSQL_HOST -P MYSQL_PORT -u MYSQL_USER -p

# بررسی لاگ‌ها
docker logs connect
```

### Events دریافت نمی‌شود

**علت**: binlog غیرفعال یا مشکل permissions

**راه حل**:
```sql
-- فعال‌سازی binlog
SHOW VARIABLES LIKE 'log_bin';

-- بررسی دسترسی‌ها
SHOW GRANTS FOR CURRENT_USER;
```

### Performance پایین

**علت**: Network latency یا large tables

**راه حل**:
- استفاده از snapshot.mode=schema_only
- تنظیم max.batch.size
- افزایش consumer threads

## Support

اگر مشکلی داشتید:

1. لاگ‌های سیستم را چک کنید: `docker-compose logs`
2. وضعیت کانکتور را بررسی کنید: `curl http://localhost:8083/connectors/dynamic-mysql-source/status`
3. MySQL binlog را چک کنید: `SHOW BINARY LOGS;` 