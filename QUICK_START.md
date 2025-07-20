# راهنمای سریع شروع

## مراحل راه‌اندازی سریع

### 1. راه‌اندازی اولیه

```bash
# اجرای تمام سرویس‌ها
docker-compose up -d

# بررسی وضعیت
docker-compose ps
```

### 2. انتظار برای آماده شدن سرویس‌ها

```bash
# بررسی لاگ‌ها
docker-compose logs -f

# یا بررسی سرویس خاص
docker logs mysql
docker logs postgres
docker logs kafka
```

### 3. تست اولیه

```bash
# اجرای تست سیستم
./scripts/test-migration.sh
```

### 4. راه‌اندازی Debezium

```bash
# راه‌اندازی کانکتور Debezium
./scripts/setup-debezium.sh
```

### 5. دسترسی به داشبوردها

- **Monitoring Dashboard**: http://localhost:3000
- **Data Validator API**: http://localhost:8000/docs
- **Kafka UI**: http://localhost:8080

## تست عملکرد

### اضافه کردن داده تست

```bash
# اضافه کردن کاربر جدید
docker exec mysql mysql -uroot -pdebezium -D inventory -e "
INSERT INTO users (username, email, full_name, metadata) 
VALUES ('test_$(date +%s)', 'test$(date +%s)@example.com', 'Test User', '{\"test\": true}');"

# بروزرسانی محصول
docker exec mysql mysql -uroot -pdebezium -D inventory -e "
UPDATE products SET price = price * 1.1 WHERE id = 1;"
```

### مشاهده پیام‌های CDC

```bash
# مشاهده پیام‌های Kafka
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic dbserver1.inventory.users \
  --from-beginning
```

## API های مفید

```bash
# بررسی سلامت
curl http://localhost:8000/health

# آمار مهاجرت
curl http://localhost:8000/stats

# تریگر validation
curl -X POST http://localhost:8000/validate \
  -H "Content-Type: application/json" \
  -d '{"full_validation": false}'
```

## رفع مشکلات سریع

### اگر Kafka Connect کار نمی‌کند:

```bash
# ریستارت کانکتور
curl -X POST http://localhost:8083/connectors/mysql-source-connector/restart

# بررسی وضعیت
curl http://localhost:8083/connectors/mysql-source-connector/status
```

### اگر PostgreSQL خالی است:

```bash
# بررسی لاگ data-validator
docker logs data-validator

# اجرای مجدد validation
curl -X POST http://localhost:8000/validate
```

## متوقف کردن سیستم

```bash
# متوقف کردن تمام سرویس‌ها
docker-compose down

# حذف volumes (برای پاک کردن کامل)
docker-compose down -v
``` 