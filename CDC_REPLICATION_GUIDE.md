# راهنمای سیستم CDC Replication

## معرفی قابلیت جدید

به سیستم migration یک قابلیت جدید اضافه شده که **تغییرات MySQL را به صورت خودکار روی PostgreSQL اعمال می‌کند**.

### ویژگی‌های کلیدی:

✅ **اعمال خودکار تغییرات**: هر INSERT/UPDATE/DELETE در MySQL بلافاصله روی PostgreSQL اعمال می‌شود  
✅ **حفظ کدهای قبلی**: هیچ کد قبلی تغییر نکرده و سیستم قبلی کماکان کار می‌کند  
✅ **مانیتورینگ کامل**: آمار کامل از تعداد عملیات موفق و ناموفق  
✅ **مدیریت خطا**: در صورت خطا، سیستم ادامه کار می‌دهد  
✅ **Performance بالا**: کار موازی با سیستم monitoring قبلی  

## چگونگی کار

### 1. فلوی عملیات

```
MySQL Change → Debezium → Kafka → CDC Consumer → CDC Replicator → PostgreSQL
```

### 2. نوع عملیات پشتیبانی شده

- **INSERT**: رکورد جدید در PostgreSQL اضافه می‌شود
- **UPDATE**: رکورد موجود در PostgreSQL آپدیت می‌شود
- **DELETE**: رکورد از PostgreSQL حذف می‌شود

## نصب و راه‌اندازی

### 1. سیستم از قبل نصب است
اگر سیستم قبلی شما کار می‌کند، نیازی به تغییر نیست. فقط restart کنید:

```bash
docker-compose down
docker-compose up -d
```

### 2. تست سیستم

```bash
# 1. بررسی وضعیت replication
curl http://localhost:9000/replication/stats

# 2. مشاهده آمار
curl http://localhost:9000/replication/stats | jq .
```

## تست کارکرد

### 1. اضافه کردن رکورد جدید

```sql
-- در MySQL
INSERT INTO buy_transaction (account_id, user_id, wallet_id, amount, creation_time, last_update_time, is_deleted) 
VALUES (9999, 9999, 1, '500.00', NOW(), NOW(), 0);
```

### 2. آپدیت رکورد

```sql
-- در MySQL  
UPDATE buy_transaction SET amount = '600.00' WHERE account_id = 9999;
```

### 3. حذف رکورد

```sql
-- در MySQL
DELETE FROM buy_transaction WHERE account_id = 9999;
```

### 4. بررسی نتیجه

```sql
-- در PostgreSQL
SELECT * FROM buy_transaction WHERE account_id = 9999;
```

## مانیتورینگ

### 1. آمار replication

```bash
curl http://localhost:9000/replication/stats
```

خروجی نمونه:
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

در آدرس http://localhost:4000 می‌توانید:
- آمار real-time مشاهده کنید
- تعداد رویدادهای CDC ببینید
- وضعیت connectorها را چک کنید

### 3. ریست آمار

```bash
curl -X POST http://localhost:9000/replication/reset-stats
```

## عیب‌یابی

### 1. اگر replication کار نمی‌کند

```bash
# بررسی logs
docker logs data-validator

# بررسی وضعیت
curl http://localhost:9000/health
```

### 2. خطاهای رایج

**خطا: "Table does not exist in PostgreSQL"**
- جدول در PostgreSQL ایجاد نشده
- ابتدا schema را sync کنید

**خطا: "Could not find primary key"**
- جدول primary key ندارد یا اسم آن 'id' نیست
- فیلد primary key را بررسی کنید

**خطا: "Duplicate key"**  
- رکورد قبلاً وجود داشته
- سیستم خودکار سعی در UPDATE می‌کند

### 3. بررسی کارکرد

```bash
# تعداد رکوردهای MySQL
mysql -h 46.245.77.98 -u root -p adtrace_db_stage -e "SELECT COUNT(*) FROM buy_transaction;"

# تعداد رکوردهای PostgreSQL
docker exec postgres psql -U postgres -d inventory -c "SELECT COUNT(*) FROM buy_transaction;"
```

## تنظیمات پیشرفته

### 1. کنترل جداول

فقط جداولی که در PostgreSQL وجود دارند replicate می‌شوند.

### 2. Type Conversion

سیستم خودکار انواع data را تبدیل می‌کند:
- MySQL DATETIME → PostgreSQL TIMESTAMP
- MySQL INT → PostgreSQL INTEGER
- MySQL VARCHAR → PostgreSQL TEXT

### 3. Performance

- هر رویداد CDC کمتر از 1 ثانیه اعمال می‌شود
- سیستم با MySQL و PostgreSQL connection pooling کار می‌کند
- در صورت خطا، retry نمی‌کند تا performance را حفظ کند

## سوالات متداول

**Q: آیا کدهای قبلی خراب می‌شوند؟**  
A: خیر، هیچ کد قبلی تغییر نکرده و همه چیز کماکان کار می‌کند.

**Q: اگر PostgreSQL خاموش باشد چه می‌شود؟**  
A: سیستم monitoring ادامه کار می‌دهد، فقط replication متوقف می‌شود.

**Q: آیا می‌توان replication را خاموش کرد؟**  
A: بله، کافی است متغیر محیطی تنظیم کنید یا service را restart کنید.

**Q: چگونه می‌توان جداول خاص را exclude کرد؟**  
A: در کد می‌توان فیلتر اضافه کرد یا در تنظیمات Debezium.

## لاگ‌ها

```bash
# مشاهده لاگ‌های replication
docker logs data-validator | grep "CDC Replication"

# مشاهده لاگ‌های خطا
docker logs data-validator | grep "ERROR"
```

## پشتیبانی

در صورت مشکل:
1. لاگ‌ها را بررسی کنید
2. وضعیت health endpoint را چک کنید  
3. آمار replication را مشاهده کنید
4. در صورت نیاز، issue ایجاد کنید 