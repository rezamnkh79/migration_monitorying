# مهاجرت MySQL به PostgreSQL با Debezium CDC

## 📋 فهرست مطالب
- [ساختار پروژه](#ساختار-پروژه)
- [چگونگی کارکرد Debezium](#چگونگی-کارکرد-debezium)
- [نحوه استفاده](#نحوه-استفاده)
- [مشاهده آخرین رکوردها](#مشاهده-آخرین-رکوردها)

## 🏗️ ساختار پروژه

```
migration_to_postgress/
├── services/                          # سرویس‌های اصلی
│   ├── monitoring-dashboard/           # داشبورد مانیتورینگ (Node.js)
│   │   ├── server.js                  # سرور اصلی داشبورد
│   │   ├── public/index.html          # رابط کاربری وب
│   │   └── package.json               # وابستگی‌های Node.js
│   └── data-validator/                # سرویس اعتبارسنجی داده (Python)
│       ├── main.py                    # سرور اصلی FastAPI
│       ├── services/                  # سرویس‌های مختلف
│       │   ├── kafka_consumer.py      # مصرف کننده Kafka برای CDC
│       │   ├── data_validator.py      # اعتبارسنجی داده‌ها
│       │   └── monitoring.py          # سرویس مانیتورینگ
│       ├── database/                  # کلاینت‌های پایگاه داده
│       │   ├── mysql_client.py        # اتصال به MySQL
│       │   └── postgres_client.py     # اتصال به PostgreSQL
│       └── models/                    # مدل‌های داده
├── debezium/                          # پیکربندی Debezium
│   ├── mysql-source-connector.json    # کانکتور منبع MySQL
│   └── postgres-sink-connector.json   # کانکتور مقصد PostgreSQL
├── scripts/                           # اسکریپت‌های کمکی
│   ├── setup-debezium.sh             # راه‌اندازی Debezium
│   └── test-migration.sh             # تست مهاجرت
└── docker-compose.yml                # تنظیمات Docker
```

## 🔄 چگونگی کارکرد Debezium

### مفهوم Change Data Capture (CDC)
Debezium یک پلتفرم open-source برای Change Data Capture (CDC) است که تغییرات داده‌ها را در real-time دنبال می‌کند.

### مراحل کارکرد:

#### 1️⃣ **خواندن Binary Log MySQL**
```
MySQL Database
    ↓ (Binary Log/Binlog)
MySQL Source Connector
    ↓ (Kafka Messages)
Kafka Topics
```

- Debezium به Binary Log (binlog) MySQL متصل می‌شود
- هر تغییر در داده‌ها (INSERT, UPDATE, DELETE) در binlog ثبت می‌شود
- Connector این تغییرات را می‌خواند و به پیام‌های Kafka تبدیل می‌کند

#### 2️⃣ **پردازش پیام‌ها در Kafka**
```
Kafka Topic: adtrace_block_list
├── INSERT Event: {"op": "c", "after": {...}}
├── UPDATE Event: {"op": "u", "before": {...}, "after": {...}}
└── DELETE Event: {"op": "d", "before": {...}}
```

- هر جدول یک Topic مجزا در Kafka دارد
- پیام‌ها شامل نوع عملیات (op) و داده‌ها (before/after) هستند

#### 3️⃣ **ارسال به PostgreSQL**
```
Kafka Consumer (Python)
    ↓ (Process & Transform)
PostgreSQL Database
```

### نمونه فلوی کامل:
```
1. کاربر یک رکورد جدید در MySQL اضافه می‌کند
   INSERT INTO users (name, email) VALUES ('احمد', 'ahmad@test.com')

2. MySQL این تغییر را در binlog ثبت می‌کند

3. Debezium MySQL Connector این تغییر را می‌خواند:
   {
     "op": "c",  // create
     "after": {
       "id": 123,
       "name": "احمد", 
       "email": "ahmad@test.com"
     }
   }

4. پیام به Kafka Topic ارسال می‌شود

5. Consumer Python پیام را دریافت و پردازش می‌کند

6. داده در PostgreSQL اضافه می‌شود

7. آمار در Redis و Dashboard آپدیت می‌شود
```

## 🚀 نحوه استفاده

### راه‌اندازی سیستم:
```bash
# شروع سرویس‌ها
docker-compose up -d

# راه‌اندازی Debezium
./scripts/setup-debezium.sh

# مشاهده داشبورد
http://localhost:3000
```

### مشاهده آمار:
- **تعداد کل رکوردها**: مقایسه تعداد رکوردها در MySQL و PostgreSQL
- **تعداد INSERT ها**: تعداد رکوردهای جدید اضافه شده
- **تعداد UPDATE ها**: تعداد رکوردهای آپدیت شده  
- **تعداد DELETE ها**: تعداد رکوردهای حذف شده

## 👁️ مشاهده آخرین رکوردها

### از طریق Dashboard:
1. در جدول اصلی، روی دکمه **"Latest"** کلیک کنید
2. پنجره‌ای باز می‌شود که نشان می‌دهد:
   - 5 رکورد آخر از MySQL
   - 5 رکورد آخر از PostgreSQL
   - وضعیت Sync
   - آمار CDC

### از طریق API:
```bash
# مشاهده 5 رکورد آخر از جدول users
curl http://localhost:8000/latest-records/users?limit=5

# نمونه پاسخ:
{
  "table_name": "users",
  "mysql_latest": [
    {"id": 100, "name": "علی", "created_at": "2024-01-15"},
    {"id": 99, "name": "مریم", "created_at": "2024-01-14"}
  ],
  "postgres_latest": [
    {"id": 100, "name": "علی", "created_at": "2024-01-15"},
    {"id": 99, "name": "مریم", "created_at": "2024-01-14"}
  ],
  "comparison": {
    "sync_status": "synced",
    "mysql_total": 100,
    "postgres_total": 100
  }
}
```

## 🔍 عیب‌یابی آمار صفر

اگر تعداد INSERT/UPDATE/DELETE صفر است:

### 1. بررسی اتصال Debezium:
```bash
# بررسی وضعیت کانکتورها
curl http://localhost:8083/connectors/mysql-source-connector/status
```

### 2. بررسی Kafka Topics:
```bash
# مشاهده Topic ها
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# مشاهده پیام‌ها
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic adtrace_users \
  --from-beginning
```

### 3. تست CDC:
```bash
# اضافه کردن داده تستی به MySQL
docker exec -it mysql mysql -u debezium -pdbz -e \
  "INSERT INTO inventory.users (name, email) VALUES ('تست', 'test@example.com')"
```

## 📊 مانیتورینگ Real-time

Dashboard هر 30 ثانیه آپدیت می‌شود و نشان می‌دهد:
- تعداد Events پردازش شده توسط CDC
- آخرین Event دریافت شده
- وضعیت هر جدول
- درصد پیشرفت Sync

## 🛠️ تنظیمات پیشرفته

### تغییر جداول تحت نظارت:
فایل `debezium/mysql-source-connector.json`:
```json
{
  "table.include.list": "database.table1,database.table2"
}
```

### تنظیم حافظه Redis:
فایل `docker-compose.yml`:
```yaml
redis:
  command: redis-server --maxmemory 512mb
```

این سیستم به شما امکان مهاجرت real-time و نظارت دقیق بر فرآیند انتقال داده‌ها را می‌دهد.

## 🚨 مدیریت خطاها

### مشکلات رایج و راه‌حل‌ها

1. **Debezium Connector متصل نمی‌شود:**
   ```bash
   # بررسی وضعیت connector
   curl http://localhost:8083/connectors/mysql-source-connector/status
   
   # ریستارت connector
   curl -X POST http://localhost:8083/connectors/mysql-source-connector/restart
   ```

2. **Data Validation خطا می‌دهد:**
   ```bash
   # بررسی لاگ‌های validator
   docker logs data-validator
   
   # ریست کردن validation status
   curl -X POST http://localhost:8000/reset-migration
   ```

3. **PostgreSQL Out of Sync:**
   ```bash
   # Sync manual یک رکورد خاص
   curl -X POST http://localhost:8000/sync-record \
     -H "Content-Type: application/json" \
     -d '{"table_name": "users", "mysql_id": 1, "operation": "INSERT"}'
   ```

## 📁 ساختار پروژه

```
migration_to_postgress/
├── docker-compose.yml              # تعریف کل سیستم
├── mysql/                          # تنظیمات MySQL
│   ├── my.cnf                     # کانفیگ MySQL
│   └── init.sql                   # اسکریپت اولیه
├── postgres/                       # تنظیمات PostgreSQL
│   └── init.sql                   # اسکریپت اولیه
├── services/
│   ├── data-validator/            # سرویس اعتبارسنجی
│   │   ├── main.py               # اپلیکیشن اصلی
│   │   ├── database/             # کلاینت‌های دیتابیس
│   │   ├── services/             # سرویس‌های validation
│   │   └── models/               # مدل‌های داده
│   └── monitoring-dashboard/      # داشبورد مانیتورینگ
│       ├── server.js             # سرور Node.js
│       └── public/               # فایل‌های استاتیک
└── scripts/                        # اسکریپت‌های کمکی
    ├── setup-debezium.sh         # راه‌اندازی Debezium
    └── test-migration.sh          # تست سیستم
```

## 🔒 امنیت

- تمام passwordها در environment variables
- Network isolation در Docker
- Access control برای API endpoints
- SSL/TLS برای production (نیاز به تنظیم اضافی)

## 📚 مستندات اضافی

### Debezium Configuration

برای تنظیمات پیشرفته‌تر Debezium:

```json
{
  "snapshot.mode": "initial",
  "snapshot.locking.mode": "minimal",
  "decimal.handling.mode": "precise",
  "time.precision.mode": "adaptive"
}
```

### Monitoring Metrics

متریک‌های کلیدی برای نظارت:

- **Lag Metrics**: تاخیر در انتقال داده
- **Error Rates**: نرخ خطاها
- **Throughput**: تعداد رکوردهای پردازش شده
- **Data Consistency**: میزان تطابق داده‌ها

## 🤝 مشارکت

برای مشارکت در پروژه:

1. Fork کردن repository
2. ایجاد branch جدید
3. Commit کردن تغییرات
4. Push به branch
5. ایجاد Pull Request

## 📄 مجوز

این پروژه تحت مجوز MIT منتشر شده است.

## 🆘 پشتیبانی

برای دریافت کمک:

1. بررسی Issue های موجود در GitHub
2. ایجاد Issue جدید با جزئیات مشکل
3. اجرای `docker logs <service-name>` برای بررسی لاگ‌ها

---

**نکته**: این پروژه برای محیط development و test طراحی شده است. برای استفاده در production نیاز به تنظیمات امنیتی و performance اضافی دارد. 