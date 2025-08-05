#!/usr/bin/env python3
"""
Schema Migration Script - MySQL to PostgreSQL
تاریخ: 2025-08-05
نویسنده: AdTrace Migration System
هدف: ایجاد تمام جداول MySQL در PostgreSQL با schema مناسب
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
import MySQLdb
import psycopg2
from sqlalchemy import create_engine, text
import argparse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MySQLToPostgreSQLMigrator:
    """اسکریپت مهاجرت schema از MySQL به PostgreSQL"""
    
    def __init__(self):
        # MySQL Configuration
        self.mysql_host = os.getenv('MYSQL_HOST', '46.245.77.98')
        self.mysql_user = os.getenv('MYSQL_USER', 'root')
        self.mysql_password = os.getenv('MYSQL_PASSWORD', 'mauFJcuf5dhRMQrjj')
        self.mysql_database = os.getenv('MYSQL_DATABASE', 'adtrace_db_stage')
        self.mysql_port = int(os.getenv('MYSQL_PORT', '3306'))
        
        # PostgreSQL Configuration  
        self.postgres_host = os.getenv('POSTGRES_HOST', '172.17.0.1')
        self.postgres_user = os.getenv('POSTGRES_USER', 'postgres')
        self.postgres_password = os.getenv('POSTGRES_PASSWORD', 'postgres')
        self.postgres_port = int(os.getenv('POSTGRES_PORT', '5432'))
        
        # New database name with today's date
        today = datetime.now().strftime('%Y%m%d')
        self.target_database = f'adtrace_live_{today}'
        self.admin_database = 'postgres'  # For initial connection to create new DB
        
        # Database engines
        self.mysql_engine = None
        self.postgres_engine = None
        self.admin_engine = None
        
        # Type mapping from MySQL to PostgreSQL
        self.type_mapping = {
            'int': 'INTEGER',
            'bigint': 'BIGINT',
            'smallint': 'SMALLINT',
            'tinyint': 'SMALLINT',
            'varchar': 'VARCHAR',
            'char': 'CHAR',
            'text': 'TEXT',
            'longtext': 'TEXT',
            'mediumtext': 'TEXT',
            'tinytext': 'TEXT',
            'decimal': 'DECIMAL',
            'float': 'REAL',
            'double': 'DOUBLE PRECISION',
            'datetime': 'TIMESTAMP',
            'timestamp': 'TIMESTAMP',
            'date': 'DATE',
            'time': 'TIME',
            'year': 'INTEGER',
            'blob': 'BYTEA',
            'longblob': 'BYTEA',
            'mediumblob': 'BYTEA',
            'tinyblob': 'BYTEA',
            'json': 'JSONB',
            'bit': 'BIT',
            'boolean': 'BOOLEAN',
            'enum': 'VARCHAR',
            'set': 'TEXT'
        }
        
        # Tables to exclude from migration
        self.excluded_tables = {
            'information_schema', 'performance_schema', 'mysql', 'sys',
            'migration_log', 'schema_migrations', 'flyway_schema_history'
        }
        
        self.migration_report = {
            'total_tables': 0,
            'migrated_tables': 0,
            'failed_tables': [],
            'skipped_tables': [],
            'start_time': datetime.now(),
            'end_time': None
        }
        
    def connect_databases(self):
        """اتصال به هر دو دیتابیس"""
        try:
            # MySQL connection
            mysql_conn_str = f"mysql://{self.mysql_user}:{self.mysql_password}@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
            self.mysql_engine = create_engine(mysql_conn_str, echo=False)
            
            # Test MySQL connection
            with self.mysql_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"✅ MySQL connected: {self.mysql_host}:{self.mysql_port}/{self.mysql_database}")
            
            # PostgreSQL admin connection to create new database
            admin_conn_str = f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.admin_database}"
            self.admin_engine = create_engine(admin_conn_str, echo=False, isolation_level="AUTOCOMMIT")
            
            # Test admin connection
            with self.admin_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"✅ PostgreSQL admin connected: {self.postgres_host}:{self.postgres_port}")
            
            # Create new database
            self.create_target_database()
            
            # Connect to the new database
            target_conn_str = f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.target_database}"
            self.postgres_engine = create_engine(target_conn_str, echo=False)
            
            # Test target database connection
            with self.postgres_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"✅ Target database connected: {self.postgres_host}:{self.postgres_port}/{self.target_database}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Database connection failed: {str(e)}")
            return False
    
    def create_target_database(self):
        """ایجاد دیتابیس جدید با نام امروز"""
        try:
            with self.admin_engine.connect() as conn:
                # Check if database already exists
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM pg_database WHERE datname = :db_name
                """), {"db_name": self.target_database})
                
                if result.fetchone()[0] > 0:
                    logger.info(f"✅ Database {self.target_database} already exists")
                else:
                    # Create new database
                    conn.execute(text(f'CREATE DATABASE "{self.target_database}"'))
                    logger.info(f"✅ Created new database: {self.target_database}")
                    
        except Exception as e:
            logger.error(f"❌ Failed to create database {self.target_database}: {str(e)}")
            raise
    
    def get_mysql_tables(self) -> List[str]:
        """دریافت لیست جداول MySQL"""
        try:
            with self.mysql_engine.connect() as conn:
                result = conn.execute(text("SHOW TABLES"))
                tables = [row[0] for row in result.fetchall()]
                
            # Filter out excluded tables
            filtered_tables = [
                table for table in tables 
                if table not in self.excluded_tables and not table.startswith('_')
            ]
            
            logger.info(f"📋 Found {len(filtered_tables)} tables to migrate (out of {len(tables)} total)")
            return filtered_tables
            
        except Exception as e:
            logger.error(f"❌ Failed to get MySQL tables: {str(e)}")
            return []
    
    def get_mysql_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """دریافت schema جدول از MySQL"""
        try:
            with self.mysql_engine.connect() as conn:
                result = conn.execute(text(f"DESCRIBE {table_name}"))
                schema = [dict(row._mapping) for row in result.fetchall()]
                
                # Get indexes information
                index_result = conn.execute(text(f"SHOW INDEX FROM {table_name}"))
                indexes = [dict(row._mapping) for row in index_result.fetchall()]
                
                return schema, indexes
                
        except Exception as e:
            logger.error(f"❌ Failed to get schema for {table_name}: {str(e)}")
            return [], []
    
    def convert_mysql_type_to_postgres(self, mysql_type: str) -> str:
        """تبدیل نوع داده MySQL به PostgreSQL"""
        # Parse MySQL type (e.g., "varchar(255)", "int(11)", "decimal(10,2)")
        mysql_type = mysql_type.lower()
        
        # Handle unsigned types
        is_unsigned = 'unsigned' in mysql_type
        if is_unsigned:
            mysql_type = mysql_type.replace(' unsigned', '').strip()
        
        # Handle types with parameters
        if '(' in mysql_type:
            base_type = mysql_type.split('(')[0]
            params = mysql_type.split('(')[1].rstrip(')')
        else:
            base_type = mysql_type
            params = None
        
        # Special cases
        if base_type == 'tinyint' and params == '1':
            return 'BOOLEAN'
        
        # Handle unsigned integers
        if is_unsigned and base_type in ['int', 'smallint', 'bigint', 'tinyint']:
            if base_type == 'tinyint':
                return 'SMALLINT'
            elif base_type == 'smallint':
                return 'INTEGER'
            elif base_type == 'int':
                return 'BIGINT'
            elif base_type == 'bigint':
                return 'BIGINT'
        
        if base_type in self.type_mapping:
            postgres_type = self.type_mapping[base_type]
            
            # Add parameters if needed
            if params and postgres_type in ['VARCHAR', 'CHAR', 'DECIMAL', 'BIT']:
                return f"{postgres_type}({params})"
            else:
                return postgres_type
        else:
            # Default fallback
            logger.warning(f"⚠️ Unknown MySQL type: {mysql_type}, defaulting to TEXT")
            return 'TEXT'
    
    def create_postgres_table_sql(self, table_name: str, schema: List[Dict[str, Any]], indexes: List[Dict[str, Any]]) -> str:
        """ایجاد SQL برای ساخت جدول در PostgreSQL"""
        
        columns = []
        primary_keys = []
        unique_keys = []
        regular_indexes = []
        
        # Process columns
        for col in schema:
            field_name = col['Field']
            mysql_type = col['Type']
            is_nullable = col['Null'] == 'YES'
            default_value = col['Default']
            extra = col.get('Extra', '')
            
            # Convert type
            postgres_type = self.convert_mysql_type_to_postgres(mysql_type)
            
            # Build column definition
            col_def = f'"{field_name}" {postgres_type}'
            
            # Handle auto increment
            if 'auto_increment' in extra.lower():
                if postgres_type.startswith('INTEGER'):
                    col_def = f'"{field_name}" SERIAL'
                elif postgres_type.startswith('BIGINT'):
                    col_def = f'"{field_name}" BIGSERIAL'
            
            # Handle NOT NULL
            if not is_nullable:
                col_def += ' NOT NULL'
            
            # Handle DEFAULT values
            if default_value is not None and default_value != 'NULL':
                if default_value == 'CURRENT_TIMESTAMP':
                    col_def += ' DEFAULT CURRENT_TIMESTAMP'
                elif isinstance(default_value, str):
                    # Clean up MySQL charset prefixes and quotes for JSONB
                    cleaned_default = default_value
                    
                    # Remove MySQL charset prefixes like _utf8mb4, _utf8, etc.
                    if cleaned_default.startswith('_utf8mb4') or cleaned_default.startswith('_utf8') or cleaned_default.startswith('_latin1'):
                        # Find the quote start after charset prefix
                        quote_pos = cleaned_default.find("'")
                        if quote_pos != -1:
                            cleaned_default = cleaned_default[quote_pos:]
                    
                    # Handle JSONB defaults
                    if postgres_type == 'JSONB':
                        if cleaned_default == "'{}'" or cleaned_default == "'{}'":
                            col_def += " DEFAULT '{}'"
                        elif cleaned_default.startswith("'") and cleaned_default.endswith("'"):
                            # Extract content between quotes
                            json_content = cleaned_default[1:-1]
                            # Remove any escape characters that might cause issues
                            json_content = json_content.replace('\\', '')
                            if json_content == '{}' or json_content == '':
                                col_def += " DEFAULT '{}'"
                            else:
                                # Validate and clean JSON content
                                try:
                                    import json as json_module
                                    # Try to parse and reformat
                                    parsed = json_module.loads(json_content if json_content != '{}' else '{}')
                                    clean_json = json_module.dumps(parsed)
                                    col_def += f" DEFAULT '{clean_json}'"
                                except:
                                    # If parsing fails, use empty object
                                    col_def += " DEFAULT '{}'"
                        else:
                            col_def += " DEFAULT '{}'"
                    # Handle other string defaults
                    elif cleaned_default.lower() not in ['null', 'current_timestamp']:
                        if not cleaned_default.startswith("'"):
                            col_def += f" DEFAULT '{cleaned_default}'"
                        else:
                            col_def += f" DEFAULT {cleaned_default}"
                else:
                    col_def += f' DEFAULT {default_value}'
            
            columns.append(col_def)
        
        # Process indexes
        for idx in indexes:
            key_name = idx['Key_name']
            column_name = idx['Column_name']
            is_unique = idx['Non_unique'] == 0
            
            if key_name == 'PRIMARY':
                if column_name not in primary_keys:
                    primary_keys.append(column_name)
            elif is_unique and key_name != 'PRIMARY':
                unique_keys.append((key_name, column_name))
            else:
                regular_indexes.append((key_name, column_name))
        
        # Build CREATE TABLE statement
        sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n'
        sql += ',\n'.join([f'    {col}' for col in columns])
        
        # Add primary key constraint
        if primary_keys:
            pk_cols = ', '.join([f'"{pk}"' for pk in primary_keys])
            sql += f',\n    PRIMARY KEY ({pk_cols})'
        
        sql += '\n);'
        
        # Add unique constraints
        unique_sqls = []
        processed_unique = set()
        for key_name, column_name in unique_keys:
            if key_name not in processed_unique:
                unique_sqls.append(f'CREATE UNIQUE INDEX IF NOT EXISTS "idx_{table_name}_{key_name}" ON "{table_name}" ("{column_name}");')
                processed_unique.add(key_name)
        
        # Add regular indexes
        index_sqls = []
        processed_indexes = set()
        for key_name, column_name in regular_indexes:
            if key_name not in processed_indexes:
                index_sqls.append(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_{key_name}" ON "{table_name}" ("{column_name}");')
                processed_indexes.add(key_name)
        
        # Combine all SQL statements
        all_sql = [sql] + unique_sqls + index_sqls
        return '\n\n'.join(all_sql)
    
    def table_exists_in_postgres(self, table_name: str) -> bool:
        """بررسی وجود جدول در PostgreSQL"""
        try:
            with self.postgres_engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = :table_name
                """), {"table_name": table_name})
                return result.fetchone()[0] > 0
        except Exception as e:
            logger.error(f"❌ Error checking table existence {table_name}: {str(e)}")
            return False
    
    def migrate_table(self, table_name: str, force_recreate: bool = False) -> bool:
        """مهاجرت یک جدول"""
        try:
            logger.info(f"🔄 Migrating table: {table_name}")
            
            # Check if table already exists
            if self.table_exists_in_postgres(table_name) and not force_recreate:
                logger.info(f"⏭️ Table {table_name} already exists, skipping...")
                self.migration_report['skipped_tables'].append(table_name)
                return True
            
            # Get MySQL schema
            schema, indexes = self.get_mysql_table_schema(table_name)
            if not schema:
                raise Exception(f"Could not get schema for table {table_name}")
            
            # Generate PostgreSQL SQL
            create_sql = self.create_postgres_table_sql(table_name, schema, indexes)
            
            # Drop table if force recreate
            if force_recreate and self.table_exists_in_postgres(table_name):
                logger.info(f"🗑️ Dropping existing table {table_name}")
                with self.postgres_engine.connect() as conn:
                    trans = conn.begin()
                    try:
                        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
                        trans.commit()
                    except Exception as e:
                        trans.rollback()
                        raise e
            
            # Create table in PostgreSQL
            with self.postgres_engine.connect() as conn:
                trans = conn.begin()
                try:
                    # Split SQL by statements and execute each
                    statements = create_sql.split(';')
                    for statement in statements:
                        statement = statement.strip()
                        if statement:
                            conn.execute(text(statement))
                    
                    trans.commit()
                    logger.info(f"✅ Successfully migrated table: {table_name}")
                    self.migration_report['migrated_tables'] += 1
                    return True
                    
                except Exception as e:
                    trans.rollback()
                    raise e
                    
        except Exception as e:
            logger.error(f"❌ Failed to migrate table {table_name}: {str(e)}")
            self.migration_report['failed_tables'].append({'table': table_name, 'error': str(e)})
            return False
    
    def run_migration(self, specific_tables: Optional[List[str]] = None, force_recreate: bool = False):
        """اجرای کامل مهاجرت"""
        logger.info("🚀 Starting MySQL to PostgreSQL schema migration")
        logger.info(f"📅 Migration date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"🎯 Target database: {self.target_database}")
        
        # Connect to databases
        if not self.connect_databases():
            return False
        
        # Get tables to migrate
        if specific_tables:
            tables_to_migrate = specific_tables
            logger.info(f"📋 Migrating specific tables: {tables_to_migrate}")
        else:
            tables_to_migrate = self.get_mysql_tables()
        
        if not tables_to_migrate:
            logger.error("❌ No tables found to migrate")
            return False
        
        self.migration_report['total_tables'] = len(tables_to_migrate)
        
        # Migrate each table
        success_count = 0
        for table_name in tables_to_migrate:
            if self.migrate_table(table_name, force_recreate):
                success_count += 1
            
            # Progress update
            progress = (success_count / len(tables_to_migrate)) * 100
            logger.info(f"📊 Progress: {success_count}/{len(tables_to_migrate)} ({progress:.1f}%)")
        
        # Final report
        self.migration_report['end_time'] = datetime.now()
        self.print_migration_report()
        
        return success_count == len(tables_to_migrate)
    
    def print_migration_report(self):
        """چاپ گزارش نهایی مهاجرت"""
        duration = self.migration_report['end_time'] - self.migration_report['start_time']
        
        logger.info("="*60)
        logger.info("📊 MIGRATION REPORT")
        logger.info("="*60)
        logger.info(f"🎯 Target Database: {self.target_database}")
        logger.info(f"⏱️ Duration: {duration}")
        logger.info(f"📋 Total tables: {self.migration_report['total_tables']}")
        logger.info(f"✅ Successfully migrated: {self.migration_report['migrated_tables']}")
        logger.info(f"⏭️ Skipped tables: {len(self.migration_report['skipped_tables'])}")
        logger.info(f"❌ Failed tables: {len(self.migration_report['failed_tables'])}")
        
        if self.migration_report['skipped_tables']:
            logger.info(f"⏭️ Skipped: {', '.join(self.migration_report['skipped_tables'])}")
        
        if self.migration_report['failed_tables']:
            logger.info("❌ Failed tables:")
            for failed in self.migration_report['failed_tables']:
                logger.info(f"   - {failed['table']}: {failed['error']}")
        
        success_rate = (self.migration_report['migrated_tables'] / self.migration_report['total_tables']) * 100
        logger.info(f"📈 Success rate: {success_rate:.1f}%")
        
        if success_rate > 0:
            logger.info("")
            logger.info("🔍 Next Steps:")
            logger.info(f"1. Verify tables in new database:")
            logger.info(f"   psql -h {self.postgres_host} -U {self.postgres_user} -d {self.target_database}")
            logger.info(f"   \\dt")
            logger.info("")
            logger.info(f"2. Update .env file to use new database:")
            logger.info(f"   POSTGRES_DATABASE={self.target_database}")
            logger.info("")
            logger.info(f"3. Test connection to new database:")
            logger.info(f"   psql -h {self.postgres_host} -U {self.postgres_user} -d {self.target_database} -c 'SELECT COUNT(*) FROM information_schema.tables;'")
        
        logger.info("="*60)

def main():
    """تابع اصلی اسکریپت"""
    parser = argparse.ArgumentParser(description='MySQL to PostgreSQL Schema Migration Tool')
    parser.add_argument('--tables', nargs='+', help='Specific tables to migrate (default: all tables)')
    parser.add_argument('--force', action='store_true', help='Force recreate existing tables')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be migrated without actually doing it')
    
    args = parser.parse_args()
    
    # Create migrator instance
    migrator = MySQLToPostgreSQLMigrator()
    
    if args.dry_run:
        logger.info("🔍 DRY RUN MODE - No changes will be made")
        if not migrator.connect_databases():
            return 1
        
        tables = args.tables if args.tables else migrator.get_mysql_tables()
        logger.info(f"📋 Would migrate {len(tables)} tables: {', '.join(tables)}")
        return 0
    
    # Run migration
    success = migrator.run_migration(
        specific_tables=args.tables,
        force_recreate=args.force
    )
    
    return 0 if success else 1

if __name__ == "__main__":
    exit(main()) 