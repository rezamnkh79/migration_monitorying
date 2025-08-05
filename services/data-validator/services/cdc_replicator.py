import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from database.mysql_client import MySQLClient
from database.postgres_client import PostgreSQLClient

logger = logging.getLogger(__name__)

class CDCReplicator:
    """
    CDC Replicator Service - اعمال تغییرات MySQL روی PostgreSQL
    
    این سرویس CDC events رو دریافت می‌کنه و به صورت خودکار 
    تغییرات رو روی PostgreSQL اعمال می‌کنه:
    - INSERT: رکورد جدید اضافه می‌کنه
    - UPDATE: رکورد موجود رو آپدیت می‌کنه  
    - DELETE: رکورد رو حذف می‌کنه
    """
    
    def __init__(self, mysql_client: MySQLClient, postgres_client: PostgreSQLClient, redis_client, global_stats: Dict[str, Any]):
        self.mysql = mysql_client
        self.postgres = postgres_client
        self.redis = redis_client
        self.global_stats = global_stats
        
        # آمار replication
        self.replication_stats = {
            "total_replicated": 0,
            "successful_inserts": 0,
            "successful_updates": 0, 
            "successful_deletes": 0,
            "failed_operations": 0,
            "last_replication": None
        }
        
        logger.info("CDC Replicator initialized")
    
    def process_cdc_event(self, cdc_event: Dict[str, Any], table_name: str, operation: str) -> bool:
        """
        پردازش CDC event و اعمال تغییر روی PostgreSQL
        
        Args:
            cdc_event: رویداد CDC از Debezium
            table_name: نام جدول
            operation: نوع عملیات (insert, update, delete)
            
        Returns:
            bool: موفقیت یا شکست عملیات
        """
        try:
            logger.info(f"Processing CDC replication: {operation} on {table_name}")
            
            # بررسی اینکه جدول در PostgreSQL وجود دارد
            if not self._table_exists_in_postgres(table_name):
                logger.warning(f"Table {table_name} does not exist in PostgreSQL, skipping replication")
                return False
            
            success = False
            
            if operation == "insert":
                success = self._handle_insert(cdc_event, table_name)
            elif operation == "update":
                success = self._handle_update(cdc_event, table_name)
            elif operation == "delete":
                success = self._handle_delete(cdc_event, table_name)
            else:
                logger.warning(f"Unknown operation: {operation}")
                return False
            
            # آپدیت آمار
            self._update_replication_stats(operation, success)
            
            return success
            
        except Exception as e:
            logger.error(f"Error processing CDC replication for {table_name}: {str(e)}")
            self._update_replication_stats(operation, False)
            return False
    
    def _handle_insert(self, cdc_event: Dict[str, Any], table_name: str) -> bool:
        """اعمال INSERT operation روی PostgreSQL"""
        try:
            # استخراج data از CDC event
            insert_data = self._extract_after_data(cdc_event)
            if not insert_data:
                logger.error("No 'after' data found in INSERT event")
                return False
            
            # تبدیل نام فیلدها و types
            postgres_data = self._convert_data_for_postgres(insert_data, table_name)
            
            # INSERT در PostgreSQL
            success = self.postgres.insert_record(table_name, postgres_data)
            
            if success:
                logger.info(f"Successfully replicated INSERT to {table_name}")
                return True
            else:
                logger.error(f"Failed to replicate INSERT to {table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error handling INSERT for {table_name}: {str(e)}")
            return False
    
    def _handle_update(self, cdc_event: Dict[str, Any], table_name: str) -> bool:
        """اعمال UPDATE operation روی PostgreSQL"""
        try:
            # استخراج before و after data
            before_data = self._extract_before_data(cdc_event)
            after_data = self._extract_after_data(cdc_event)
            
            if not before_data or not after_data:
                logger.error("Missing before/after data in UPDATE event")
                return False
            
            # پیدا کردن primary key
            record_id = self._extract_primary_key(before_data, table_name)
            if not record_id:
                logger.error(f"Could not find primary key for UPDATE in {table_name}")
                return False
            
            # تبدیل data برای PostgreSQL
            postgres_data = self._convert_data_for_postgres(after_data, table_name)
            
            # UPDATE در PostgreSQL
            success = self.postgres.update_record(table_name, record_id, postgres_data)
            
            if success:
                logger.info(f"Successfully replicated UPDATE to {table_name}, ID: {record_id}")
                return True
            else:
                logger.error(f"Failed to replicate UPDATE to {table_name}, ID: {record_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error handling UPDATE for {table_name}: {str(e)}")
            return False
    
    def _handle_delete(self, cdc_event: Dict[str, Any], table_name: str) -> bool:
        """اعمال DELETE operation روی PostgreSQL"""
        try:
            # استخراج before data (حاوی رکورد حذف شده)
            before_data = self._extract_before_data(cdc_event)
            if not before_data:
                logger.error("No 'before' data found in DELETE event")
                return False
            
            # پیدا کردن primary key
            record_id = self._extract_primary_key(before_data, table_name)
            if not record_id:
                logger.error(f"Could not find primary key for DELETE in {table_name}")
                return False
            
            # DELETE از PostgreSQL
            success = self.postgres.delete_record(table_name, record_id)
            
            if success:
                logger.info(f"Successfully replicated DELETE from {table_name}, ID: {record_id}")
                return True
            else:
                logger.error(f"Failed to replicate DELETE from {table_name}, ID: {record_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error handling DELETE for {table_name}: {str(e)}")
            return False
    
    def _extract_before_data(self, cdc_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """استخراج before data از CDC event"""
        return cdc_event.get('before')
    
    def _extract_after_data(self, cdc_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """استخراج after data از CDC event"""
        return cdc_event.get('after')
    
    def _extract_primary_key(self, data: Dict[str, Any], table_name: str) -> Optional[int]:
        """استخراج primary key از data"""
        # معمولاً primary key اسمش 'id' است
        if 'id' in data:
            return data['id']
        
        # سعی در پیدا کردن key های احتمالی دیگر
        possible_keys = [f'{table_name}_id', 'primary_key', 'pk']
        for key in possible_keys:
            if key in data:
                return data[key]
        
        logger.warning(f"Could not find primary key in data for table {table_name}")
        return None
    
    def _convert_data_for_postgres(self, data: Dict[str, Any], table_name: str) -> Dict[str, Any]:
        """تبدیل data برای سازگاری با PostgreSQL"""
        from datetime import datetime
        
        converted_data = {}
        
        for key, value in data.items():
            # حذف ID در صورت INSERT (PostgreSQL خودش generate می‌کنه)
            if key.lower() == 'id' and value is None:
                continue
                
            # تبدیل Unix timestamp به datetime
            if value is not None and (key.endswith('_time') or 'time' in key.lower()):
                try:
                    # اگر value یه عدد بزرگه، احتمالاً Unix timestamp هست (milliseconds)
                    if isinstance(value, (int, float)) and value > 1000000000:
                        if value > 10000000000:  # milliseconds
                            timestamp = value / 1000
                        else:  # seconds
                            timestamp = value
                        converted_data[key] = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        converted_data[key] = value
                except (ValueError, OverflowError):
                    converted_data[key] = value
            # تبدیل boolean fields (is_deleted, is_active, etc.)
            elif key.startswith('is_') or key.endswith('_flag') or key in ['deleted', 'active', 'enabled']:
                if isinstance(value, (int, str)):
                    converted_data[key] = bool(int(value)) if str(value).isdigit() else bool(value)
                else:
                    converted_data[key] = bool(value)
            else:
                converted_data[key] = value
                
        return converted_data
    
    def _is_datetime_string(self, value: str) -> bool:
        """بررسی اینکه string یک datetime است یا نه"""
        try:
            # فرمت‌های مختلف datetime
            datetime_formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d"
            ]
            
            for fmt in datetime_formats:
                try:
                    datetime.strptime(value, fmt)
                    return True
                except ValueError:
                    continue
            return False
        except:
            return False
    
    def _table_exists_in_postgres(self, table_name: str) -> bool:
        """بررسی وجود جدول در PostgreSQL"""
        try:
            postgres_tables = self.postgres.get_table_list()
            return table_name in postgres_tables
        except Exception as e:
            logger.error(f"Error checking table existence: {str(e)}")
            return False
    
    def _update_replication_stats(self, operation: str, success: bool):
        """آپدیت آمار replication"""
        self.replication_stats["total_replicated"] += 1
        self.replication_stats["last_replication"] = datetime.now().isoformat()
        
        if success:
            if operation == "insert":
                self.replication_stats["successful_inserts"] += 1
            elif operation == "update": 
                self.replication_stats["successful_updates"] += 1
            elif operation == "delete":
                self.replication_stats["successful_deletes"] += 1
        else:
            self.replication_stats["failed_operations"] += 1
        
        # ذخیره در global stats
        if "replication_stats" not in self.global_stats:
            self.global_stats["replication_stats"] = {}
        
        self.global_stats["replication_stats"].update(self.replication_stats)
    
    def get_replication_stats(self) -> Dict[str, Any]:
        """دریافت آمار replication"""
        return self.replication_stats.copy()
    
    def reset_stats(self):
        """ریست آمار replication"""
        self.replication_stats = {
            "total_replicated": 0,
            "successful_inserts": 0,
            "successful_updates": 0,
            "successful_deletes": 0, 
            "failed_operations": 0,
            "last_replication": None
        }
        logger.info("Replication stats reset") 