import asyncio
import hashlib
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import redis

from database.mysql_client import MySQLClient
from database.postgres_client import PostgreSQLClient
from models.validation_models import ValidationResult, TableStats

logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self, mysql_client: MySQLClient, postgres_client: PostgreSQLClient, redis_client: redis.Redis):
        self.mysql = mysql_client
        self.postgres = postgres_client
        self.redis = redis_client
        self.tables_to_sync = ['users', 'products', 'orders', 'order_items']
    
    def _calculate_row_hash(self, row_data: Dict[str, Any]) -> str:
        """Calculate hash for a row to detect changes"""
        # Convert row to JSON string and hash it
        # Handle datetime and other non-serializable objects
        serializable_data = {}
        for key, value in row_data.items():
            if isinstance(value, datetime):
                serializable_data[key] = value.isoformat()
            elif value is None:
                serializable_data[key] = None
            else:
                serializable_data[key] = str(value)
        
        json_str = json.dumps(serializable_data, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(json_str.encode('utf-8')).hexdigest()
    
    def _transform_mysql_to_postgres(self, table_name: str, mysql_record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform MySQL record to PostgreSQL format"""
        postgres_record = mysql_record.copy()
        
        # Handle specific transformations based on table
        if table_name == 'users':
            # Convert JSON string to dict for metadata field
            if 'metadata' in postgres_record and isinstance(postgres_record['metadata'], str):
                try:
                    postgres_record['metadata'] = json.loads(postgres_record['metadata'])
                except (json.JSONDecodeError, TypeError):
                    postgres_record['metadata'] = {}
        
        # Remove auto-increment ID for insertion (PostgreSQL will generate it)
        if 'id' in postgres_record:
            del postgres_record['id']
        
        return postgres_record
    
    async def validate_table(self, table_name: str, quick_check: bool = True) -> ValidationResult:
        """Validate data consistency between MySQL and PostgreSQL for a specific table"""
        logger.info(f"🔍 Validating table: {table_name} (quick_check: {quick_check})")
        
        try:
            # Get row counts
            mysql_count = self.mysql.get_table_count(table_name)
            postgres_count = self.postgres.get_table_count(table_name)
            
            result = ValidationResult(
                table_name=table_name,
                mysql_count=mysql_count,
                postgres_count=postgres_count,
                is_consistent=(mysql_count == postgres_count),
                validation_time=datetime.now(),
                quick_check=quick_check
            )
            
            if quick_check:
                # Quick validation - just check counts and recent records
                if mysql_count != postgres_count:
                    result.is_consistent = False
                    result.discrepancies.append(f"Row count mismatch: MySQL={mysql_count}, PostgreSQL={postgres_count}")
                
                # Check latest 10 records
                mysql_latest = self.mysql.get_latest_records(table_name, 10)
                postgres_latest = self.postgres.get_latest_records(table_name, 10)
                
                if len(mysql_latest) != len(postgres_latest):
                    result.discrepancies.append(f"Latest records count mismatch")
                    result.is_consistent = False
            
            else:
                # Full validation - compare all records
                await self._full_table_validation(table_name, result)
            
            # Cache result in Redis
            cache_key = f"validation:{table_name}:{datetime.now().strftime('%Y%m%d%H')}"
            result_dict = {
                "table_name": result.table_name,
                "mysql_count": result.mysql_count,
                "postgres_count": result.postgres_count,
                "is_consistent": result.is_consistent,
                "validation_time": result.validation_time.isoformat(),
                "quick_check": result.quick_check,
                "discrepancies": result.discrepancies
            }
            self.redis.setex(cache_key, 3600, json.dumps(result_dict))
            
            logger.info(f"✅ Validation completed for {table_name}: consistent={result.is_consistent}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Validation failed for table {table_name}: {str(e)}")
            return ValidationResult(
                table_name=table_name,
                mysql_count=0,
                postgres_count=0,
                is_consistent=False,
                validation_time=datetime.now(),
                quick_check=quick_check,
                discrepancies=[f"Validation error: {str(e)}"]
            )
    
    async def _full_table_validation(self, table_name: str, result: ValidationResult):
        """Perform full table validation by comparing all records"""
        try:
            # Get all data from both databases
            mysql_data = self.mysql.get_table_data(table_name)
            postgres_data = self.postgres.get_table_data(table_name)
            
            # Compare data
            if len(mysql_data) != len(postgres_data):
                result.discrepancies.append(f"Data length mismatch: MySQL={len(mysql_data)}, PostgreSQL={len(postgres_data)}")
                result.is_consistent = False
                return
            
            # Compare each record
            discrepancy_count = 0
            for idx in range(len(mysql_data)):
                mysql_row = mysql_data.iloc[idx].to_dict()
                
                # Find corresponding PostgreSQL row (assuming same order or matching by a key)
                if idx < len(postgres_data):
                    postgres_row = postgres_data.iloc[idx].to_dict()
                    
                    # Compare critical fields (excluding auto-generated timestamps that might differ)
                    mysql_hash = self._calculate_row_hash(mysql_row)
                    postgres_hash = self._calculate_row_hash(postgres_row)
                    
                    if mysql_hash != postgres_hash:
                        discrepancy_count += 1
                        if discrepancy_count <= 10:  # Limit detailed discrepancy reporting
                            result.discrepancies.append(f"Row {idx}: Data mismatch")
            
            if discrepancy_count > 0:
                result.is_consistent = False
                result.discrepancies.append(f"Total data mismatches: {discrepancy_count}")
                
        except Exception as e:
            result.discrepancies.append(f"Full validation error: {str(e)}")
            result.is_consistent = False
    
    async def validate_all_tables(self, quick_check: bool = True) -> List[ValidationResult]:
        """Validate all tables"""
        logger.info(f"🔍 Starting validation of all tables (quick_check: {quick_check})")
        
        results = []
        for table_name in self.tables_to_sync:
            result = await self.validate_table(table_name, quick_check)
            results.append(result)
        
        # Store overall validation result in Redis
        overall_status = {
            'timestamp': datetime.now().isoformat(),
            'total_tables': len(results),
            'consistent_tables': sum(1 for r in results if r.is_consistent),
            'inconsistent_tables': sum(1 for r in results if not r.is_consistent),
            'quick_check': quick_check
        }
        self.redis.setex('validation:overall', 1800, json.dumps(overall_status))
        
        logger.info(f"✅ Overall validation completed: {overall_status}")
        return results
    
    async def sync_record(self, table_name: str, mysql_id: int, operation: str) -> bool:
        """Sync a specific record from MySQL to PostgreSQL"""
        logger.info(f"🔄 Syncing record: {table_name}.{mysql_id} ({operation})")
        
        try:
            if operation.upper() == 'INSERT':
                return await self._sync_insert(table_name, mysql_id)
            elif operation.upper() == 'UPDATE':
                return await self._sync_update(table_name, mysql_id)
            elif operation.upper() == 'DELETE':
                return await self._sync_delete(table_name, mysql_id)
            else:
                logger.error(f"Unknown operation: {operation}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Failed to sync record {table_name}.{mysql_id}: {str(e)}")
            return False
    
    async def _sync_insert(self, table_name: str, mysql_id: int) -> bool:
        """Sync INSERT operation"""
        try:
            # Get record from MySQL
            mysql_record = self.mysql.get_record_by_id(table_name, mysql_id)
            if not mysql_record:
                logger.warning(f"Record {mysql_id} not found in MySQL table {table_name}")
                return False
            
            # Transform for PostgreSQL
            postgres_record = self._transform_mysql_to_postgres(table_name, mysql_record)
            
            # Insert into PostgreSQL
            success = self.postgres.insert_record(table_name, postgres_record)
            
            if success:
                # Log the migration
                data_hash = self._calculate_row_hash(mysql_record)
                self.postgres.log_migration(table_name, 'INSERT', mysql_id, None, data_hash)
                
                # Update Redis stats
                self._update_redis_stats(table_name, 'INSERT')
                
                logger.info(f"✅ Successfully synced INSERT: {table_name}.{mysql_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Insert sync failed for {table_name}.{mysql_id}: {str(e)}")
            return False
    
    async def _sync_update(self, table_name: str, mysql_id: int) -> bool:
        """Sync UPDATE operation"""
        try:
            # Get updated record from MySQL
            mysql_record = self.mysql.get_record_by_id(table_name, mysql_id)
            if not mysql_record:
                logger.warning(f"Record {mysql_id} not found in MySQL table {table_name}")
                return False
            
            # Transform for PostgreSQL
            postgres_data = self._transform_mysql_to_postgres(table_name, mysql_record)
            
            # Update in PostgreSQL (assuming same ID)
            success = self.postgres.update_record(table_name, mysql_id, postgres_data)
            
            if success:
                # Log the migration
                data_hash = self._calculate_row_hash(mysql_record)
                self.postgres.log_migration(table_name, 'UPDATE', mysql_id, mysql_id, data_hash)
                
                # Update Redis stats
                self._update_redis_stats(table_name, 'UPDATE')
                
                logger.info(f"✅ Successfully synced UPDATE: {table_name}.{mysql_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Update sync failed for {table_name}.{mysql_id}: {str(e)}")
            return False
    
    async def _sync_delete(self, table_name: str, mysql_id: int) -> bool:
        """Sync DELETE operation"""
        try:
            # Delete from PostgreSQL
            success = self.postgres.delete_record(table_name, mysql_id)
            
            if success:
                # Log the migration
                self.postgres.log_migration(table_name, 'DELETE', mysql_id, None, '')
                
                # Update Redis stats
                self._update_redis_stats(table_name, 'DELETE')
                
                logger.info(f"✅ Successfully synced DELETE: {table_name}.{mysql_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Delete sync failed for {table_name}.{mysql_id}: {str(e)}")
            return False
    
    def _update_redis_stats(self, table_name: str, operation: str):
        """Update Redis statistics"""
        try:
            today = datetime.now().strftime('%Y%m%d')
            
            # Increment operation counters
            self.redis.hincrby(f"stats:{table_name}:{today}", operation.lower(), 1)
            self.redis.hincrby(f"stats:total:{today}", operation.lower(), 1)
            
            # Set expiration (7 days)
            self.redis.expire(f"stats:{table_name}:{today}", 604800)
            self.redis.expire(f"stats:total:{today}", 604800)
            
            # Update last sync timestamp
            self.redis.set(f"last_sync:{table_name}", datetime.now().isoformat())
            
        except Exception as e:
            logger.error(f"Failed to update Redis stats: {str(e)}")
    
    async def get_migration_log(self, limit: int = 100, table_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get migration log entries"""
        try:
            query = "SELECT * FROM migration_log"
            params = {}
            
            if table_name:
                query += " WHERE table_name = :table_name"
                params['table_name'] = table_name
            
            query += " ORDER BY migrated_at DESC LIMIT :limit"
            params['limit'] = limit
            
            return self.postgres.execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Failed to get migration log: {str(e)}")
            return []
    
    async def reset_migration_status(self):
        """Reset migration status (for development/testing)"""
        try:
            # Clear Redis cache
            for key in self.redis.scan_iter(match="validation:*"):
                self.redis.delete(key)
            for key in self.redis.scan_iter(match="stats:*"):
                self.redis.delete(key)
            for key in self.redis.scan_iter(match="last_sync:*"):
                self.redis.delete(key)
            
            # Clear migration log
            self.postgres.execute_query("TRUNCATE TABLE migration_log")
            
            logger.info("✅ Migration status reset successfully")
            
        except Exception as e:
            logger.error(f"Failed to reset migration status: {str(e)}")
            raise 