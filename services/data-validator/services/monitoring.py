import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import redis
from database.mysql_client import MySQLClient
from database.postgres_client import PostgreSQLClient
from models.validation_models import TableStats, MigrationStatus

logger = logging.getLogger(__name__)

class MonitoringService:
    def __init__(self, redis_client: redis.Redis, mysql_client: MySQLClient, postgres_client: PostgreSQLClient):
        self.redis = redis_client
        self.mysql = mysql_client
        self.postgres = postgres_client
        # No hardcoded tables - completely dynamic
        self._cached_tables = None
        self._last_table_refresh = None
        self._table_refresh_interval = 300  # Refresh every 5 minutes
    
    def get_table_list(self) -> List[str]:
        """Get dynamic list of tables from MySQL"""
        current_time = datetime.now()
        
        # Check if we need to refresh the table list
        if (self._cached_tables is None or 
            self._last_table_refresh is None or 
            (current_time - self._last_table_refresh).total_seconds() > self._table_refresh_interval):
            
            self._refresh_table_list()
        
        return self._cached_tables or []
    
    def _refresh_table_list(self):
        """Refresh the table list from MySQL database"""
        try:
            logger.info("Refreshing table list from MySQL database...")
            
            # Get all tables from MySQL
            mysql_tables = self.mysql.get_table_list() if self.mysql else []
            
            if mysql_tables:
                # Filter out system and excluded tables
                excluded_patterns = [
                    'information_schema', 'performance_schema', 'mysql', 'sys',
                    'migration_log', 'schema_migrations', 'flyway_schema_history'
                ]
                
                def should_monitor_table(table_name):
                    table_lower = table_name.lower()
                    
                    # Skip system tables
                    for pattern in excluded_patterns:
                        if pattern in table_lower:
                            return False
                    
                    # Skip tables starting with underscore or tmp
                    if table_name.startswith('_') or table_name.startswith('tmp_'):
                        return False
                    
                    # Skip backup tables
                    if '_backup' in table_lower or '_bak' in table_lower:
                        return False
                    
                    return True
                
                # Filter tables for monitoring
                filtered_tables = [table for table in mysql_tables if should_monitor_table(table)]
                
                # Update cache
                self._cached_tables = filtered_tables
                self._last_table_refresh = datetime.now()
                
                logger.info(f"Discovered {len(filtered_tables)} tables to monitor: {filtered_tables[:10]}{'...' if len(filtered_tables) > 10 else ''}")
                
                # Store in Redis for other services
                if self.redis:
                    self.redis.setex('dynamic:monitored_tables', 300, json.dumps(filtered_tables))
            else:
                logger.warning("No tables found in MySQL database")
                if not self._cached_tables:
                    self._cached_tables = []
                    
        except Exception as e:
            logger.error(f"Failed to refresh table list: {str(e)}")
            # Try to get from Redis cache as fallback
            try:
                cached_data = self.redis.get('dynamic:monitored_tables')
                if cached_data:
                    self._cached_tables = json.loads(cached_data)
                    logger.info("Using cached table list from Redis")
                elif not self._cached_tables:
                    # Absolute fallback
                    self._cached_tables = []
                    logger.warning("Using empty table list as last resort")
            except Exception:
                if not self._cached_tables:
                    self._cached_tables = []
    
    def collect_metrics(self):
        """Collect current metrics from all data sources"""
        logger.info("Collecting metrics...")
        
        try:
            metrics = {
                'timestamp': datetime.now().isoformat(),
                'database_stats': self._collect_database_stats(),
                'sync_stats': self._collect_sync_stats(),
                'health_status': self._collect_health_status()
            }
            
            # Store in Redis with expiration
            self.redis.setex('metrics:current', 300, json.dumps(metrics))
            
            # Store hourly snapshot
            hour_key = f"metrics:hourly:{datetime.now().strftime('%Y%m%d%H')}"
            self.redis.setex(hour_key, 86400, json.dumps(metrics))
            
            logger.info("Metrics collected successfully")
            
        except Exception as e:
            logger.error(f"Failed to collect metrics: {str(e)}")
    
    def _collect_database_stats(self) -> Dict[str, Any]:
        """Collect database statistics"""
        # Get dynamic table list
        tables = self.get_table_list()
        
        stats = {
            'mysql': {'status': 'unknown', 'tables': {}, 'table_list': []},
            'postgres': {'status': 'unknown', 'tables': {}, 'table_list': []}
        }
        
        try:
            # MySQL stats
            if self.mysql.test_connection():
                stats['mysql']['status'] = 'connected'
                mysql_tables = self.mysql.get_table_list()
                stats['mysql']['table_list'] = mysql_tables
                
                for table in mysql_tables:
                    try:
                        count = self.mysql.get_table_count(table)
                        stats['mysql']['tables'][table] = count
                    except Exception as e:
                        logger.error(f"Failed to get MySQL count for {table}: {str(e)}")
                        stats['mysql']['tables'][table] = -1
            else:
                stats['mysql']['status'] = 'disconnected'
            
            # PostgreSQL stats
            if self.postgres.test_connection():
                stats['postgres']['status'] = 'connected'
                postgres_tables = self.postgres.get_table_list()
                stats['postgres']['table_list'] = postgres_tables
                
                for table in postgres_tables:
                    try:
                        count = self.postgres.get_table_count(table)
                        stats['postgres']['tables'][table] = count
                    except Exception as e:
                        logger.error(f"Failed to get PostgreSQL count for {table}: {str(e)}")
                        stats['postgres']['tables'][table] = -1
            else:
                stats['postgres']['status'] = 'disconnected'
                
        except Exception as e:
            logger.error(f"Database stats collection failed: {str(e)}")
        
        return stats
    
    def _collect_sync_stats(self) -> Dict[str, Any]:
        """Collect synchronization statistics"""
        sync_stats = {}
        today = datetime.now().strftime('%Y%m%d')
        
        try:
            # Get sync stats for each table
            for table in self.get_table_list(): # Use get_table_list() here
                table_stats = self.redis.hgetall(f"stats:{table}:{today}")
                last_sync = self.redis.get(f"last_sync:{table}")
                
                sync_stats[table] = {
                    'insert': int(table_stats.get('insert', 0)),
                    'update': int(table_stats.get('update', 0)),
                    'delete': int(table_stats.get('delete', 0)),
                    'last_sync': last_sync,
                    'sync_lag': self._calculate_sync_lag(last_sync) if last_sync else None
                }
            
            # Get total stats
            total_stats = self.redis.hgetall(f"stats:total:{today}")
            sync_stats['total'] = {
                'insert': int(total_stats.get('insert', 0)),
                'update': int(total_stats.get('update', 0)),
                'delete': int(total_stats.get('delete', 0))
            }
            
        except Exception as e:
            logger.error(f"Sync stats collection failed: {str(e)}")
        
        return sync_stats
    
    def _collect_health_status(self) -> Dict[str, Any]:
        """Collect overall health status"""
        health = {
            'overall': 'unknown',
            'mysql': 'unknown',
            'postgres': 'unknown',
            'redis': 'unknown',
            'kafka': 'unknown',
            'sync_lag_warning': False,
            'data_inconsistency_warning': False
        }
        
        try:
            # Database health
            health['mysql'] = 'healthy' if self.mysql.test_connection() else 'unhealthy'
            health['postgres'] = 'healthy' if self.postgres.test_connection() else 'unhealthy'
            
            # Redis health
            try:
                self.redis.ping()
                health['redis'] = 'healthy'
            except:
                health['redis'] = 'unhealthy'
            
            # Check for warnings
            health['sync_lag_warning'] = self._check_sync_lag_warning()
            health['data_inconsistency_warning'] = self._check_data_inconsistency_warning()
            
            # Overall health
            critical_services = [health['mysql'], health['postgres'], health['redis']]
            if all(status == 'healthy' for status in critical_services):
                if health['sync_lag_warning'] or health['data_inconsistency_warning']:
                    health['overall'] = 'warning'
                else:
                    health['overall'] = 'healthy'
            else:
                health['overall'] = 'unhealthy'
                
        except Exception as e:
            logger.error(f"Health status collection failed: {str(e)}")
        
        return health
    
    def _calculate_sync_lag(self, last_sync_str: str) -> Optional[int]:
        """Calculate sync lag in seconds"""
        try:
            if not last_sync_str:
                return None
            
            last_sync = datetime.fromisoformat(last_sync_str)
            lag = (datetime.now() - last_sync).total_seconds()
            return int(lag)
        except:
            return None
    
    def _check_sync_lag_warning(self) -> bool:
        """Check if any table has significant sync lag"""
        try:
            for table in self.get_table_list(): # Use get_table_list() here
                last_sync = self.redis.get(f"last_sync:{table}")
                if last_sync:
                    lag = self._calculate_sync_lag(last_sync)
                    if lag and lag > 300:  # 5 minutes
                        return True
            return False
        except:
            return False
    
    def _check_data_inconsistency_warning(self) -> bool:
        """Check for recent data inconsistencies"""
        try:
            # Check recent validation results
            overall_validation = self.redis.get('validation:overall')
            if overall_validation:
                data = json.loads(overall_validation)
                if data.get('inconsistent_tables', 0) > 0:
                    return True
            return False
        except:
            return False
    
    async def get_migration_stats(self) -> MigrationStatus:
        """Get overall migration status"""
        try:
            # Get current metrics
            metrics_data = self.redis.get('metrics:current')
            if not metrics_data:
                # Collect metrics if not available
                self.collect_metrics()
                metrics_data = self.redis.get('metrics:current')
            
            if metrics_data:
                metrics = json.loads(metrics_data)
                db_stats = metrics.get('database_stats', {})
                health = metrics.get('health_status', {})
                
                # Get table list from metrics or fallback
                mysql_table_list = db_stats.get('mysql', {}).get('table_list', [])
                postgres_table_list = db_stats.get('postgres', {}).get('table_list', [])
                
                # Use MySQL tables as the primary source, fallback to PostgreSQL, then defaults
                if mysql_table_list:
                    tables = mysql_table_list
                elif postgres_table_list:
                    tables = postgres_table_list
                else:
                    tables = self.get_table_list() # Use get_table_list() here
                
                # Calculate migration progress
                total_tables = len(tables)
                synced_tables = 0
                failed_tables = 0
                
                mysql_tables = db_stats.get('mysql', {}).get('tables', {})
                postgres_tables = db_stats.get('postgres', {}).get('tables', {})
                
                for table in tables:
                    mysql_count = mysql_tables.get(table, 0)
                    postgres_count = postgres_tables.get(table, 0)
                    
                    if mysql_count >= 0 and postgres_count >= 0:
                        if mysql_count == postgres_count:
                            synced_tables += 1
                        elif postgres_count > 0:
                            # Partially synced, count as synced for progress
                            synced_tables += 1
                    else:
                        failed_tables += 1
                
                pending_tables = total_tables - synced_tables - failed_tables
                progress = (synced_tables / total_tables) * 100 if total_tables > 0 else 0
                
                return MigrationStatus(
                    total_tables=total_tables,
                    synced_tables=synced_tables,
                    pending_tables=pending_tables,
                    failed_tables=failed_tables,
                    overall_progress=progress,
                    last_update=datetime.now(),
                    is_healthy=(health.get('overall') == 'healthy')
                )
            
            # Fallback if no metrics available
            fallback_tables = self.get_table_list()
            return MigrationStatus(
                total_tables=len(fallback_tables),
                synced_tables=0,
                pending_tables=len(fallback_tables),
                failed_tables=0,
                overall_progress=0.0,
                last_update=datetime.now(),
                is_healthy=False
            )
            
        except Exception as e:
            logger.error(f"Failed to get migration stats: {str(e)}")
            raise
    
    async def get_table_stats(self, table_name: str) -> TableStats:
        """Get statistics for a specific table"""
        try:
            # Get database counts
            mysql_count = self.mysql.get_table_count(table_name)
            postgres_count = self.postgres.get_table_count(table_name)
            
            # Get sync stats
            today = datetime.now().strftime('%Y%m%d')
            sync_data = self.redis.hgetall(f"stats:{table_name}:{today}")
            last_sync_str = self.redis.get(f"last_sync:{table_name}")
            
            last_sync_time = None
            sync_lag = None
            if last_sync_str:
                try:
                    last_sync_time = datetime.fromisoformat(last_sync_str)
                    sync_lag = self._calculate_sync_lag(last_sync_str)
                except:
                    pass
            
            return TableStats(
                table_name=table_name,
                mysql_count=mysql_count,
                postgres_count=postgres_count,
                last_sync_time=last_sync_time,
                insert_count=int(sync_data.get('insert', 0)),
                update_count=int(sync_data.get('update', 0)),
                delete_count=int(sync_data.get('delete', 0)),
                sync_lag_seconds=sync_lag
            )
            
        except Exception as e:
            logger.error(f"Failed to get table stats for {table_name}: {str(e)}")
            raise
    
    def get_historical_metrics(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get historical metrics for the last N hours"""
        try:
            historical_data = []
            now = datetime.now()
            
            for i in range(hours):
                hour_time = now - timedelta(hours=i)
                hour_key = f"metrics:hourly:{hour_time.strftime('%Y%m%d%H')}"
                
                data = self.redis.get(hour_key)
                if data:
                    historical_data.append(json.loads(data))
            
            return list(reversed(historical_data))  # Chronological order
            
        except Exception as e:
            logger.error(f"Failed to get historical metrics: {str(e)}")
            return [] 