"""
Centralized table discovery utility for dynamic table management
"""
import logging
from typing import List, Set, Optional
from database.mysql_client import MySQLClient

logger = logging.getLogger(__name__)

class TableDiscoveryService:
    """Service for discovering and filtering database tables dynamically"""
    
    EXCLUDED_PATTERNS = [
        'information_schema', 'performance_schema', 'mysql', 'sys',
        'migration_log', 'schema_migrations', 'flyway_schema_history'
    ]
    
    EXCLUDED_PREFIXES = ['_', 'tmp_', 'temp_', 'bak_', 'backup_']
    EXCLUDED_SUFFIXES = ['_backup', '_bak', '_temp', '_tmp', '_old']
    
    @classmethod
    def get_syncable_tables(cls, mysql_client: MySQLClient) -> List[str]:
        """
        Get list of tables that should be synchronized
        
        Args:
            mysql_client: MySQL database client
            
        Returns:
            List of table names suitable for synchronization
        """
        try:
            if not mysql_client:
                logger.warning("No MySQL client provided for table discovery")
                return []
            
            # Get all tables from MySQL
            all_tables = mysql_client.get_table_list()
            if not all_tables:
                logger.warning("No tables found in MySQL database")
                return []
            
            # Filter tables
            syncable_tables = []
            for table in all_tables:
                if cls._should_sync_table(table):
                    syncable_tables.append(table)
                else:
                    logger.debug(f"Excluding table from sync: {table}")
            
            logger.info(f"Discovered {len(syncable_tables)} syncable tables out of {len(all_tables)} total tables")
            return sorted(syncable_tables)
            
        except Exception as e:
            logger.error(f"Failed to discover syncable tables: {str(e)}")
            return []
    
    @classmethod
    def get_monitorable_tables(cls, mysql_client: MySQLClient) -> List[str]:
        """
        Get list of tables that should be monitored
        
        Args:
            mysql_client: MySQL database client
            
        Returns:
            List of table names suitable for monitoring
        """
        # For now, syncable and monitorable tables are the same
        # This could be different in the future if needed
        return cls.get_syncable_tables(mysql_client)
    
    @classmethod
    def _should_sync_table(cls, table_name: str) -> bool:
        """
        Determine if a table should be included in synchronization
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if table should be synchronized, False otherwise
        """
        if not table_name:
            return False
        
        table_lower = table_name.lower()
        
        # Check excluded patterns
        for pattern in cls.EXCLUDED_PATTERNS:
            if pattern in table_lower:
                return False
        
        # Check excluded prefixes
        for prefix in cls.EXCLUDED_PREFIXES:
            if table_name.startswith(prefix):
                return False
        
        # Check excluded suffixes
        for suffix in cls.EXCLUDED_SUFFIXES:
            if table_lower.endswith(suffix):
                return False
        
        # Additional checks for common exclusion patterns
        if any(keyword in table_lower for keyword in ['test', 'debug', 'log', 'cache']):
            return False
        
        return True
    
    @classmethod
    def get_table_categories(cls, tables: List[str]) -> dict:
        """
        Categorize tables by their apparent purpose
        
        Args:
            tables: List of table names
            
        Returns:
            Dictionary with categorized tables
        """
        categories = {
            'adtrace_core': [],
            'business_data': [],
            'django_system': [],
            'authentication': [],
            'financial': [],
            'other': []
        }
        
        for table in tables:
            table_lower = table.lower()
            
            if table.startswith('adtrace_'):
                categories['adtrace_core'].append(table)
            elif table.startswith('django_') or table.startswith('auth_'):
                categories['django_system'].append(table)
            elif 'auth' in table_lower or 'user' in table_lower or 'identity' in table_lower:
                categories['authentication'].append(table)
            elif any(word in table_lower for word in ['transaction', 'payment', 'invoice', 'wallet', 'price']):
                categories['financial'].append(table)
            elif any(word in table_lower for word in ['mobile_app', 'partner', 'plan', 'message', 'email']):
                categories['business_data'].append(table)
            else:
                categories['other'].append(table)
        
        return categories
    
    @classmethod
    def get_priority_tables(cls, tables: List[str]) -> List[str]:
        """
        Get high-priority tables that should be synced first
        
        Args:
            tables: List of all available tables
            
        Returns:
            List of high-priority table names
        """
        priority_patterns = [
            'adtrace_tracker',
            'adtrace_mobile_app', 
            'adtrace_event_type',
            'buy_transaction',
            'mobile_app_detail',
            'adtrace_transaction'
        ]
        
        priority_tables = []
        for pattern in priority_patterns:
            if pattern in tables:
                priority_tables.append(pattern)
        
        return priority_tables 