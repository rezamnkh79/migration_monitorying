import os
import logging
from typing import Dict, List, Any, Optional
import MySQLdb
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd

logger = logging.getLogger(__name__)

class MySQLClient:
    def __init__(self):
        self.host = os.getenv('MYSQL_HOST', 'mysql')
        self.user = os.getenv('MYSQL_USER', 'debezium')
        self.password = os.getenv('MYSQL_PASSWORD', 'debezium')  # Changed from 'dbz' to 'debezium'
        self.database = os.getenv('MYSQL_DATABASE', 'inventory')
        self.port = int(os.getenv('MYSQL_PORT', '3306'))
        
        # SQLAlchemy engine
        self.engine = None
        self.Session = None
        self._init_connection()
    
    def _init_connection(self):
        """Initialize database connection"""
        try:
            connection_string = f"mysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            self.engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_recycle=3600,
                pool_size=2,  # Limit connection pool size
                max_overflow=1,  # Limit overflow connections
                pool_timeout=10,  # Timeout for getting connection from pool
                connect_args={
                    'connect_timeout': 5,  # Connection timeout
                    'read_timeout': 10,    # Read timeout
                    'write_timeout': 10    # Write timeout
                },
                echo=False
            )
            self.Session = sessionmaker(bind=self.engine)
            logger.info(f"MySQL connection initialized: {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to initialize MySQL connection: {str(e)}")
            raise
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                return result.fetchone()[0] == 1
        except Exception as e:
            logger.error(f"MySQL connection test failed: {str(e)}")
            return False
    
    def get_table_list(self) -> List[str]:
        """Get list of all tables in the database"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SHOW TABLES"))
                tables = [row[0] for row in result.fetchall()]
                logger.info(f"Found {len(tables)} tables in MySQL database")
                return tables
        except Exception as e:
            logger.error(f"Failed to get table list from MySQL: {str(e)}")
            return []
    
    def get_table_count(self, table_name: str) -> int:
        """Get row count for a table"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                return result.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to get count for table {table_name}: {str(e)}")
            return 0
    
    def get_table_data(self, table_name: str, limit: Optional[int] = None, offset: int = 0) -> pd.DataFrame:
        """Get data from a table"""
        try:
            query = f"SELECT * FROM {table_name}"
            if limit:
                query += f" LIMIT {limit} OFFSET {offset}"
            
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logger.error(f"Failed to get data from table {table_name}: {str(e)}")
            return pd.DataFrame()
    
    def get_record_by_id(self, table_name: str, record_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific record by ID"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT * FROM {table_name} WHERE id = :id"), {"id": record_id})
                row = result.fetchone()
                if row:
                    return dict(row._mapping)
                return None
        except Exception as e:
            logger.error(f"Failed to get record {record_id} from table {table_name}: {str(e)}")
            return None
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """Get table schema information"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"DESCRIBE {table_name}"))
                return [dict(row._mapping) for row in result.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {str(e)}")
            return []
    
    def get_tables(self) -> List[str]:
        """Get list of all tables in the database"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SHOW TABLES"))
                return [row[0] for row in result.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get tables list: {str(e)}")
            return []
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Execute a custom query"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return [dict(row._mapping) for row in result.fetchall()]
        except Exception as e:
            logger.error(f"Failed to execute query: {str(e)}")
            return []
    
    def get_latest_records(self, table_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get latest records from a table (assuming there's a timestamp column)"""
        try:
            # Try common timestamp column names
            timestamp_cols = ['created_at', 'updated_at', 'timestamp', 'date_created']
            schema = self.get_table_schema(table_name)
            timestamp_col = None
            
            for col in schema:
                if col['Field'] in timestamp_cols:
                    timestamp_col = col['Field']
                    break
            
            if timestamp_col:
                query = f"SELECT * FROM {table_name} ORDER BY {timestamp_col} DESC LIMIT {limit}"
            else:
                # Fallback to ID ordering
                query = f"SELECT * FROM {table_name} ORDER BY id DESC LIMIT {limit}"
            
            return self.execute_query(query)
        except Exception as e:
            logger.error(f"Failed to get latest records from {table_name}: {str(e)}")
            return []
    
    def close(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("MySQL connection closed") 