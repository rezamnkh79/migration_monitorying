import os
import logging
from typing import Dict, List, Any, Optional
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd

logger = logging.getLogger(__name__)

class PostgreSQLClient:
    def __init__(self):
        self.host = os.getenv('POSTGRES_HOST', 'postgres')
        self.user = os.getenv('POSTGRES_USER', 'postgres')
        self.password = os.getenv('POSTGRES_PASSWORD', 'postgres')
        self.database = os.getenv('POSTGRES_DATABASE', 'inventory')
        self.port = int(os.getenv('POSTGRES_PORT', '5432'))
        
        # SQLAlchemy engine
        self.engine = None
        self.Session = None
        self._init_connection()
    
    def _init_connection(self):
        """Initialize database connection"""
        try:
            connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            self.engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False
            )
            self.Session = sessionmaker(bind=self.engine)
            logger.info(f"PostgreSQL connection initialized: {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL connection: {str(e)}")
            raise
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                return result.fetchone()[0] == 1
        except Exception as e:
            logger.error(f"PostgreSQL connection test failed: {str(e)}")
            return False
    
    def get_table_list(self) -> List[str]:
        """Get list of all tables in the database"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """))
                tables = [row[0] for row in result.fetchall()]
                logger.info(f"Found {len(tables)} tables in PostgreSQL database")
                return tables
        except Exception as e:
            logger.error(f"Failed to get table list from PostgreSQL: {str(e)}")
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
            query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length
            FROM information_schema.columns 
            WHERE table_name = :table_name 
            AND table_schema = 'public'
            ORDER BY ordinal_position
            """
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"table_name": table_name})
                return [dict(row._mapping) for row in result.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {str(e)}")
            return []
    
    def get_tables(self) -> List[str]:
        """Get list of all tables in the database"""
        try:
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            """
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
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
    
    def insert_record(self, table_name: str, data: Dict[str, Any]) -> bool:
        """Insert a record into PostgreSQL"""
        try:
            # Build INSERT query
            columns = list(data.keys())
            placeholders = [f":{col}" for col in columns]
            
            query = f"""
            INSERT INTO {table_name} ({', '.join(columns)}) 
            VALUES ({', '.join(placeholders)})
            """
            
            with self.engine.connect() as conn:
                trans = conn.begin()
                try:
                    conn.execute(text(query), data)
                    trans.commit()
                    return True
                except Exception as e:
                    trans.rollback()
                    raise e
                    
        except Exception as e:
            logger.error(f"Failed to insert record into {table_name}: {str(e)}")
            return False
    
    def update_record(self, table_name: str, record_id: int, data: Dict[str, Any]) -> bool:
        """Update a record in PostgreSQL"""
        try:
            # Build UPDATE query
            set_clauses = [f"{col} = :{col}" for col in data.keys()]
            query = f"""
            UPDATE {table_name} 
            SET {', '.join(set_clauses)}
            WHERE id = :record_id
            """
            
            data['record_id'] = record_id
            
            with self.engine.connect() as conn:
                trans = conn.begin()
                try:
                    conn.execute(text(query), data)
                    trans.commit()
                    return True
                except Exception as e:
                    trans.rollback()
                    raise e
                    
        except Exception as e:
            logger.error(f"Failed to update record {record_id} in {table_name}: {str(e)}")
            return False
    
    def delete_record(self, table_name: str, record_id: int) -> bool:
        """Delete a record from PostgreSQL"""
        try:
            query = f"DELETE FROM {table_name} WHERE id = :record_id"
            
            with self.engine.connect() as conn:
                trans = conn.begin()
                try:
                    conn.execute(text(query), {"record_id": record_id})
                    trans.commit()
                    return True
                except Exception as e:
                    trans.rollback()
                    raise e
                    
        except Exception as e:
            logger.error(f"Failed to delete record {record_id} from {table_name}: {str(e)}")
            return False
    
    def log_migration(self, table_name: str, operation: str, mysql_id: int, 
                     postgres_id: Optional[int], data_hash: str) -> bool:
        """Log migration operation"""
        try:
            data = {
                'table_name': table_name,
                'operation': operation,
                'mysql_id': mysql_id,
                'postgres_id': postgres_id,
                'data_hash': data_hash
            }
            return self.insert_record('migration_log', data)
        except Exception as e:
            logger.error(f"Failed to log migration: {str(e)}")
            return False
    
    def get_latest_records(self, table_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get latest records from a table"""
        try:
            # Try common timestamp column names
            timestamp_cols = ['created_at', 'updated_at', 'timestamp', 'date_created']
            schema = self.get_table_schema(table_name)
            timestamp_col = None
            
            for col in schema:
                if col['column_name'] in timestamp_cols:
                    timestamp_col = col['column_name']
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
            logger.info("PostgreSQL connection closed") 