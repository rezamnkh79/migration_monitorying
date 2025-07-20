from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, ConfigDict

class ValidationResult(BaseModel):
    model_config = ConfigDict(
        json_encoders={
            datetime: lambda v: v.isoformat()
        }
    )
    
    table_name: str
    mysql_count: int
    postgres_count: int
    is_consistent: bool
    validation_time: datetime
    quick_check: bool = True
    discrepancies: List[str] = []

class TableStats(BaseModel):
    model_config = ConfigDict(
        json_encoders={
            datetime: lambda v: v.isoformat()
        }
    )
    
    table_name: str
    mysql_count: int
    postgres_count: int
    last_sync_time: Optional[datetime] = None
    insert_count: int = 0
    update_count: int = 0
    delete_count: int = 0
    sync_lag_seconds: Optional[int] = None

class MigrationStatus(BaseModel):
    model_config = ConfigDict(
        json_encoders={
            datetime: lambda v: v.isoformat()
        }
    )
    
    total_tables: int
    synced_tables: int
    pending_tables: int
    failed_tables: int
    overall_progress: float
    last_update: datetime
    is_healthy: bool

class MigrationLogEntry(BaseModel):
    id: int
    table_name: str
    operation: str
    mysql_id: Optional[int] = None
    postgres_id: Optional[int] = None
    data_hash: str
    migrated_at: datetime
    verified: bool = False
    verification_notes: Optional[str] = None
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        } 