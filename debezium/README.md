# Dynamic CDC Connectors System

## Overview
This system now uses **Dynamic Table Monitoring** instead of static connector configuration files.

## How It Works

### 1. Dynamic Discovery
- Automatically discovers all tables in MySQL database
- No hardcoded table names
- Monitors for new tables every 30 seconds
- Excludes system tables automatically

### 2. Single Dynamic Connector
- Creates one `dynamic-mysql-source` connector for all discovered tables
- Updates connector configuration when new tables are added
- Uses regex transformation to route topics correctly

### 3. No Static Files
All previous static connector files have been removed:
- ✅ **REMOVED**: `mysql-source-connector.json`
- ✅ **REMOVED**: `mysql-source-connector-inventory.json`
- ✅ **REMOVED**: `postgres-sink-connector.json`
- ✅ **REMOVED**: `postgres-sink-connector-inventory.json`
- ✅ **REMOVED**: `postgres-sink-connector-inventory-fixed.json`
- ✅ **REMOVED**: `postgres-sink-simple.json`
- ✅ **REMOVED**: `postgres-sink-final.json`

## Current Setup

### Active Connector
**Name**: `dynamic-mysql-source`
- **Type**: MySQL Source Connector
- **Tables**: Dynamically discovered
- **Topic Prefix**: `dynamic`
- **Snapshot Mode**: `initial`

### Monitoring
- **API Endpoint**: `/table-monitor/status`
- **Discovery Interval**: 30 seconds
- **Real-time CDC**: ✅ Active
- **Table Count**: Dynamic (currently 6 tables)

## API Endpoints

### Setup
```bash
POST /table-monitor/setup
```

### Status
```bash
GET /table-monitor/status
GET /discover-tables
```

### Manual Add Table
```bash
POST /table-monitor/add-table/{table_name}
```

## Benefits

1. **✅ No Hardcoded Tables**: Automatically discovers new tables
2. **✅ Clean Architecture**: Single connector instead of multiple files
3. **✅ Real-time Monitoring**: 30-second discovery interval
4. **✅ Schema Awareness**: Tracks column changes
5. **✅ Auto Cleanup**: Removes unused connectors

## Current Status
- **MySQL Connector**: ✅ RUNNING
- **CDC Events**: ✅ Active
- **Table Discovery**: ✅ Active
- **Dynamic Updates**: ✅ Working

## No Manual Configuration Required
The system is fully automated and requires no manual connector setup! 