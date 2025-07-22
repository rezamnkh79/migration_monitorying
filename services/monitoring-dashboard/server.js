const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const cors = require('cors');
const path = require('path');
const axios = require('axios');
const redis = require('redis');
const mysql = require('mysql2/promise');
const { Client } = require('pg');

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Configuration
const config = {
  redis: {
    host: process.env.REDIS_HOST || 'redis',
    port: 6379
  },
  mysql: {
    host: process.env.MYSQL_HOST || 'mysql',
    user: process.env.MYSQL_USER || 'debezium',
    password: process.env.MYSQL_PASSWORD || 'dbz',
    database: process.env.MYSQL_DATABASE || 'inventory',
    port: parseInt(process.env.MYSQL_PORT) || 3306
  },
  postgres: {
    host: process.env.POSTGRES_HOST || 'postgres',
    user: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD || 'postgres',
    database: process.env.POSTGRES_DATABASE || 'inventory',
    port: parseInt(process.env.POSTGRES_PORT) || 5432
  },
  validator: {
    url: 'http://data-validator:8000'
  }
};

// Initialize connections
let redisClient;
let mysqlConnection;
let postgresClient;

async function initializeConnections() {
  try {
    // Redis connection
    redisClient = redis.createClient({
      socket: {
        host: config.redis.host,
        port: config.redis.port
      }
    });
    
    redisClient.on('error', (err) => {
      console.error('Redis Client Error:', err);
    });
    
    await redisClient.connect();
    console.log('Connected to Redis');

    // MySQL connection
    mysqlConnection = await mysql.createConnection(config.mysql);
    console.log('Connected to MySQL');

    // PostgreSQL connection
    postgresClient = new Client(config.postgres);
    await postgresClient.connect();
    console.log('Connected to PostgreSQL');

  } catch (error) {
    console.error('Failed to initialize connections:', error);
  }
}

// Routes
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/api/status', async (req, res) => {
  try {
    const status = {
      timestamp: new Date().toISOString(),
      services: {
        redis: await testRedisConnection(),
        mysql: await testMySQLConnection(),
        postgres: await testPostgresConnection(),
        validator: await testValidatorConnection()
      }
    };
    res.json(status);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/metrics', async (req, res) => {
  try {
    // Get metrics from data validator service with dynamic table lists
    const response = await axios.get(`${config.validator.url}/metrics`, { timeout: 15000 });
    res.json(response.data);
  } catch (error) {
    console.error('Error getting metrics from validator:', error.message);
    
    // Fallback to direct database queries
    try {
      const fallbackMetrics = await getFallbackMetrics();
      res.json(fallbackMetrics);
    } catch (fallbackError) {
      console.error('Error getting fallback metrics:', fallbackError);
      res.status(500).json({ error: error.message });
    }
  }
});

app.get('/api/validation', async (req, res) => {
  try {
    const response = await axios.get(`${config.validator.url}/stats`);
    res.json(response.data);
  } catch (error) {
    console.error('Error getting validation data:', error);
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/kafka-status', async (req, res) => {
  try {
    const response = await axios.get(`${config.validator.url}/kafka-status`);
    res.json(response.data);
  } catch (error) {
    console.error('Error getting Kafka status:', error);
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/trigger-validation', async (req, res) => {
  try {
    const { tables, fullValidation } = req.body;
    const response = await axios.post(`${config.validator.url}/validate`, {
      tables: tables,
      full_validation: fullValidation || false
    });
    res.json(response.data);
  } catch (error) {
    console.error('Error triggering validation:', error);
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/migration-log', async (req, res) => {
  try {
    const { limit = 50, table } = req.query;
    let url = `${config.validator.url}/migration-log?limit=${limit}`;
    if (table) {
      url += `&table_name=${table}`;
    }
    const response = await axios.get(url);
    res.json(response.data);
  } catch (error) {
    console.error('Error getting migration log:', error);
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/latest-records/:tableName', async (req, res) => {
  try {
    const { tableName } = req.params;
    const { limit = 5 } = req.query;
    
    const response = await axios.get(`${config.validator.url}/latest-records/${tableName}?limit=${limit}`);
    res.json(response.data);
  } catch (error) {
    console.error('Error getting latest records:', error);
    res.status(500).json({ error: error.message });
  }
});

// Helper functions
async function testRedisConnection() {
  try {
    await redisClient.ping();
    return { status: 'connected', message: 'Redis is healthy' };
  } catch (error) {
    return { status: 'disconnected', message: error.message };
  }
}

async function testMySQLConnection() {
  try {
    await mysqlConnection.ping();
    return { status: 'connected', message: 'MySQL is healthy' };
  } catch (error) {
    return { status: 'disconnected', message: error.message };
  }
}

async function testPostgresConnection() {
  try {
    await postgresClient.query('SELECT 1');
    return { status: 'connected', message: 'PostgreSQL is healthy' };
  } catch (error) {
    return { status: 'disconnected', message: error.message };
  }
}

async function testValidatorConnection() {
  try {
    const response = await axios.get(`${config.validator.url}/health`, { timeout: 15000 });
    return { status: 'connected', message: 'Data validator is healthy', data: response.data };
  } catch (error) {
    return { status: 'disconnected', message: error.message };
  }
}

// Get list of tables from MySQL database
async function getMySQLTables() {
  try {
    if (!mysqlConnection) return [];
    const [rows] = await mysqlConnection.execute('SHOW TABLES');
    return rows.map(row => Object.values(row)[0]);
  } catch (error) {
    console.error('Error getting MySQL tables:', error);
    return [];
  }
}

// Get list of tables from PostgreSQL database
async function getPostgresTables() {
  try {
    if (!postgresClient) return [];
    const result = await postgresClient.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
    `);
    return result.rows.map(row => row.table_name);
  } catch (error) {
    console.error('Error getting PostgreSQL tables:', error);
    return [];
  }
}

async function getFallbackMetrics() {
  // Get tables dynamically from MySQL
  const mysqlTables = await getMySQLTables();
  const postgresTables = await getPostgresTables();
  
  // Use tables from MySQL as the source of truth - no hardcoded fallback
  const tables = mysqlTables.length > 0 ? mysqlTables : [];
  
  const metrics = {
    timestamp: new Date().toISOString(),
    database_stats: {
      mysql: { status: 'unknown', tables: {}, table_list: mysqlTables },
      postgres: { status: 'unknown', tables: {}, table_list: postgresTables }
    },
    sync_stats: {},
    health_status: {
      overall: 'unknown'
    }
  };

  try {
    // Get MySQL table counts for all discovered tables
    if (mysqlConnection && tables.length > 0) {
      metrics.database_stats.mysql.status = 'connected';
      for (const table of tables) {
        try {
          const [rows] = await mysqlConnection.execute(`SELECT COUNT(*) as count FROM \`${table}\``);
          metrics.database_stats.mysql.tables[table] = rows[0].count;
        } catch (error) {
          console.error(`Error counting MySQL table ${table}:`, error);
          metrics.database_stats.mysql.tables[table] = -1;
        }
      }
    }

    // Get PostgreSQL table counts for all discovered tables
    if (postgresClient && tables.length > 0) {
      metrics.database_stats.postgres.status = 'connected';
      for (const table of tables) {
        try {
          const result = await postgresClient.query(`SELECT COUNT(*) as count FROM "${table}"`);
          metrics.database_stats.postgres.tables[table] = parseInt(result.rows[0].count);
        } catch (error) {
          console.error(`Error counting PostgreSQL table ${table}:`, error);
          metrics.database_stats.postgres.tables[table] = -1;
        }
      }
    }

  } catch (error) {
    console.error('Error getting fallback metrics:', error);
  }

  return metrics;
}

// Real-time updates via Socket.IO
io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);

  // Send initial data
  sendRealTimeUpdate(socket);

  // Send periodic updates
  const updateInterval = setInterval(() => {
    sendRealTimeUpdate(socket);
  }, 300000); // Update every 30 seconds instead of 5

  socket.on('disconnect', () => {
    console.log('Client disconnected:', socket.id);
    clearInterval(updateInterval);
  });

  socket.on('requestUpdate', () => {
    sendRealTimeUpdate(socket);
  });
});

async function sendRealTimeUpdate(socket) {
  try {
    // Get current metrics from data validator API first
    let metrics;
    try {
      const response = await axios.get(`${config.validator.url}/metrics`, { timeout: 15000 });
      metrics = response.data;
      console.log("Got metrics from validator API, MySQL tables:", metrics?.database_stats?.mysql?.table_list?.length || 0);
    } catch (error) {
      console.error("Failed to get metrics from validator, using fallback:", error.message);
      // Fallback to Redis or getFallbackMetrics
      const metricsData = await redisClient.get("metrics:current");
      if (metricsData) {
        metrics = JSON.parse(metricsData);
      } else {
        metrics = await getFallbackMetrics();
      }
    }

    // Get validation status
    let validationStatus = null;
    try {
      const response = await axios.get(`${config.validator.url}/stats`, { timeout: 10000 });
      validationStatus = response.data;
    } catch (error) {
      console.error("Failed to get validation status:", error.message);
    }

    // Get Kafka status
    let kafkaStatus = null;
    try {
      const response = await axios.get(`${config.validator.url}/kafka-status`, { timeout: 10000 });
      kafkaStatus = response.data;
    } catch (error) {
      console.error("Failed to get Kafka status:", error.message);
    }

    const updateData = {
      timestamp: new Date().toISOString(),
      metrics,
      validation: validationStatus,
      kafka: kafkaStatus
    };

    socket.emit("dataUpdate", updateData);
  } catch (error) {
    console.error("Error sending real-time update:", error);
    socket.emit("error", { message: "Failed to fetch real-time data" });
  }
}

// Start server
const PORT = process.env.PORT || 3000;

async function startServer() {
  await initializeConnections();
  
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`Monitoring Dashboard running on port ${PORT}`);
    console.log(`Dashboard URL: http://localhost:${PORT}`);
  });
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down gracefully...');
  
  if (redisClient) {
    await redisClient.quit();
  }
  if (mysqlConnection) {
    await mysqlConnection.end();
  }
  if (postgresClient) {
    await postgresClient.end();
  }
  
  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
});

startServer().catch(console.error); 