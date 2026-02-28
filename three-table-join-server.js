const express = require('express');
const mysql = require('mysql2');
const cors = require('cors');
const bodyParser = require('body-parser');

const app = express();
const port = 8013;

app.use(bodyParser.json({ limit: '10000mb' }));
app.use(bodyParser.urlencoded({ limit: '10000mb', extended: true }));
app.use(cors());

// 将 dbName 改为 let，以便在清理后可以更新时间戳（如果需要生成新名字）
// 这里保持逻辑简单，清理后只需重建即可
let dbName = `terminal_${Date.now()}`;

// 数据库配置常量
const DB_CONFIG = {
    host: 'localhost',
    user: 'root',
    password: 'Guoyanjun123.'
};

// Initialize database and create Visit, Terminal, and Scan tables
async function initDatabase() {
    // console.log(`Initializing database: ${dbName}...`);
    
    // 【修复点 1】baseDb 在函数内部创建，确保每次运行都是崭新的连接
    const baseDb = mysql.createConnection({
        host: DB_CONFIG.host,
        user: DB_CONFIG.user,
        password: DB_CONFIG.password
    });

    try {
        // 连接到 MySQL (无特定数据库)
        await new Promise((resolve, reject) => {
            baseDb.connect(err => err ? reject(`Base connection failed: ${err.message}`) : resolve());
        });

        // 创建数据库
        await new Promise((resolve, reject) => {
            baseDb.query(`CREATE DATABASE IF NOT EXISTS \`${dbName}\``, err =>
                err ? reject(`Failed to create database: ${err.message}`) : resolve()
            );
        });

        // 【修复点 2】用完立即关闭这个临时连接
        baseDb.end();

        // 连接到新创建的具体数据库
        const db = mysql.createConnection({
            ...DB_CONFIG,
            database: dbName
        });

        await new Promise((resolve, reject) => {
            db.connect(err => err ? reject(`Failed to connect to new database: ${err.message}`) : resolve());
        });

        // Create Visit table
        const createVisitTable = `
            CREATE TABLE IF NOT EXISTS Visit (
                客户编码 VARCHAR(50),
                INDEX idx_visit_customer (客户编码)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        `;

        // Create Terminal table (主表)
        const createTerminalTable = `
            CREATE TABLE IF NOT EXISTS Terminal (
                客户编码 VARCHAR(50),
                客户名称 VARCHAR(100),
                所属片区 VARCHAR(100),
                所属大区 VARCHAR(100),
                UNIQUE INDEX idx_terminal_customer (客户编码)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        `;

        // Create Scan table
        const createScanTable = `
            CREATE TABLE IF NOT EXISTS Scan (
                客户编码 VARCHAR(50),
                产品编码 VARCHAR(50),
                产品名称 VARCHAR(100),
                INDEX idx_scan_customer (客户编码)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        `;

        // Execute table creation queries
        await new Promise((resolve, reject) => {
            db.query(createVisitTable, err => err ? reject(`Failed to create Visit table: ${err.message}`) : resolve());
        });

        await new Promise((resolve, reject) => {
            db.query(createTerminalTable, err => err ? reject(`Failed to create Terminal table: ${err.message}`) : resolve());
        });

        await new Promise((resolve, reject) => {
            db.query(createScanTable, err => err ? reject(`Failed to create Scan table: ${err.message}`) : resolve());
        });

        // Store database connection in app instance for later use
        // 如果之前有连接，先尝试关闭它防止泄漏（虽然通常由 cleanup 处理）
        const oldDb = app.get('db');
        if (oldDb) {
            try { oldDb.end(); } catch(e) {}
        }
        
        app.set('db', db);
        // console.log(`Database initialization completed: ${dbName}`);
        // console.log('Visit, Terminal, and Scan tables have been created successfully');

    } catch (error) {
        console.error('Database initialization failed:', error);
        // 如果是初始化启动失败则退出，如果是运行时重置失败则抛出给调用者
        if (process.uptime() < 5) {
            process.exit(1);
        } else {
            throw error;
        }
    }
}

// Upload Visit records
app.post('/api/audit_visit/three_table_join/uploadVisit', (req, res) => {
    const db = app.get('db');
    const records = req.body.records;
    
    if (!Array.isArray(records) || records.length === 0) {
        return res.status(400).json({ success: false, error: 'Invalid Visit data provided' });
    }

    const values = records.map(r => [
        r.客户编码 || null
    ]);

    const sql = 'INSERT INTO Visit (客户编码) VALUES ?';
    db.query(sql, [values], (err, result) => {
        if (err) {
            console.error('Failed to insert Visit records:', err);
            res.status(500).json({ success: false, error: err.message });
        } else {
            res.json({ success: true, message: `${result.affectedRows} Visit records imported successfully` });
        }
    });
});

// Upload Terminal records
app.post('/api/audit_visit/three_table_join/uploadTerminal', (req, res) => {
    const db = app.get('db');
    const records = req.body.records;
    
    if (!Array.isArray(records) || records.length === 0) {
        return res.status(400).json({ success: false, error: 'Invalid Terminal data provided' });
    }

    const values = records.map(r => [
        r.客户编码 || null,
        r.客户名称 || null,
        r.所属片区 || null,
        r.所属大区 || null
    ]);

    const sql = 'INSERT IGNORE INTO Terminal (客户编码, 客户名称, 所属片区, 所属大区) VALUES ?';
    db.query(sql, [values], (err, result) => {
        if (err) {
            console.error('Failed to insert Terminal records:', err);
            res.status(500).json({ success: false, error: err.message });
        } else {
            res.json({ success: true, message: `${result.affectedRows} Terminal records imported successfully (duplicates automatically skipped)` });
        }
    });
});

// Upload Scan records
app.post('/api/audit_visit/three_table_join/uploadScan', (req, res) => {
    const db = app.get('db');
    const records = req.body.records;
    
    if (!Array.isArray(records) || records.length === 0) {
        return res.status(400).json({ success: false, error: 'Invalid Scan data provided' });
    }

    const values = records.map(r => [
        r.客户编码 || null,
        r.产品编码 || null,
        r.产品名称 || null
    ]);

    const sql = 'INSERT INTO Scan (客户编码, 产品编码, 产品名称) VALUES ?';
    db.query(sql, [values], (err, result) => {
        if (err) {
            console.error('Failed to insert Scan records:', err);
            res.status(500).json({ success: false, error: err.message });
        } else {
            res.json({ success: true, message: `${result.affectedRows} Scan records imported successfully` });
        }
    });
});

// Query merged data (三表 LEFT JOIN)
app.get('/api/audit_visit/three_table_join/getMergedData', (req, res) => {
    const db = app.get('db');
    
    let {
        customerName = '',
        customerCode = '',
        area = '',
        region = ''
    } = req.query;

    let conditions = [];
    let params = [];

    if (customerName) {
        conditions.push('t.`客户名称` LIKE ?');
        params.push(`%${customerName}%`);
    }
    if (customerCode) {
        conditions.push('t.`客户编码` LIKE ?');
        params.push(`%${customerCode}%`);
    }
    if (area) {
        conditions.push('t.`所属片区` LIKE ?');
        params.push(`%${area}%`);
    }
    if (region) {
        conditions.push('t.`所属大区` LIKE ?');
        params.push(`%${region}%`);
    }

    const whereClause = conditions.length > 0 ? 'WHERE ' + conditions.join(' AND ') : '';

    const sql = `
        SELECT
            t.客户名称 AS 客户名称,
            t.客户编码 AS 客户编码,
            t.所属片区 AS 所属片区,
            t.所属大区 AS 所属大区,
            COALESCE(v.visit_count, 0) AS 被拜访总次数,
            COALESCE(s.scan_count, 0) AS 扫码次数,
            COALESCE(s.scan_products, '') AS 扫码商品
        FROM
            Terminal t
        LEFT JOIN (
            SELECT
                客户编码,
                COUNT(*) AS visit_count
            FROM
                Visit
            GROUP BY
                客户编码
        ) v ON t.客户编码 = v.客户编码
        LEFT JOIN (
            SELECT
                客户编码,
                COUNT(*) AS scan_count,
                GROUP_CONCAT(
                    DISTINCT CONCAT(产品编码, '-', 产品名称)
                    ORDER BY 产品编码
                    SEPARATOR '; '
                ) AS scan_products
            FROM
                Scan
            GROUP BY
                客户编码
        ) s ON t.客户编码 = s.客户编码
        ${whereClause}
        ORDER BY
            被拜访总次数 DESC;
    `;

    // console.log('Executing merged data query...');
    
    db.query(sql, params, (err, results) => {
        if (err) {
            console.error('Failed to query merged data:', err);
            res.status(500).json({ success: false, error: err.message });
        } else {
            // console.log(`Query successful, found ${results.length} records`);
            res.json({ success: true, data: results });
        }
    });
});

// ============ Manual Cleanup: Drop entire database ============
app.post('/api/audit_visit/three_table_join/cleanup', async (req, res) => {
    // console.log('Manual database cleanup requested...');
    try {
        // 1. 先删除旧库
        await dropDatabase();
        
        // 2. 生成一个新的数据库名称 (可选，为了防止缓存或延迟，使用新时间戳)
        dbName = `terminal_${Date.now()}`;
        
        // 3. 重新初始化数据库
        await initDatabase();
        
        // console.log('Database cleanup and re-initialization completed');
        res.json({ success: true, message: `Database has been reset. New DB: ${dbName}` });
    } catch (error) {
        console.error('Failed to cleanup database:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Core function to drop database
async function dropDatabase() {
    // Close current database connection if exists
    const db = app.get('db');
    if (db) {
        try { 
            await new Promise((resolve) => db.end(resolve)); // 优雅关闭
            // console.log('Closed existing database connection');
        } catch (e) { 
            console.error('Error closing database connection:', e.message);
        }
    }

    // Create new connection for cleanup operation
    const cleanupDb = mysql.createConnection({
        host: DB_CONFIG.host,
        user: DB_CONFIG.user,
        password: DB_CONFIG.password
    });

    await new Promise((resolve, reject) => {
        cleanupDb.connect(err => err ? reject(err) : resolve());
    });

    // Execute database drop command
    await new Promise((resolve, reject) => {
        cleanupDb.query(`DROP DATABASE IF EXISTS \`${dbName}\``, err => {
            if (err) {
                reject(err);
            } else {
                // console.log(`Dropped database: ${dbName}`);
                resolve();
            }
        });
    });

    // Close cleanup connection
    cleanupDb.end();
}

// Setup process cleanup on exit
function setupProcessCleanup() {
    async function handleExit(signal) {
        // console.log(`\nReceived ${signal} signal, cleaning up before exit...`);
        try {
            await dropDatabase();
        } catch (error) {
            console.error('Error during cleanup:', error.message);
        }
        process.exit(0);
    }

    process.on('SIGINT', () => handleExit('SIGINT'));
    process.on('SIGTERM', () => handleExit('SIGTERM'));
}

// Initialize database and start server
initDatabase().then(() => {
    setupProcessCleanup();
    
    app.listen(port, () => {
        // console.log('='.repeat(60));
        console.log(`Server running on http://localhost:${port}`);
        // console.log(`Current database: ${dbName}`);
        // console.log('='.repeat(60));
    });
}).catch(error => {
    console.error('Failed to start server:', error);
    process.exit(1);
});