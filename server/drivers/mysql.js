import mysql from 'mysql2/promise';

function serializeValue(value) {
  if (value === null || value === undefined) return null;
  if (Buffer.isBuffer(value)) return value.toString('utf8');
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'bigint') {
    const n = Number(value);
    return Number.isSafeInteger(n) ? n : String(value);
  }
  if (typeof value === 'object') {
    try { return JSON.stringify(value); } catch (_) { return String(value); }
  }
  return value;
}

async function withConnection(config, fn) {
  const connection = await mysql.createConnection({
    host: config.host,
    port: config.port,
    user: config.username,
    password: config.password,
    database: config.database,
    connectTimeout: 10000
  });
  try {
    return await fn(connection);
  } finally {
    await connection.end();
  }
}

export async function listTables(config) {
  return withConnection(config, async (conn) => {
    const [rows] = await conn.query(
      `SELECT TABLE_NAME AS tableName, COALESCE(TABLE_ROWS, 0) AS estimatedRows
       FROM INFORMATION_SCHEMA.TABLES
       WHERE TABLE_SCHEMA = ?
       ORDER BY TABLE_NAME ASC`,
      [config.database]
    );
    return (Array.isArray(rows) ? rows : []).map((r) => ({
      tableName: String(r?.tableName || ''),
      estimatedRows: Number(r?.estimatedRows || 0)
    }));
  });
}

export async function fetchTableData(config, tableName, rowLimit, incrementalOpts) {
  return withConnection(config, async (conn) => {
    const [columns] = await conn.query(
      `SELECT COLUMN_NAME AS columnName, DATA_TYPE AS dataType
       FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
       ORDER BY ORDINAL_POSITION ASC`,
      [config.database, tableName]
    );
    const columnList = (Array.isArray(columns) ? columns : []).map((c) => ({
      name: String(c?.columnName || ''),
      dataType: String(c?.dataType || 'text')
    }));
    if (!columnList.length) return { tableName, columns: [], rows: [] };

    let query = `SELECT * FROM ${mysql.escapeId(tableName)}`;
    const params = [];
    if (incrementalOpts?.primaryKey && incrementalOpts?.lastValue != null) {
      query += ` WHERE ${mysql.escapeId(incrementalOpts.primaryKey)} > ?`;
      params.push(incrementalOpts.lastValue);
      query += ` ORDER BY ${mysql.escapeId(incrementalOpts.primaryKey)} ASC`;
    }
    query += ` LIMIT ?`;
    params.push(rowLimit);

    const [rows] = await conn.query(query, params);
    const normalizedRows = (Array.isArray(rows) ? rows : []).map((row) => {
      const record = {};
      for (const key of Object.keys(row || {})) {
        record[key] = serializeValue(row[key]);
      }
      return record;
    });
    return { tableName, columns: columnList, rows: normalizedRows };
  });
}
