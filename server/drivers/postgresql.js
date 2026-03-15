import pg from 'pg';

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

async function withClient(config, fn) {
  const client = new pg.Client({
    host: config.host,
    port: config.port,
    database: config.database,
    user: config.username,
    password: config.password,
    connectionTimeoutMillis: 10000
  });
  await client.connect();
  try {
    return await fn(client);
  } finally {
    await client.end();
  }
}

export async function listTables(config) {
  return withClient(config, async (client) => {
    const result = await client.query(
      `SELECT t.table_name AS "tableName",
              COALESCE(s.n_live_tup, 0) AS "estimatedRows"
       FROM information_schema.tables t
       LEFT JOIN pg_stat_user_tables s ON s.relname = t.table_name
       WHERE t.table_schema = 'public' AND t.table_type = 'BASE TABLE'
       ORDER BY t.table_name ASC`
    );
    return (result.rows || []).map((r) => ({
      tableName: String(r.tableName || ''),
      estimatedRows: Number(r.estimatedRows || 0)
    }));
  });
}

export async function fetchTableData(config, tableName, rowLimit, incrementalOpts) {
  return withClient(config, async (client) => {
    const colResult = await client.query(
      `SELECT column_name AS "columnName", data_type AS "dataType"
       FROM information_schema.columns
       WHERE table_schema = 'public' AND table_name = $1
       ORDER BY ordinal_position ASC`,
      [tableName]
    );
    const columnList = (colResult.rows || []).map((c) => ({
      name: String(c.columnName || ''),
      dataType: String(c.dataType || 'text')
    }));
    if (!columnList.length) return { tableName, columns: [], rows: [] };

    const ident = `"${tableName.replace(/"/g, '""')}"`;
    let query = `SELECT * FROM ${ident}`;
    const params = [];
    let paramIndex = 1;

    if (incrementalOpts?.primaryKey && incrementalOpts?.lastValue != null) {
      const pkIdent = `"${incrementalOpts.primaryKey.replace(/"/g, '""')}"`;
      query += ` WHERE ${pkIdent} > $${paramIndex}`;
      params.push(incrementalOpts.lastValue);
      paramIndex++;
      query += ` ORDER BY ${pkIdent} ASC`;
    }
    query += ` LIMIT $${paramIndex}`;
    params.push(rowLimit);

    const result = await client.query(query, params);
    const normalizedRows = (result.rows || []).map((row) => {
      const record = {};
      for (const key of Object.keys(row || {})) {
        record[key] = serializeValue(row[key]);
      }
      return record;
    });
    return { tableName, columns: columnList, rows: normalizedRows };
  });
}
